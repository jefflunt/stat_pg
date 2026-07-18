use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::guc::*;
use pgrx::spi::SpiClient;
use pgrx::datum::DatumWithOid;
use std::collections::{HashMap, HashSet};
use std::ffi::CString;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::thread;

pgrx::pg_module_magic!();

// Tables definition using extension_sql!
extension_sql!(
    r#"
    CREATE TABLE IF NOT EXISTS evt_secs (
        code VARCHAR NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, code)
    );
    CREATE TABLE IF NOT EXISTS evt_mins (
        code VARCHAR NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, code)
    );
    CREATE TABLE IF NOT EXISTS evt_hours (
        code VARCHAR NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, code)
    );
    CREATE TABLE IF NOT EXISTS evt_days (
        code VARCHAR NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, code)
    );
    CREATE TABLE IF NOT EXISTS evt_30s (
        code VARCHAR NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, code)
    );
    "#,
    name = "create_evt_tables"
);

// GUC configuration settings
static DB_NAME: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static DB_USER: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static BIND_ADDR: GucSetting<Option<CString>> = GucSetting::<Option<CString>>::new(None);
static PORT: GucSetting<i32> = GucSetting::<i32>::new(8125);
static FLUSH_INTERVAL: GucSetting<f64> = GucSetting::<f64>::new(1.0);

#[pg_guard]
pub extern "C-unwind" fn _PG_init() {
    GucRegistry::define_string_guc(
        c"stat_pg.database",
        c"Database name for BGWorker connection",
        c"The database name that the stat-pg background worker should connect to.",
        &DB_NAME,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"stat_pg.user",
        c"Database user for BGWorker connection",
        c"The database user name that the stat-pg background worker should connect as.",
        &DB_USER,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_string_guc(
        c"stat_pg.bind_address",
        c"IP address to bind the UDP socket to",
        c"The local IP address that the UDP listener socket binds to.",
        &BIND_ADDR,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"stat_pg.port",
        c"UDP port to listen on",
        c"The local UDP port that the listener socket binds to.",
        &PORT,
        1024,
        65535,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_float_guc(
        c"stat_pg.flush_interval",
        c"Aggregate flush interval in seconds",
        c"The interval in seconds at which the background worker writes aggregated data to the database tables.",
        &FLUSH_INTERVAL,
        0.1,
        3600.0,
        GucContext::Sighup,
        GucFlags::default(),
    );

    BackgroundWorkerBuilder::new("stat_pg_aggregator")
        .set_function("stat_pg_main")
        .set_library("stat_pg")
        .set_start_time(BgWorkerStartTime::PostmasterStart)
        .set_restart_time(Some(Duration::from_secs(10)))
        .enable_spi_access()
        .load();
}

#[derive(Clone, Debug, PartialEq)]
struct Metric {
    code: Arc<str>,
    value: f64,
    at: i64,
}

#[derive(Default)]
struct MetricCache {
    secs: HashMap<(Arc<str>, i64), f64>,
    mins: HashMap<(Arc<str>, i64), f64>,
    hours: HashMap<(Arc<str>, i64), f64>,
    days: HashMap<(Arc<str>, i64), f64>,
    days30: HashMap<(Arc<str>, i64), f64>,
}

impl MetricCache {
    fn add(&mut self, m: Metric) {
        let code = m.code;
        let val = m.value;
        let ts = m.at;

        // In the hot path, we ONLY insert/aggregate into the seconds map.
        // This cuts down hashing overhead in the parser loop by 80% (1 lookup instead of 5).
        self.secs.entry((code, ts)).and_modify(|e| *e += val).or_insert(val);
    }

    fn populate_rollups(&mut self) {
        for ((code, ts), &val) in &self.secs {
            // minutes
            let min_ts = ts / 60;
            self.mins.entry((code.clone(), min_ts)).and_modify(|e| *e += val).or_insert(val);
            // hours
            let hr_ts = ts / 3600;
            self.hours.entry((code.clone(), hr_ts)).and_modify(|e| *e += val).or_insert(val);
            // days
            let day_ts = ts / 86400;
            self.days.entry((code.clone(), day_ts)).and_modify(|e| *e += val).or_insert(val);
            // 30-days
            let day30_ts = ts / 2592000;
            self.days30.entry((code.clone(), day30_ts)).and_modify(|e| *e += val).or_insert(val);
        }
    }

    fn is_empty(&self) -> bool {
        self.secs.is_empty()
    }
}

fn parse_variable_binary(
    packet: &[u8],
    pool: &mut HashSet<Arc<str>>,
    cache: &mut MetricCache,
    processed_metrics: &mut usize,
) {
    let mut offset = 0;
    while offset + 17 <= packet.len() {
        let name_len = packet[offset] as usize;
        if offset + 1 + name_len + 16 > packet.len() {
            break; // Partial metric or malformed buffer chunk
        }

        let name_bytes = &packet[offset + 1 .. offset + 1 + name_len];
        if let Ok(code_str) = std::str::from_utf8(name_bytes) {
            let val_offset = offset + 1 + name_len;
            if let (Some(val_bytes), Some(ts_bytes)) = (
                packet.get(val_offset .. val_offset + 8),
                packet.get(val_offset + 8 .. val_offset + 16)
            ) {
                let value = f64::from_le_bytes(val_bytes.try_into().unwrap());
                let at = i64::from_le_bytes(ts_bytes.try_into().unwrap());

                // Interning lookup: get reference-counted slice
                let code = if let Some(interned) = pool.get(code_str) {
                    interned.clone()
                } else {
                    let interned: Arc<str> = Arc::from(code_str);
                    pool.insert(interned.clone());
                    interned
                };

                cache.add(Metric { code, value, at });
                *processed_metrics += 1;
            }
        }
        offset += 1 + name_len + 16;
    }
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn stat_pg_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);

    let db_name = DB_NAME.get()
        .and_then(|c| c.into_string().ok())
        .unwrap_or_else(|| "postgres".to_string());
    let db_user = DB_USER.get()
        .and_then(|c| c.into_string().ok())
        .unwrap_or_else(|| "postgres".to_string());
    let bind_addr = BIND_ADDR.get()
        .and_then(|c| c.into_string().ok())
        .unwrap_or_else(|| "0.0.0.0".to_string());
    let port = PORT.get();
    let flush_sec = FLUSH_INTERVAL.get();

    BackgroundWorker::connect_worker_to_spi(Some(&db_name), Some(&db_user));

    let socket_addr = format!("{}:{}", bind_addr, port);
    let socket = match UdpSocket::bind(&socket_addr) {
        Ok(s) => {
            log!("stat-pg: bound UDP socket to {}", socket_addr);
            s
        }
        Err(e) => {
            log!("stat-pg: failed to bind UDP socket to {}: {}", socket_addr, e);
            return;
        }
    };

    if let Err(e) = socket.set_read_timeout(Some(Duration::from_millis(100))) {
        log!("stat-pg: failed to set read timeout on socket: {}", e);
        return;
    }

    let running = Arc::new(AtomicBool::new(true));
    let running_clone = running.clone();
    let (tx, rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) = channel();

    // Spawn high-speed UDP receiver thread
    // CRITICAL: We DO NOT call any pg_sys/pgrx functions (like worker_continue() or log!)
    // inside this spawned thread to prevent thread-local memory context segfaults.
    thread::spawn(move || {
        let mut buf = [0u8; 65536];

        while running_clone.load(Ordering::Relaxed) {
            match socket.recv_from(&mut buf) {
                Ok((amt, _src)) => {
                    let mut vec = vec![0u8; amt];
                    vec.copy_from_slice(&buf[..amt]);
                    let _ = tx.send(vec);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Timeout reached, loop again to check running state
                }
                Err(_e) => {
                    // Ignored receiver error
                }
            }
        }
    });

    let mut cache = MetricCache::default();
    let mut last_flush = SystemTime::now();
    let mut intern_pool = HashSet::new();

    while BackgroundWorker::worker_continue() {
        // 1. Process packet payloads from the channel (up to 500,000 metrics per iteration to prevent starvation)
        let mut processed_metrics = 0;
        while processed_metrics < 500_000 {
            if let Ok(packet_bytes) = rx.try_recv() {
                parse_variable_binary(&packet_bytes, &mut intern_pool, &mut cache, &mut processed_metrics);
            } else {
                break;
            }
        }

        // 2. Check if it's time to flush to the tables
        let now = SystemTime::now();
        let elapsed = now.duration_since(last_flush).unwrap_or(Duration::ZERO);
        if elapsed.as_secs_f64() >= flush_sec {
            if !cache.is_empty() {
                cache.populate_rollups(); // Run the rollup aggregation only once per second!
                let result = BackgroundWorker::transaction(|| {
                    Spi::connect_mut(|client| {
                        if let Err(e) = flush_tables(client, &cache) {
                            log!("stat-pg SPI flush error: {:?}", e);
                        }
                        Ok::<(), pgrx::spi::Error>(())
                    })
                });
                if let Err(e) = result {
                    log!("stat-pg SPI connection error: {:?}", e);
                }
                cache = MetricCache::default();
            }
            last_flush = now;
        }

        // 3. Briefly yield execution to avoid CPU pegging
        // Adaptive sleep latch: sleep only 1ms under load to maximize throughput, or 50ms when idle
        let sleep_dur = if processed_metrics > 0 {
            Duration::from_millis(1)
        } else {
            Duration::from_millis(50)
        };
        BackgroundWorker::wait_latch(Some(sleep_dur));
    }

    // Shut down spawned thread
    running.store(false, Ordering::Relaxed);
    log!("stat-pg background worker stopped");
}

fn flush_tables(client: &mut SpiClient<'_>, cache: &MetricCache) -> Result<(), pgrx::spi::Error> {
    flush_table(client, "evt_secs", &cache.secs)?;
    flush_table(client, "evt_mins", &cache.mins)?;
    flush_table(client, "evt_hours", &cache.hours)?;
    flush_table(client, "evt_days", &cache.days)?;
    flush_table(client, "evt_30s", &cache.days30)?;
    Ok(())
}

fn make_batch_insert_query(table_name: &str, batch_size: usize) -> String {
    let mut sql = format!("INSERT INTO {} (code, at, value) VALUES ", table_name);
    for i in 0..batch_size {
        if i > 0 {
            sql.push_str(", ");
        }
        let base = i * 3;
        sql.push_str(&format!("(${}, ${}, ${})", base + 1, base + 2, base + 3));
    }
    sql.push_str(&format!(
        " ON CONFLICT (at, code) DO UPDATE SET value = {}.value + EXCLUDED.value;",
        table_name
    ));
    sql
}

fn flush_table(
    client: &mut SpiClient<'_>,
    table_name: &str,
    data: &HashMap<(Arc<str>, i64), f64>,
) -> Result<(), pgrx::spi::Error> {
    if data.is_empty() {
        return Ok(());
    }

    let entries: Vec<(&(Arc<str>, i64), &f64)> = data.iter().collect();
    let mut index = 0;
    let total = entries.len();

    // Target bulk insertion chunk sizes
    let chunks = [5000, 500, 1];

    for &chunk_size in &chunks {
        if index >= total {
            break;
        }

        // Prepare the statement once for this chunk size if we have enough elements left
        if index + chunk_size <= total {
            let sql = make_batch_insert_query(table_name, chunk_size);
            
            // Build the argument type list
            let mut arg_types = Vec::with_capacity(chunk_size * 3);
            for _ in 0..chunk_size {
                arg_types.push(PgOid::from(pg_sys::TEXTOID));
                arg_types.push(PgOid::from(pg_sys::INT8OID));
                arg_types.push(PgOid::from(pg_sys::FLOAT8OID));
            }

            let prepared = client.prepare_mut(&sql, &arg_types)?;

            while index + chunk_size <= total {
                let mut args = Vec::with_capacity(chunk_size * 3);
                for i in 0..chunk_size {
                    let ((code, at), value) = entries[index + i];
                    args.push(DatumWithOid::from(code.as_ref()));
                    args.push(DatumWithOid::from(*at));
                    args.push(DatumWithOid::from(*value));
                }
                client.update(&prepared, None, &args)?;
                index += chunk_size;
            }
        }
    }

    Ok(())
}

// Unit Tests for packet parsing
#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_parse_variable_binary_valid() {
        let mut pool = HashSet::new();
        let mut cache = MetricCache::default();
        let mut processed = 0;

        // Build a packet with two metrics
        let mut packet = Vec::new();

        // Metric 1: "api.req" (7 chars), value 5.5, ts 1000
        packet.push(7u8);
        packet.extend_from_slice(b"api.req");
        packet.extend_from_slice(&5.5f64.to_le_bytes());
        packet.extend_from_slice(&1000i64.to_le_bytes());

        // Metric 2: "db.query" (8 chars), value 10.2, ts 2000
        packet.push(8u8);
        packet.extend_from_slice(b"db.query");
        packet.extend_from_slice(&10.2f64.to_le_bytes());
        packet.extend_from_slice(&2000i64.to_le_bytes());

        parse_variable_binary(&packet, &mut pool, &mut cache, &mut processed);

        assert_eq!(processed, 2);
        assert_eq!(*cache.secs.get(&(Arc::from("api.req"), 1000)).unwrap(), 5.5);
        assert_eq!(*cache.secs.get(&(Arc::from("db.query"), 2000)).unwrap(), 10.2);
    }

    #[test]
    fn test_parse_variable_binary_malformed() {
        let mut pool = HashSet::new();
        let mut cache = MetricCache::default();
        let mut processed = 0;

        // Build a partial metric
        let mut packet = Vec::new();
        packet.push(5u8);
        packet.extend_from_slice(b"short"); // missing value and ts

        parse_variable_binary(&packet, &mut pool, &mut cache, &mut processed);
        assert_eq!(processed, 0);
        assert!(cache.secs.is_empty());
    }
}

// Database Integration Tests running inside live PostgreSQL
#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use std::sync::Arc;

    #[pg_test]
    fn test_table_insertion_and_upsert() {
        Spi::connect_mut(|client| {
            // Verify our tables are created and we can flush into them
            let mut cache = crate::MetricCache::default();
            cache.add(crate::Metric {
                code: Arc::from("test_metric"),
                value: 4.5,
                at: 1000,
            });

            cache.populate_rollups();
            crate::flush_tables(client, &cache).expect("Flush tables failed");

            // Query check for seconds
            let val = client.select(
                "SELECT value::float8 FROM evt_secs WHERE code = 'test_metric' AND at = 1000;",
                None, &[]
            ).expect("Select failed")
            .first()
            .get_one::<f64>()
            .expect("Failed to get one")
            .expect("Expected a value");

            assert_eq!(val, 4.5);

            // Upsert (conflict update) check
            let mut cache2 = crate::MetricCache::default();
            cache2.add(crate::Metric {
                code: Arc::from("test_metric"),
                value: 5.5,
                at: 1000,
            });

            cache2.populate_rollups();
            crate::flush_tables(client, &cache2).expect("Flush tables failed");

            // Query check again (value should be aggregated: 4.5 + 5.5 = 10.0)
            let val2 = client.select(
                "SELECT value::float8 FROM evt_secs WHERE code = 'test_metric' AND at = 1000;",
                None, &[]
            ).expect("Select failed")
            .first()
            .get_one::<f64>()
            .expect("Failed to get one")
            .expect("Expected a value");

            assert_eq!(val2, 10.0);
        });
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
