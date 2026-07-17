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
use std::time::{Duration, SystemTime, UNIX_EPOCH};
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

        // seconds
        self.secs.entry((code.clone(), ts)).and_modify(|e| *e += val).or_insert(val);
        // minutes
        let min_ts = ts / 60;
        self.mins.entry((code.clone(), min_ts)).and_modify(|e| *e += val).or_insert(val);
        // hours
        let hr_ts = ts / 3600;
        self.hours.entry((code.clone(), hr_ts)).and_modify(|e| *e += val).or_insert(val);
        // days
        let day_ts = ts / 86400;
        self.days.entry((code.clone(), day_ts)).and_modify(|e| *e += val).or_insert(val);
        // 30-days (corrected interval to 2592000 seconds)
        let day30_ts = ts / 2592000;
        self.days30.entry((code, day30_ts)).and_modify(|e| *e += val).or_insert(val);
    }

    fn is_empty(&self) -> bool {
        self.secs.is_empty()
    }
}

fn parse_packet(payload: &str, pool: &mut HashSet<Arc<str>>) -> Option<Metric> {
    let mut parts = payload.split_whitespace();
    let code_str = parts.next()?;
    
    // Limit maximum code name length to 64
    if code_str.len() > 64 {
        return None;
    }

    let value_str = parts.next()?;
    let value: f64 = value_str.parse().ok()?;
    
    // timestamp is optional; fallback to current time
    let at = if let Some(ts_str) = parts.next() {
        ts_str.parse::<i64>().ok()?
    } else {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_secs() as i64
    };
    
    // String interning logic: reuse existing Arc<str> if present in the local pool
    let code = if let Some(interned) = pool.get(code_str) {
        interned.clone()
    } else {
        let interned: Arc<str> = Arc::from(code_str);
        pool.insert(interned.clone());
        interned
    };

    Some(Metric { code, value, at })
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
    let (tx, rx): (Sender<Metric>, Receiver<Metric>) = channel();

    // Spawn high-speed UDP receiver thread
    // CRITICAL: We DO NOT call any pg_sys/pgrx functions (like worker_continue() or log!)
    // inside this spawned thread to prevent thread-local memory context segfaults.
    thread::spawn(move || {
        let mut buf = [0u8; 4096];
        // Initialize thread-local string interning pool to reuse Arc<str> refs
        let mut intern_pool = HashSet::new();

        while running_clone.load(Ordering::Relaxed) {
            match socket.recv_from(&mut buf) {
                Ok((amt, _src)) => {
                    if let Ok(payload) = std::str::from_utf8(&buf[..amt]) {
                        for line in payload.lines() {
                            if let Some(metric) = parse_packet(line.trim(), &mut intern_pool) {
                                let _ = tx.send(metric);
                            }
                        }
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Timeout reached, loop again to check running state
                }
                Err(_e) => {
                    // Ignored receiver error to avoid calling pg_sys logging inside a thread
                }
            }
        }
    });

    let mut cache = MetricCache::default();
    let mut last_flush = SystemTime::now();

    while BackgroundWorker::worker_continue() {
        // 1. Process metrics from the channel (up to 100,000 at a time to prevent flusher starvation)
        let mut processed = 0;
        while processed < 100_000 {
            if let Ok(metric) = rx.try_recv() {
                cache.add(metric);
                processed += 1;
            } else {
                break;
            }
        }

        // 2. Check if it's time to flush to the tables
        let now = SystemTime::now();
        let elapsed = now.duration_since(last_flush).unwrap_or(Duration::ZERO);
        if elapsed.as_secs_f64() >= flush_sec {
            if !cache.is_empty() {
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
        BackgroundWorker::wait_latch(Some(Duration::from_millis(50)));
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

fn flush_table(
    client: &mut SpiClient<'_>,
    table_name: &str,
    data: &HashMap<(Arc<str>, i64), f64>,
) -> Result<(), pgrx::spi::Error> {
    for ((code, at), value) in data {
        let query = format!(
            "INSERT INTO {} (code, at, value) VALUES ($1, $2, $3) \
             ON CONFLICT (at, code) DO UPDATE SET value = {}.value + EXCLUDED.value;",
            table_name, table_name
        );
        let args = vec![
            DatumWithOid::from(code.as_ref()),
            DatumWithOid::from(*at),
            DatumWithOid::from(*value),
        ];
        client.update(&query, None, &args)?;
    }
    Ok(())
}

// Unit Tests for packet parsing
#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_parse_packet_valid() {
        let mut pool = HashSet::new();
        let parsed = parse_packet("api.request 5.5 1721237340", &mut pool);
        assert!(parsed.is_some());
        let m = parsed.unwrap();
        assert_eq!(m.code.as_ref(), "api.request");
        assert_eq!(m.value, 5.5);
        assert_eq!(m.at, 1721237340);
    }

    #[test]
    fn test_parse_packet_optional_ts() {
        let mut pool = HashSet::new();
        let parsed = parse_packet("api.request 10", &mut pool);
        assert!(parsed.is_some());
        let m = parsed.unwrap();
        assert_eq!(m.code.as_ref(), "api.request");
        assert_eq!(m.value, 10.0);
        assert!(m.at > 0);
    }

    #[test]
    fn test_parse_packet_invalid() {
        let mut pool = HashSet::new();
        assert!(parse_packet("malformed_input", &mut pool).is_none());
        assert!(parse_packet("api.request abc", &mut pool).is_none());
    }

    #[test]
    fn test_parse_packet_length_limit() {
        let mut pool = HashSet::new();
        // 64-char string is valid
        let valid_code = "a".repeat(64);
        assert!(parse_packet(&format!("{} 1", valid_code), &mut pool).is_some());

        // 65-char string is invalid
        let invalid_code = "a".repeat(65);
        assert!(parse_packet(&format!("{} 1", invalid_code), &mut pool).is_none());
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
