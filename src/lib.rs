use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::guc::*;
use pgrx::spi::SpiClient;
use pgrx::datum::DatumWithOid;
use crate::pg_sys::PgOid;
use std::collections::HashMap;
use std::ffi::CString;
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;
use std::panic::AssertUnwindSafe;

pgrx::pg_module_magic!();

// Tables definition using extension_sql!
extension_sql!(
    r#"
    CREATE TABLE IF NOT EXISTS stat_pg_metrics (
        id SERIAL PRIMARY KEY,
        name VARCHAR NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS evt_secs (
        metric_id INT NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, metric_id)
    );
    CREATE TABLE IF NOT EXISTS evt_mins (
        metric_id INT NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, metric_id)
    );
    CREATE TABLE IF NOT EXISTS evt_hours (
        metric_id INT NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, metric_id)
    );
    CREATE TABLE IF NOT EXISTS evt_days (
        metric_id INT NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, metric_id)
    );
    CREATE TABLE IF NOT EXISTS evt_30s (
        metric_id INT NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL,
        PRIMARY KEY (at, metric_id)
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
static BUFFER_WINDOW: GucSetting<i32> = GucSetting::<i32>::new(5);

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
        c"IP address to bind the TCP listener to",
        c"The local IP address that the TCP listener socket binds to.",
        &BIND_ADDR,
        GucContext::Sighup,
        GucFlags::default(),
    );

    GucRegistry::define_int_guc(
        c"stat_pg.port",
        c"TCP port to listen on",
        c"The local TCP port that the listener socket binds to.",
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

    GucRegistry::define_int_guc(
        c"stat_pg.buffer_window",
        c"Aggregator sliding window size in seconds",
        c"The sliding window size (in seconds) to buffer metrics and absorb network delay or jitter.",
        &BUFFER_WINDOW,
        1,
        60,
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

struct RegRequest {
    name: String,
    resp: Sender<i32>,
}

struct Slot {
    timestamp: i64,
    data: Vec<f64>,
}

struct SlidingWindow {
    window_size: usize,
    slots: Vec<Slot>,
    max_timestamp: i64,
}

impl SlidingWindow {
    fn new(window_size: usize) -> Self {
        let mut slots = Vec::with_capacity(window_size);
        for _ in 0..window_size {
            slots.push(Slot {
                timestamp: 0,
                data: Vec::new(),
            });
        }
        Self {
            window_size,
            slots,
            max_timestamp: 0,
        }
    }

    fn add(&mut self, metric_id: usize, value: f64, ts: i64, client: &mut SpiClient<'_>) -> Result<(), pgrx::spi::Error> {
        let w = self.window_size;

        if ts > self.max_timestamp {
            if self.max_timestamp == 0 {
                self.max_timestamp = ts;
                for i in 0..w {
                    self.slots[i].timestamp = ts - (w as i64 - 1) + (i as i64);
                }
            } else {
                for t in (self.max_timestamp + 1)..=ts {
                    let idx = (t % w as i64) as usize;
                    if self.slots[idx].timestamp > 0 {
                        flush_slot(client, self.slots[idx].timestamp, &self.slots[idx].data)?;
                    }
                    self.slots[idx].timestamp = t;
                    self.slots[idx].data.clear();
                }
                self.max_timestamp = ts;
            }
        }

        let min_ts = self.max_timestamp - w as i64 + 1;
        if ts >= min_ts && ts <= self.max_timestamp {
            let idx = (ts % w as i64) as usize;
            if self.slots[idx].timestamp == ts {
                if metric_id >= self.slots[idx].data.len() {
                    self.slots[idx].data.resize(metric_id + 1, 0.0);
                }
                self.slots[idx].data[metric_id] += value;
            }
        } else {
            // Packet is older than our window - execute directly to prevent data loss
            let mut single_data = Vec::new();
            single_data.resize(metric_id + 1, 0.0);
            single_data[metric_id] = value;
            flush_slot(client, ts, &single_data)?;
        }

        Ok(())
    }

    fn flush_expired(&mut self, client: &mut SpiClient<'_>, max_age_seconds: i64) -> Result<(), pgrx::spi::Error> {
        let now_sec = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs() as i64;
        
        let oldest_allowed = now_sec - max_age_seconds;

        for slot in &mut self.slots {
            if slot.timestamp > 0 && slot.timestamp < oldest_allowed {
                let has_data = slot.data.iter().any(|&v| v > 0.0);
                if has_data {
                    flush_slot(client, slot.timestamp, &slot.data)?;
                }
                slot.timestamp = 0;
                slot.data.clear();
            }
        }
        Ok(())
    }
}

fn flush_slot(client: &mut SpiClient<'_>, ts: i64, data: &[f64]) -> Result<(), pgrx::spi::Error> {
    let mut active_entries = Vec::new();
    for (metric_id, &value) in data.iter().enumerate() {
        if value > 0.0 {
            active_entries.push((metric_id as i32, value));
        }
    }
    if active_entries.is_empty() {
        return Ok(());
    }
    
    // Aggregate values to all rollup periods and flush
    flush_table_slice(client, "evt_secs", ts, &active_entries)?;
    flush_table_slice(client, "evt_mins", ts / 60, &active_entries)?;
    flush_table_slice(client, "evt_hours", ts / 3600, &active_entries)?;
    flush_table_slice(client, "evt_days", ts / 86400, &active_entries)?;
    flush_table_slice(client, "evt_30s", ts / 2592000, &active_entries)?;

    Ok(())
}

fn make_batch_insert_query(table_name: &str, batch_size: usize) -> String {
    let mut sql = format!("INSERT INTO {} (metric_id, at, value) VALUES ", table_name);
    for i in 0..batch_size {
        if i > 0 {
            sql.push_str(", ");
        }
        let base = i * 3;
        sql.push_str(&format!("(${}, ${}, ${})", base + 1, base + 2, base + 3));
    }
    sql.push_str(&format!(
        " ON CONFLICT (at, metric_id) DO UPDATE SET value = {}.value + EXCLUDED.value;",
        table_name
    ));
    sql
}

fn flush_table_slice(
    client: &mut SpiClient<'_>,
    table_name: &str,
    ts: i64,
    entries: &[(i32, f64)],
) -> Result<(), pgrx::spi::Error> {
    let mut index = 0;
    let total = entries.len();
    let chunks = [5000, 500, 1];

    for &chunk_size in &chunks {
        if index >= total {
            break;
        }

        if index + chunk_size <= total {
            let sql = make_batch_insert_query(table_name, chunk_size);
            
            let mut arg_types = Vec::with_capacity(chunk_size * 3);
            for _ in 0..chunk_size {
                arg_types.push(PgOid::from(pg_sys::INT4OID));
                arg_types.push(PgOid::from(pg_sys::INT8OID));
                arg_types.push(PgOid::from(pg_sys::FLOAT8OID));
            }

            let prepared = client.prepare_mut(&sql, &arg_types)?;

            while index + chunk_size <= total {
                let mut args = Vec::with_capacity(chunk_size * 3);
                for i in 0..chunk_size {
                    let (metric_id, value) = entries[index + i];
                    args.push(DatumWithOid::from(metric_id));
                    args.push(DatumWithOid::from(ts));
                    args.push(DatumWithOid::from(value));
                }
                client.update(&prepared, None, &args)?;
                index += chunk_size;
            }
        }
    }
    Ok(())
}

fn handle_client(
    mut stream: std::net::TcpStream,
    shared_cache: Arc<std::sync::RwLock<HashMap<String, i32>>>,
    reg_tx: Sender<RegRequest>,
    metric_tx: Sender<(i32, f64, i64)>,
    running: Arc<AtomicBool>,
) {
    use std::io::Read;
    use std::io::Write;

    let mut buf = vec![0u8; 65536];
    let mut read_buf = Vec::new();

    if let Err(_) = stream.set_read_timeout(Some(Duration::from_millis(100))) {
        return;
    }

    while running.load(Ordering::Relaxed) {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                read_buf.extend_from_slice(&buf[..n]);
                
                let mut offset = 0;
                while offset < read_buf.len() {
                    let msg_type = read_buf[offset];
                    if msg_type == 0x01 {
                        if offset + 3 > read_buf.len() {
                            break;
                        }
                        let name_len = u16::from_be_bytes([read_buf[offset + 1], read_buf[offset + 2]]) as usize;
                        if offset + 3 + name_len > read_buf.len() {
                            break;
                        }
                        
                        let name_bytes = &read_buf[offset + 3 .. offset + 3 + name_len];
                        if let Ok(name_str) = std::str::from_utf8(name_bytes) {
                            let mut id = {
                                let read_guard = shared_cache.read().unwrap();
                                read_guard.get(name_str).cloned()
                            };
                            
                            if id.is_none() {
                                let (tx_resp, rx_resp) = channel();
                                let req = RegRequest {
                                    name: name_str.to_string(),
                                    resp: tx_resp,
                                };
                                if reg_tx.send(req).is_ok() {
                                    id = rx_resp.recv().ok();
                                }
                            }
                            
                            let final_id = id.unwrap_or(-1);
                            
                            let mut resp = Vec::with_capacity(3 + name_len + 4);
                            resp.push(0x01);
                            resp.extend_from_slice(&(name_len as u16).to_be_bytes());
                            resp.extend_from_slice(name_bytes);
                            resp.extend_from_slice(&final_id.to_le_bytes());
                            let _ = stream.write_all(&resp);
                        }
                        offset += 3 + name_len;
                    } else if msg_type == 0x02 {
                        if offset + 21 > read_buf.len() {
                            break;
                        }
                        let id_bytes = &read_buf[offset + 1 .. offset + 5];
                        let val_bytes = &read_buf[offset + 5 .. offset + 13];
                        let ts_bytes = &read_buf[offset + 13 .. offset + 21];
                        
                        let metric_id = i32::from_le_bytes(id_bytes.try_into().unwrap());
                        let value = f64::from_le_bytes(val_bytes.try_into().unwrap());
                        let at = i64::from_le_bytes(ts_bytes.try_into().unwrap());
                        
                        let _ = metric_tx.send((metric_id, value, at));
                        offset += 21;
                    } else {
                        offset += 1;
                    }
                }
                
                if offset > 0 {
                    read_buf.drain(..offset);
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // Timeout, loop to check running state
            }
            Err(_) => break,
        }
    }
}

// Wrapper structure for thread worker state captured by the transaction closure
struct WorkerState {
    sliding_window: SlidingWindow,
    last_flush: SystemTime,
    metrics_processed: i32,
    spi_err: Option<pgrx::spi::SpiError>,
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

    // Load existing metrics lookup cache from database
    let name_to_id_result = BackgroundWorker::transaction(|| {
        let mut cache = HashMap::new();
        Spi::connect_mut(|client| {
            let table_exists: bool = client.select(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'stat_pg_metrics');",
                None, &[]
            )?.first().get_one()?.unwrap_or(false);

            if table_exists {
                let tuples = client.select("SELECT id, name FROM stat_pg_metrics;", None, &[])?;
                for row in tuples {
                    let id: i32 = row.get_by_name("id")?.expect("id field");
                    let name: String = row.get_by_name("name")?.expect("name field");
                    cache.insert(name, id);
                }
            }
            Ok::<(), pgrx::spi::Error>(())
        })?;
        Ok::<HashMap<String, i32>, pgrx::spi::Error>(cache)
    });

    let name_to_id = match name_to_id_result {
        Ok(cache) => cache,
        Err(e) => {
            log!("stat-pg: failed to load existing metrics cache: {:?}", e);
            HashMap::new()
        }
    };
    let shared_cache = Arc::new(std::sync::RwLock::new(name_to_id));

    let running = Arc::new(AtomicBool::new(true));

    let (reg_tx, reg_rx) = channel::<RegRequest>();
    let (metric_tx, metric_rx) = channel::<(i32, f64, i64)>();

    // Spawn TCP listener thread
    let shared_cache_clone = shared_cache.clone();
    let reg_tx_clone = reg_tx.clone();
    let metric_tx_clone = metric_tx.clone();
    let running_thread = running.clone();

    thread::spawn(move || {
        let socket_addr = format!("{}:{}", bind_addr, port);
        let listener = match TcpListener::bind(&socket_addr) {
            Ok(l) => {
                eprintln!("stat-pg: bound TCP listener to {}", socket_addr);
                l
            }
            Err(e) => {
                eprintln!("stat-pg: failed to bind TCP listener to {}: {}", socket_addr, e);
                return;
            }
        };

        if let Err(e) = listener.set_nonblocking(true) {
            eprintln!("stat-pg: failed to set TCP listener non-blocking: {}", e);
            return;
        }

        while running_thread.load(Ordering::Relaxed) {
            match listener.accept() {
                Ok((stream, _addr)) => {
                    let cache = shared_cache_clone.clone();
                    let r_tx = reg_tx_clone.clone();
                    let m_tx = metric_tx_clone.clone();
                    let run = running_thread.clone();
                    thread::spawn(move || {
                        handle_client(stream, cache, r_tx, m_tx, run);
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(50));
                }
                Err(_) => {}
            }
        }
    });

    let buffer_window = BUFFER_WINDOW.get() as usize;

    let mut state = WorkerState {
        sliding_window: SlidingWindow::new(buffer_window),
        last_flush: SystemTime::now(),
        metrics_processed: 0,
        spi_err: None,
    };

    // Use a raw pointer wrapped in AssertUnwindSafe to safely share state
    // across the UnwindSafe transaction closures without capturing mut references.
    let state_ptr = &mut state as *mut WorkerState;
    let state_safe = AssertUnwindSafe(state_ptr);

    while BackgroundWorker::worker_continue() {
        // 1. Process dynamic metric registration requests from clients
        while let Ok(req) = reg_rx.try_recv() {
            let id = {
                let read_guard = shared_cache.read().unwrap();
                read_guard.get(&req.name).cloned()
            };

            if let Some(existing_id) = id {
                let _ = req.resp.send(existing_id);
            } else {
                let result = BackgroundWorker::transaction(|| {
                    Spi::connect_mut(|client| {
                        let query = "INSERT INTO stat_pg_metrics (name) VALUES ($1) \
                                     ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name \
                                     RETURNING id;";
                        let args = vec![DatumWithOid::from(req.name.as_str())];
                        let id = client.update(query, None, &args)?
                            .first()
                            .get_one::<i32>()?
                            .expect("Expected OID");
                        Ok::<i32, pgrx::spi::Error>(id)
                    })
                });
                match result {
                    Ok(new_id) => {
                        let mut write_guard = shared_cache.write().unwrap();
                        write_guard.insert(req.name.clone(), new_id);
                        let _ = req.resp.send(new_id);
                    }
                    Err(e) => {
                        log!("stat-pg: failed to register metric name: {:?}", e);
                        let _ = req.resp.send(-1);
                    }
                }
            }
        }

        // 2. Connect to SPI to aggregate incoming metrics and flush
        let result = BackgroundWorker::transaction(|| {
            // SAFETY: Single-threaded background worker main loop execution
            let s = unsafe { &mut *state_safe.0 };

            Spi::connect_mut(|client| {
                s.metrics_processed = 0;
                while s.metrics_processed < 500_000 {
                    if let Ok((metric_id, value, at)) = metric_rx.try_recv() {
                        if let Err(e) = s.sliding_window.add(metric_id as usize, value, at, client) {
                            s.spi_err = Some(e);
                            break;
                        }
                        s.metrics_processed += 1;
                    } else {
                        break;
                    }
                }

                let now = SystemTime::now();
                let elapsed = now.duration_since(s.last_flush).unwrap_or(Duration::ZERO);
                if elapsed.as_secs_f64() >= flush_sec {
                    if let Err(e) = s.sliding_window.flush_expired(client, buffer_window as i64) {
                        s.spi_err = Some(e);
                    }
                    s.last_flush = now;
                }

                Ok::<(), pgrx::spi::Error>(())
            })
        });

        if let Err(e) = result {
            log!("stat-pg SPI connection transaction error: {:?}", e);
        }
        if let Some(ref e) = state.spi_err {
            log!("stat-pg SPI execution error: {:?}", e);
        }

        let sleep_dur = if state.metrics_processed > 0 {
            Duration::from_millis(1)
        } else {
            Duration::from_millis(50)
        };
        BackgroundWorker::wait_latch(Some(sleep_dur));
    }

    running.store(false, Ordering::Relaxed);
    log!("stat-pg background worker stopped");
}

// Database Integration Tests running inside live PostgreSQL
#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_table_insertion_and_upsert() {
        Spi::connect_mut(|client| {
            // 1. Insert metric names lookup
            client.update(
                "INSERT INTO stat_pg_metrics (id, name) VALUES (1, 'test_metric') ON CONFLICT DO NOTHING;",
                None, &[]
            ).unwrap();

            // 2. Perform aggregate flush of a slot
            let data = vec![0.0, 4.5]; // index 1 is test_metric
            crate::flush_slot(client, 1000, &data).expect("Flush slot failed");

            // Query check for seconds
            let val = client.select(
                "SELECT value::float8 FROM evt_secs WHERE metric_id = 1 AND at = 1000;",
                None, &[]
            ).expect("Select failed")
            .first()
            .get_one::<f64>()
            .expect("Failed to get one")
            .expect("Expected a value");

            assert_eq!(val, 4.5);

            // Upsert (conflict update) check
            let data2 = vec![0.0, 5.5];
            crate::flush_slot(client, 1000, &data2).expect("Flush slot failed");

            // Query check again (value should be aggregated: 4.5 + 5.5 = 10.0)
            let val2 = client.select(
                "SELECT value::float8 FROM evt_secs WHERE metric_id = 1 AND at = 1000;",
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
