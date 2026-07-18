use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::guc::*;
use pgrx::spi::SpiClient;
use pgrx::datum::DatumWithOid;
use crate::pg_sys::PgOid;
use std::ffi::CString;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::thread;

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

    // Load initial max ID from database to size the initial buffer
    let max_id_result = BackgroundWorker::transaction(|| {
        Spi::connect_mut(|client| {
            let table_exists: bool = client.select(
                "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'stat_pg_metrics');",
                None, &[]
            )?.first().get_one()?.unwrap_or(false);

            let id = if table_exists {
                client.select("SELECT COALESCE(MAX(id), 0) FROM stat_pg_metrics;", None, &[])?
                    .first()
                    .get_one::<i32>()?
                    .unwrap_or(0)
            } else {
                0
            };
            Ok::<i32, pgrx::spi::Error>(id)
        })
    });

    let max_id = max_id_result.unwrap_or(0);
    let max_valid_id = Arc::new(AtomicI32::new(max_id));
    let active_buffer = Arc::new(Mutex::new(vec![0.0; max_id as usize + 1]));

    let running = Arc::new(AtomicBool::new(true));

    // Spawn UDP listener thread
    let running_thread = running.clone();
    let active_buffer_clone = active_buffer.clone();
    let max_valid_id_clone = max_valid_id.clone();

    thread::spawn(move || {
        let socket_addr = format!("{}:{}", bind_addr, port);
        let socket = match UdpSocket::bind(&socket_addr) {
            Ok(s) => {
                eprintln!("stat-pg: bound UDP listener to {}", socket_addr);
                s
            }
            Err(e) => {
                eprintln!("stat-pg: failed to bind UDP listener to {}: {}", socket_addr, e);
                return;
            }
        };

        if let Err(e) = socket.set_read_timeout(Some(Duration::from_millis(100))) {
            eprintln!("stat-pg: failed to set UDP read timeout: {}", e);
            return;
        }

        let mut buf = vec![0u8; 65536];

        while running_thread.load(Ordering::Relaxed) {
            match socket.recv_from(&mut buf) {
                Ok((n, _src)) => {
                    // Parse 12-byte metric updates: [ID (4B LE i32)] [Value (8B LE f64)]
                    let mut offset = 0;
                    while offset + 12 <= n {
                        let id_bytes = &buf[offset..offset + 4];
                        let val_bytes = &buf[offset + 4..offset + 12];
                        
                        let id = i32::from_le_bytes(id_bytes.try_into().unwrap());
                        let value = f64::from_le_bytes(val_bytes.try_into().unwrap());

                        let max_id = max_valid_id_clone.load(Ordering::Relaxed) as usize;
                        if id > 0 && (id as usize) <= max_id {
                            let mut active = active_buffer_clone.lock().unwrap();
                            if (id as usize) < active.len() {
                                active[id as usize] += value;
                            }
                        }
                        offset += 12;
                    }
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    // Timeout, loop to check running state
                }
                Err(_) => {}
            }
        }
    });

    let mut last_flush = SystemTime::now();
    let mut local_flush_buffer = vec![0.0; max_id as usize + 1];

    while BackgroundWorker::worker_continue() {
        let now = SystemTime::now();
        let elapsed = now.duration_since(last_flush).unwrap_or(Duration::ZERO);
        if elapsed.as_secs_f64() >= flush_sec {
            let current_max = max_valid_id.load(Ordering::Relaxed) as usize;
            
            if local_flush_buffer.len() < current_max + 1 {
                local_flush_buffer.resize(current_max + 1, 0.0);
            }

            {
                let mut active = active_buffer.lock().unwrap();
                if active.len() < current_max + 1 {
                    active.resize(current_max + 1, 0.0);
                }
                std::mem::swap(&mut *active, &mut local_flush_buffer);
            }

            let at = now.duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs() as i64;

            let result = BackgroundWorker::transaction(|| {
                Spi::connect_mut(|client| {
                    let table_exists: bool = client.select(
                        "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'stat_pg_metrics');",
                        None, &[]
                    )?.first().get_one()?.unwrap_or(false);

                    if !table_exists {
                        return Ok(());
                    }

                    // 1. Refresh bounds by querying database max ID
                    let db_max_id: i32 = client.select(
                        "SELECT COALESCE(MAX(id), 0) FROM stat_pg_metrics;",
                        None, &[]
                    )?.first().get_one()?.unwrap_or(0);

                    max_valid_id.store(db_max_id, Ordering::Relaxed);

                    // 2. Flush slot data
                    flush_slot(client, at, &local_flush_buffer)?;

                    Ok::<(), pgrx::spi::Error>(())
                })
            });

            if let Err(e) = result {
                log!("stat-pg SPI transaction error: {:?}", e);
            }

            // Reset local_flush_buffer to 0.0
            for val in &mut local_flush_buffer {
                *val = 0.0;
            }
            last_flush = now;
        }

        BackgroundWorker::wait_latch(Some(Duration::from_millis(50)));
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
mod unit_tests {
    #[test]
    fn test_make_batch_insert_query() {
        let query = crate::make_batch_insert_query("evt_secs", 2);
        assert_eq!(
            query,
            "INSERT INTO evt_secs (metric_id, at, value) VALUES ($1, $2, $3), ($4, $5, $6) ON CONFLICT (at, metric_id) DO UPDATE SET value = evt_secs.value + EXCLUDED.value;"
        );
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
