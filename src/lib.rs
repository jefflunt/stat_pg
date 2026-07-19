use pgrx::bgworkers::*;
use pgrx::prelude::*;
use pgrx::guc::*;
use pgrx::spi::SpiClient;
use std::ffi::CString;
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
    CREATE UNLOGGED TABLE IF NOT EXISTS stat_pg_staging (
        metric_id INT NOT NULL,
        at BIGINT NOT NULL,
        value DECIMAL(20, 12) NOT NULL
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
static WORKERS: GucSetting<i32> = GucSetting::<i32>::new(4);

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

    GucRegistry::define_int_guc(
        c"stat_pg.workers",
        c"Number of aggregation workers",
        c"The number of background processes to spawn for UDP metric aggregation.",
        &WORKERS,
        1,
        16,
        GucContext::Sighup,
        GucFlags::default(),
    );

    let num_workers = WORKERS.get();
    for i in 0..num_workers {
        BackgroundWorkerBuilder::new(&format!("stat_pg_aggregator_{}", i))
            .set_function("stat_pg_main")
            .set_library("stat_pg")
            .set_start_time(BgWorkerStartTime::PostmasterStart)
            .set_restart_time(Some(Duration::from_secs(10)))
            .set_argument(Some(pg_sys::Datum::from(i as usize)))
            .enable_spi_access()
            .load();
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

    let pid = std::process::id();
    let filename = format!("/tmp/flush_{}.csv", pid);
    
    use std::io::{BufWriter, Write};
    if let Ok(file) = std::fs::File::create(&filename) {
        let mut writer = BufWriter::new(file);
        for (metric_id, value) in active_entries {
            let _ = writeln!(writer, "{},{},{}", metric_id, ts, value);
        }
        let _ = writer.flush();
    }

    let copy_query = format!(
        "COPY stat_pg_staging (metric_id, at, value) FROM '{}' WITH (FORMAT csv);",
        filename
    );
    client.update(&copy_query, None, &[])?;

    let _ = std::fs::remove_file(&filename);

    Ok(())
}

fn coordinator_merge(client: &mut SpiClient<'_>) -> Result<(), pgrx::spi::Error> {
    let has_data: bool = client.select(
        "SELECT EXISTS (SELECT FROM stat_pg_staging LIMIT 1);",
        None, &[]
    )?.first().get_one()?.unwrap_or(false);

    if !has_data {
        return Ok(());
    }

    // 1. Snapshot the staging table into a temporary table and truncate staging immediately
    client.update(
        "CREATE TEMPORARY TABLE stat_pg_temp ON COMMIT DROP AS
         SELECT metric_id, at, value FROM stat_pg_staging;",
        None, &[]
    )?;
    client.update("TRUNCATE stat_pg_staging;", None, &[])?;

    // 2. Merge into evt_secs from temp table
    client.update(
        "INSERT INTO evt_secs (metric_id, at, value)
         SELECT metric_id, at, SUM(value) FROM stat_pg_temp GROUP BY metric_id, at
         ON CONFLICT (at, metric_id) DO UPDATE SET value = evt_secs.value + EXCLUDED.value;",
        None, &[]
    )?;

    // 3. Merge into evt_mins from temp table
    client.update(
        "INSERT INTO evt_mins (metric_id, at, value)
         SELECT metric_id, at / 60, SUM(value) FROM stat_pg_temp GROUP BY metric_id, at / 60
         ON CONFLICT (at, metric_id) DO UPDATE SET value = evt_mins.value + EXCLUDED.value;",
        None, &[]
    )?;

    // 4. Merge into evt_hours from temp table
    client.update(
        "INSERT INTO evt_hours (metric_id, at, value)
         SELECT metric_id, at / 3600, SUM(value) FROM stat_pg_temp GROUP BY metric_id, at / 3600
         ON CONFLICT (at, metric_id) DO UPDATE SET value = evt_hours.value + EXCLUDED.value;",
        None, &[]
    )?;

    // 5. Merge into evt_days from temp table
    client.update(
        "INSERT INTO evt_days (metric_id, at, value)
         SELECT metric_id, at / 86400, SUM(value) FROM stat_pg_temp GROUP BY metric_id, at / 86400
         ON CONFLICT (at, metric_id) DO UPDATE SET value = evt_days.value + EXCLUDED.value;",
        None, &[]
    )?;

    // 6. Merge into evt_30s from temp table
    client.update(
        "INSERT INTO evt_30s (metric_id, at, value)
         SELECT metric_id, at / 2592000, SUM(value) FROM stat_pg_temp GROUP BY metric_id, at / 2592000
         ON CONFLICT (at, metric_id) DO UPDATE SET value = evt_30s.value + EXCLUDED.value;",
        None, &[]
    )?;

    // 7. Clean up temporary table
    client.update("DROP TABLE stat_pg_temp;", None, &[])?;

    Ok(())
}



#[cfg(target_os = "linux")]
fn recvmmsg_sys(
    socket: &std::net::UdpSocket,
    bufs: &mut [[u8; 2048]],
    lengths: &mut [usize],
    nonblocking: bool,
) -> std::io::Result<usize> {
    use std::os::unix::io::AsRawFd;
    let fd = socket.as_raw_fd();
    let num_buffers = bufs.len();

    let mut iovecs: Vec<libc::iovec> = bufs.iter_mut().map(|buf| {
        libc::iovec {
            iov_base: buf.as_mut_ptr() as *mut libc::c_void,
            iov_len: buf.len(),
        }
    }).collect();

    let mut mmsg: Vec<libc::mmsghdr> = iovecs.iter_mut().map(|iov| {
        libc::mmsghdr {
            msg_hdr: libc::msghdr {
                msg_name: std::ptr::null_mut(),
                msg_namelen: 0,
                msg_iov: iov as *mut libc::iovec,
                msg_iovlen: 1,
                msg_control: std::ptr::null_mut(),
                msg_controllen: 0,
                msg_flags: 0,
            },
            msg_len: 0,
        }
    }).collect();

    let flags = if nonblocking { libc::MSG_DONTWAIT } else { 0 };

    let res = unsafe {
        libc::recvmmsg(
            fd,
            mmsg.as_mut_ptr(),
            num_buffers as libc::c_uint,
            flags,
            std::ptr::null_mut(),
        )
    };

    if res < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() == std::io::ErrorKind::WouldBlock {
            return Ok(0);
        }
        return Err(err);
    }

    let count = res as usize;
    for i in 0..count {
        lengths[i] = mmsg[i].msg_len as usize;
    }

    Ok(count)
}

#[cfg(target_os = "linux")]
fn recv_batch_blocking(
    socket: &std::net::UdpSocket,
    bufs: &mut [[u8; 2048]],
    lengths: &mut [usize],
) -> std::io::Result<usize> {
    recvmmsg_sys(socket, bufs, lengths, false)
}

#[cfg(target_os = "linux")]
fn recv_batch_nonblocking(
    socket: &std::net::UdpSocket,
    bufs: &mut [[u8; 2048]],
    lengths: &mut [usize],
) -> std::io::Result<usize> {
    recvmmsg_sys(socket, bufs, lengths, true)
}

#[cfg(not(target_os = "linux"))]
fn recv_batch_blocking(
    socket: &std::net::UdpSocket,
    bufs: &mut [[u8; 2048]],
    lengths: &mut [usize],
) -> std::io::Result<usize> {
    match socket.recv_from(&mut bufs[0]) {
        Ok((n, _src)) => {
            lengths[0] = n;
            Ok(1)
        }
        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => Ok(0),
        Err(e) => Err(e),
    }
}

#[cfg(not(target_os = "linux"))]
fn recv_batch_nonblocking(
    _socket: &std::net::UdpSocket,
    _bufs: &mut [[u8; 2048]],
    _lengths: &mut [usize],
) -> std::io::Result<usize> {
    // Non-blocking fallback is not used on non-Linux systems to avoid fcntl overhead
    Ok(0)
}

#[pg_guard]
#[no_mangle]
pub extern "C-unwind" fn stat_pg_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGHUP | SignalWakeFlags::SIGTERM);
    let worker_index = usize::from(_arg) as i32;

    let mut guard = pprof::ProfilerGuardBuilder::default()
        .frequency(99)
        .blocklist(&["libc", "libgcc", "pthread"])
        .build()
        .ok();

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
        let addr: std::net::SocketAddr = match socket_addr.parse() {
            Ok(a) => a,
            Err(e) => {
                eprintln!("stat-pg: failed to parse socket address {}: {}", socket_addr, e);
                return;
            }
        };

        let socket = match socket2::Socket::new(
            socket2::Domain::IPV4,
            socket2::Type::DGRAM,
            Some(socket2::Protocol::UDP),
        ) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("stat-pg: failed to create socket: {}", e);
                return;
            }
        };

        let _ = socket.set_reuse_address(true);
        #[cfg(unix)]
        {
            use std::os::unix::io::AsRawFd;
            let optval: libc::c_int = 1;
            let _ = unsafe {
                libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_REUSEPORT,
                    &optval as *const libc::c_int as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
        }
        if let Err(e) = socket.set_recv_buffer_size(256 * 1024 * 1024) {
            eprintln!("stat-pg: failed to set UDP receive buffer size (using OS max): {}", e);
        }

        let socket = match socket.bind(&addr.into()) {
            Ok(_) => {
                let std_sock: std::net::UdpSocket = socket.into();
                eprintln!("stat-pg: bound UDP listener to {}", socket_addr);
                std_sock
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

        let mut bufs = vec![[0u8; 2048]; 32];
        let mut lengths = vec![0; 32];

        while running_thread.load(Ordering::Relaxed) {
            match recv_batch_blocking(&socket, &mut bufs, &mut lengths) {
                Ok(count) if count > 0 => {
                    let mut active = active_buffer_clone.lock().unwrap();
                    let max_id = max_valid_id_clone.load(Ordering::Relaxed) as usize;

                    // Process the first batch
                    for i in 0..count {
                        let n = lengths[i];
                        let mut offset = 0;
                        while offset + 12 <= n {
                            let id_bytes = &bufs[i][offset..offset + 4];
                            let val_bytes = &bufs[i][offset + 4..offset + 12];
                            let id = i32::from_le_bytes(id_bytes.try_into().unwrap());
                            let value = f64::from_le_bytes(val_bytes.try_into().unwrap());
                            if id > 0 && (id as usize) <= max_id && (id as usize) < active.len() {
                                active[id as usize] += value;
                            }
                            offset += 12;
                        }
                    }

                    // Drain loop: fetch any other packets in the queue using non-blocking calls
                    loop {
                        match recv_batch_nonblocking(&socket, &mut bufs, &mut lengths) {
                            Ok(drain_count) if drain_count > 0 => {
                                for i in 0..drain_count {
                                    let n = lengths[i];
                                    let mut offset = 0;
                                    while offset + 12 <= n {
                                        let id_bytes = &bufs[i][offset..offset + 4];
                                        let val_bytes = &bufs[i][offset + 4..offset + 12];
                                        let id = i32::from_le_bytes(id_bytes.try_into().unwrap());
                                        let value = f64::from_le_bytes(val_bytes.try_into().unwrap());
                                        if id > 0 && (id as usize) <= max_id && (id as usize) < active.len() {
                                            active[id as usize] += value;
                                        }
                                        offset += 12;
                                    }
                                }
                            }
                            _ => break,
                        }
                    }
                }
                _ => {} // Timeout or error, loop again to check running state
            }
        }
    });

    let mut last_flush = SystemTime::now();
    let mut local_flush_buffer = vec![0.0; max_id as usize + 1];

    while BackgroundWorker::worker_continue() {
        if unsafe { pgrx::pg_sys::ShutdownRequestPending } != 0 {
            if let Some(g) = guard.take() {
                if let Ok(report) = g.report().build() {
                    let file_path = format!("/workspace/flamegraph_{}.svg", std::process::id());
                    if let Ok(file) = std::fs::File::create(&file_path) {
                        let _ = report.flamegraph(file);
                    }
                }
            }
            std::process::exit(0);
        }
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

            if !BackgroundWorker::worker_continue() {
                break;
            }

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

                    // 3. Coordinator worker merges staging table into rollup tables
                    if worker_index == 0 {
                        coordinator_merge(client)?;
                    }

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
    if let Some(g) = guard.take() {
        if let Ok(report) = g.report().build() {
            let file_path = format!("/workspace/flamegraph_{}.svg", std::process::id());
            if let Ok(file) = std::fs::File::create(&file_path) {
                let _ = report.flamegraph(file);
            }
        }
    }
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
            crate::coordinator_merge(client).expect("Merge failed");

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
            crate::coordinator_merge(client).expect("Merge failed");

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
