use std::collections::HashMap;
use std::env;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;

struct SimpleRng {
    state: u64,
}

impl SimpleRng {
    fn new(seed: u64) -> Self {
        let mut rng = Self { state: seed };
        if rng.state == 0 {
            rng.state = 0xDEADC0DE;
        }
        rng
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_f64(&mut self) -> f64 {
        (self.next_u64() & 0xFFFFFFFFFFFFF) as f64 / 9007199254740992.0
    }
}

fn print_help() {
    println!(
        "stat-pg-bench - High-speed TCP stress test benchmark tool\n\n\
         Usage: stat-pg-bench [OPTIONS]\n\n\
         Options:\n  \
           --host <str>      Target host IP (default: 127.0.0.1)\n  \
           --port <u16>      Target port (default: 8125)\n  \
           --threads <u32>   Sender threads (default: CPU cores)\n  \
           --duration <u32>  Duration in seconds to run benchmark (default: 180)\n  \
           --metrics <u32>   Number of unique metrics to simulate (default: 1000)\n  \
           --local-agg       Simulate client-side local aggregation before sending\n  \
           --help, -h        Print this help menu\n"
    );
}

fn register_metrics(
    db_url: &str,
    unique_metrics: u32,
) -> Result<HashMap<String, i32>, String> {
    use postgres::{Client, NoTls};
    let mut client = Client::connect(db_url, NoTls)
        .map_err(|e| format!("Failed to connect to database: {}", e))?;

    let mut name_to_id = HashMap::new();

    let mut transaction = client.transaction()
        .map_err(|e| format!("Failed to start transaction: {}", e))?;
    
    transaction.execute("LOCK TABLE stat_pg_metrics IN EXCLUSIVE MODE;", &[])
        .map_err(|e| format!("Failed to lock table: {}", e))?;

    let prepared = transaction.prepare(
        "INSERT INTO stat_pg_metrics (id, name)
         VALUES (
             (SELECT COALESCE(MIN(unused.id), 1)
              FROM (
                  SELECT 1 AS id
                  UNION ALL
                  SELECT id + 1 FROM stat_pg_metrics
              ) unused
              WHERE unused.id NOT IN (SELECT id FROM stat_pg_metrics)),
             $1
         )
         ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
         RETURNING id;"
    ).map_err(|e| format!("Failed to prepare query: {}", e))?;

    for idx in 0..unique_metrics {
        let name = format!("bench.metric.{}", idx);
        let row = transaction.query_one(&prepared, &[&name])
            .map_err(|e| format!("Failed to register {}: {}", name, e))?;
        let id: i32 = row.get(0);
        name_to_id.insert(name, id);
    }

    transaction.commit().map_err(|e| format!("Failed to commit: {}", e))?;
    Ok(name_to_id)
}

fn format_number(val: u64) -> String {
    let s = val.to_string();
    let mut result = String::new();
    let mut chars = s.chars().rev().enumerate();
    while let Some((idx, c)) = chars.next() {
        if idx > 0 && idx % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn run_psql_query(host: &str, query: &str, select_only: bool) -> std::io::Result<std::process::Output> {
    if host == "127.0.0.1" || host == "localhost" {
        let args = if select_only {
            vec![
                "exec",
                "stat-pg-db",
                "psql",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-t",
                "-c",
                query,
            ]
        } else {
            vec![
                "exec",
                "stat-pg-db",
                "psql",
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-c",
                query,
            ]
        };
        std::process::Command::new("docker")
            .args(&args)
            .output()
    } else {
        let args = if select_only {
            vec![
                "-h",
                host,
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-t",
                "-c",
                query,
            ]
        } else {
            vec![
                "-h",
                host,
                "-U",
                "postgres",
                "-d",
                "postgres",
                "-c",
                query,
            ]
        };
        let mut cmd = std::process::Command::new("psql");
        cmd.env("PGPASSWORD", "postgres");
        cmd.args(&args).output()
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return;
    }

    // Default configuration
    let mut host = "127.0.0.1".to_string();
    let mut port = 8125u16;
    let mut threads = thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let mut duration_secs = 180u32;
    let mut unique_metrics = 1000u32;
    let mut local_agg = false;

    // Parse simple command-line arguments
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" if i + 1 < args.len() => {
                host = args[i + 1].clone();
                i += 2;
            }
            "--port" if i + 1 < args.len() => {
                port = args[i + 1].parse().expect("Invalid port number");
                i += 2;
            }
            "--threads" if i + 1 < args.len() => {
                threads = args[i + 1].parse().expect("Invalid threads number");
                i += 2;
            }
            "--duration" if i + 1 < args.len() => {
                duration_secs = args[i + 1].parse().expect("Invalid duration");
                i += 2;
            }
            "--metrics" if i + 1 < args.len() => {
                unique_metrics = args[i + 1].parse().expect("Invalid metrics count");
                i += 2;
            }
            "--local-agg" => {
                local_agg = true;
                i += 1;
            }
            arg => {
                println!("Unknown argument: {}. Use --help for usage details.", arg);
                std::process::exit(1);
            }
        }
    }

    let target_addr = format!("{}:{}", host, port);
    println!("==================================================");
    println!("stat-pg-bench Starting UDP Stress Test");
    println!("Target Address : {}", target_addr);
    println!("Sender Threads : {}", threads);
    println!("Run Duration   : {} seconds ({:.1} minutes)", duration_secs, duration_secs as f64 / 60.0);
    println!("Unique Metrics : {}", unique_metrics);
    println!("Protocol Format: Raw UDP (16 formats, ~366 updates/packet)");
    println!("==================================================");

    // Atomic states shared across sender threads
    let running = Arc::new(AtomicBool::new(true));
    let packets_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let metrics_generated = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let metrics_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let bytes_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Spawn background SQL monitoring thread
    let host_clone = host.clone();
    let query_thread = thread::spawn(move || {
        let mut run = 1;
        let mut previous_processed = 0u64;
        let mut stable_seconds = 0;

        loop {
            thread::sleep(Duration::from_secs(15));
            
            let start = Instant::now();
            let output = run_psql_query(&host_clone, "SELECT COALESCE(SUM(count), 0) FROM stat_pg_processed;", true);
            
            let duration = start.elapsed();
            
            let mut current_processed = 0u64;
            match output {
                Ok(out) => {
                    if out.status.success() {
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        current_processed = stdout.trim().parse().unwrap_or(0);
                    }
                }
                Err(_) => {}
            }

            println!(
                "SQL Monitor Run {:<4} | Processed: {:<15} (Query took {:.2?})",
                run,
                format_number(current_processed),
                duration
            );

            if current_processed == previous_processed && current_processed > 0 {
                stable_seconds += 15;
                if stable_seconds >= 15 {
                    println!("\nDatabase inserts have stabilized. Ingestion queue is fully drained!");
                    break;
                }
            } else {
                stable_seconds = 0;
            }

            previous_processed = current_processed;
            run += 1;
        }
    });

    let db_port = if host == "127.0.0.1" || host == "localhost" { 5433 } else { 5432 };
    let db_url = format!("postgres://postgres:postgres@{}:{}/postgres", host, db_port);

    let mut initial_udp_received = 0i64;
    let mut initial_udp_dropped = 0i64;

    // Automatically truncate and vacuum database tables before registering metrics
    println!("Database cleanup: Truncating event tables and vacuuming...");
    if let Ok(mut client) = postgres::Client::connect(&db_url, postgres::NoTls) {
        let _ = client.execute("TRUNCATE TABLE stat_pg_metrics, evt_secs, evt_mins, evt_hours, evt_days, evt_30s, stat_pg_staging, stat_pg_processed, stat_pg_rollups RESTART IDENTITY CASCADE;", &[]);
        let _ = client.execute("VACUUM ANALYZE;", &[]);
    }

    // Register all simulated metric names in the database and retrieve their IDs
    let name_to_id = match register_metrics(&db_url, unique_metrics) {
        Ok(m) => m,
        Err(e) => {
            println!("Failed to register metrics in database: {}", e);
            std::process::exit(1);
        }
    };
    let name_to_id = Arc::new(name_to_id);

    println!("Warmup: Waiting 2.0 seconds for background worker to sync MAX(id) bounds...");
    std::thread::sleep(Duration::from_secs(2));

    // Verify registration count in the database
    if let Ok(mut client) = postgres::Client::connect(&db_url, postgres::NoTls) {
        if let Ok(row) = client.query_one("SELECT COUNT(*) FROM stat_pg_metrics;", &[]) {
            let count: i64 = row.get(0);
            println!("Warmup check: Total registered metrics in database: {}", count);
        }
        if let Ok(row) = client.query_one("SELECT received, dropped FROM stat_pg_udp_stats();", &[]) {
            initial_udp_received = row.get(0);
            initial_udp_dropped = row.get(1);
        }
    }

    // Pre-cache all registered metric IDs to avoid formatting and hashmap lookups in the hot loop
    let mut metric_ids = Vec::with_capacity(unique_metrics as usize);
    for idx in 0..unique_metrics {
        let name = format!("bench.metric.{}", idx);
        let id = *name_to_id.get(&name).expect("Registered metric not found");
        metric_ids.push(id);
    }
    let metric_ids = Arc::new(metric_ids);

    let start_time = Instant::now();
    let mut worker_handles = Vec::with_capacity(threads as usize);

    // Spawn sender threads
    for thread_id in 0..threads {
        let running_clone = running.clone();
        let packets_clone = packets_sent.clone();
        let metrics_generated_clone = metrics_generated.clone();
        let metrics_clone = metrics_sent.clone();
        let bytes_clone = bytes_sent.clone();
        let target = target_addr.clone();
        let metric_ids_clone = metric_ids.clone();
        let local_agg = local_agg;

        let handle = thread::spawn(move || {
            let socket = match UdpSocket::bind("0.0.0.0:0") {
                Ok(s) => s,
                Err(e) => {
                    println!("Thread {} failed to bind UDP socket: {}", thread_id, e);
                    return;
                }
            };

            #[cfg(unix)]
            {
                use std::os::unix::io::AsRawFd;
                let fd = socket.as_raw_fd();
                let val: libc::c_int = 50 * 1024 * 1024;
                let _ = unsafe {
                    libc::setsockopt(
                        fd,
                        libc::SOL_SOCKET,
                        libc::SO_SNDBUF,
                        &val as *const libc::c_int as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    )
                };
            }

            if let Err(e) = socket.connect(&target) {
                println!("Thread {} failed to connect UDP socket to {}: {}", thread_id, target, e);
                return;
            }

            let mut local_packets = 0u64;
            let mut local_metrics = 0u64;
            let mut local_bytes = 0u64;

            let mut rng = SimpleRng::new(12345 + thread_id as u64);

            if local_agg {
                // Local Aggregation Mode:
                // Accumulate updates in memory and flush periodically (every 5,000 generated events).
                let batch_size = 5000;
                let mut local_values = vec![0.0f64; unique_metrics as usize];
                let mut local_formats = vec![0u8; unique_metrics as usize];
                let mut touched = Vec::with_capacity(unique_metrics as usize);
                let mut touched_set = vec![false; unique_metrics as usize];

                let mut local_generated = 0u64;

                while running_clone.load(Ordering::Relaxed) {
                    for _ in 0..batch_size {
                        let metric_idx = (rng.next_u64() % unique_metrics as u64) as usize;
                        let rand_val = rng.next_f64();
                        
                        let val: f64;
                        let format: u8;
                        if rand_val < 0.60 {
                            if rng.next_u64() % 2 == 0 {
                                val = 1.0;
                                format = 0;
                            } else {
                                val = -1.0;
                                format = 1;
                            }
                        } else if rand_val < 0.75 {
                            let val_u8 = (rng.next_u64() % 256) as u8;
                            val = val_u8 as f64;
                            format = 3;
                        } else if rand_val < 0.90 {
                            let format_choice = rng.next_u64() % 4;
                            format = match format_choice {
                                0 => 4,
                                1 => 5,
                                2 => 6,
                                _ => 7,
                            };
                            if format == 6 || format == 7 {
                                val = 15.5;
                            } else {
                                val = (rng.next_u64() % 65536) as f64;
                            }
                        } else if rand_val < 0.98 {
                            let format_choice = rng.next_u64() % 4;
                            format = match format_choice {
                                0 => 8,
                                1 => 9,
                                2 => 10,
                                _ => 11,
                            };
                            if format == 10 || format == 11 {
                                val = 123.456;
                            } else {
                                val = (rng.next_u64() % 1000000) as f64;
                            }
                        } else {
                            let format_choice = rng.next_u64() % 4;
                            format = match format_choice {
                                0 => 12,
                                1 => 13,
                                2 => 14,
                                _ => 15,
                            };
                            if format == 14 || format == 15 {
                                val = 12345.6789;
                            } else {
                                val = (rng.next_u64() % 10000000) as f64;
                            }
                        }

                        local_values[metric_idx] += val;
                        local_formats[metric_idx] = format;

                        if !touched_set[metric_idx] {
                            touched_set[metric_idx] = true;
                            touched.push(metric_idx);
                        }
                    }
                    local_generated += batch_size as u64;

                    // Serialize aggregated values
                    let mut packet = Vec::with_capacity(1440);
                    let mut packet_metrics = 0u32;

                    for &metric_idx in &touched {
                        let val = local_values[metric_idx];
                        if val == 0.0 {
                            continue;
                        }

                        let id = metric_ids_clone[metric_idx];
                        let id_i16 = id as i16;

                        let final_format: u8;
                        let mut payload_bytes = Vec::new();

                        if val == 1.0 {
                            final_format = 0;
                        } else if val == -1.0 {
                            final_format = 1;
                        } else if val.fract() == 0.0 {
                            if val >= -128.0 && val <= 127.0 {
                                final_format = 2;
                                payload_bytes.push((val as i8) as u8);
                            } else if val >= 0.0 && val <= 255.0 {
                                final_format = 3;
                                payload_bytes.push(val as u8);
                            } else if val >= -32768.0 && val <= 32767.0 {
                                final_format = 4;
                                payload_bytes.extend_from_slice(&(val as i16).to_le_bytes());
                            } else if val >= 0.0 && val <= 65535.0 {
                                final_format = 5;
                                payload_bytes.extend_from_slice(&(val as u16).to_le_bytes());
                            } else if val >= i32::MIN as f64 && val <= i32::MAX as f64 {
                                final_format = 8;
                                payload_bytes.extend_from_slice(&(val as i32).to_le_bytes());
                            } else if val >= u32::MIN as f64 && val <= u32::MAX as f64 {
                                final_format = 9;
                                payload_bytes.extend_from_slice(&(val as u32).to_le_bytes());
                            } else if val >= i64::MIN as f64 && val <= i64::MAX as f64 {
                                final_format = 12;
                                payload_bytes.extend_from_slice(&(val as i64).to_le_bytes());
                            } else if val >= u64::MIN as f64 && val <= u64::MAX as f64 {
                                final_format = 13;
                                payload_bytes.extend_from_slice(&(val as u64).to_le_bytes());
                            } else {
                                final_format = 14;
                                payload_bytes.extend_from_slice(&val.to_le_bytes());
                            }
                        } else {
                            let val_f32 = val as f32;
                            if val_f32 as f64 == val {
                                final_format = 10;
                                payload_bytes.extend_from_slice(&val_f32.to_le_bytes());
                            } else {
                                final_format = 14;
                                payload_bytes.extend_from_slice(&val.to_le_bytes());
                            }
                        }

                        let width = 3 + payload_bytes.len();
                        if packet.len() + width > 1440 {
                            let _ = socket.send(&packet);
                            local_packets += 1;
                            local_metrics += packet_metrics as u64;
                            local_bytes += packet.len() as u64;

                            packet.clear();
                            packet_metrics = 0;
                        }

                        packet.push(final_format);
                        packet.extend_from_slice(&id_i16.to_le_bytes());
                        packet.extend(&payload_bytes);
                        packet_metrics += 1;
                    }

                    if !packet.is_empty() {
                        let _ = socket.send(&packet);
                        local_packets += 1;
                        local_metrics += packet_metrics as u64;
                        local_bytes += packet.len() as u64;
                    }

                    // Reset local aggregators
                    for &metric_idx in &touched {
                        local_values[metric_idx] = 0.0;
                        touched_set[metric_idx] = false;
                    }
                    touched.clear();

                    if local_packets % 32 == 0 {
                        packets_clone.fetch_add(local_packets, Ordering::Relaxed);
                        metrics_clone.fetch_add(local_metrics, Ordering::Relaxed);
                        metrics_generated_clone.fetch_add(local_generated, Ordering::Relaxed);
                        bytes_clone.fetch_add(local_bytes, Ordering::Relaxed);
                        local_packets = 0;
                        local_metrics = 0;
                        local_generated = 0;
                        local_bytes = 0;
                    }
                }
                packets_clone.fetch_add(local_packets, Ordering::Relaxed);
                metrics_clone.fetch_add(local_metrics, Ordering::Relaxed);
                metrics_generated_clone.fetch_add(local_generated, Ordering::Relaxed);
                bytes_clone.fetch_add(local_bytes, Ordering::Relaxed);
            } else {
                // Prebuilt Blaster Mode: (max throughput raw stress test)
                let num_prebuilt = 1000;
                let mut prebuilt_packets = Vec::with_capacity(num_prebuilt);
                let mut batch_idx = thread_id as usize;

                for _ in 0..num_prebuilt {
                    let mut packet = Vec::with_capacity(1440);
                    let mut updates = 0u32;
                    while packet.len() + 11 <= 1440 {
                        let metric_idx = batch_idx % unique_metrics as usize;
                        let id = metric_ids_clone[metric_idx];
                        let id_i16 = id as i16;

                        let rand_val = rng.next_f64();
                        if rand_val < 0.60 {
                            let format = if rng.next_u64() % 2 == 0 { 0u8 } else { 1u8 };
                            packet.push(format);
                            packet.extend_from_slice(&id_i16.to_le_bytes());
                        } else if rand_val < 0.75 {
                            let format = if rng.next_u64() % 2 == 0 { 2u8 } else { 3u8 };
                            packet.push(format);
                            packet.extend_from_slice(&id_i16.to_le_bytes());
                            let val_u8 = (rng.next_u64() % 256) as u8;
                            packet.push(val_u8);
                        } else if rand_val < 0.90 {
                            let format_choice = rng.next_u64() % 4;
                            let format = match format_choice {
                                0 => 4u8,
                                1 => 5u8,
                                2 => 6u8,
                                _ => 7u8,
                            };
                            packet.push(format);
                            packet.extend_from_slice(&id_i16.to_le_bytes());
                            if format == 6 || format == 7 {
                                packet.extend_from_slice(&0x4BC0u16.to_le_bytes());
                            } else {
                                let val_u16 = (rng.next_u64() % 65536) as u16;
                                packet.extend_from_slice(&val_u16.to_le_bytes());
                            }
                        } else if rand_val < 0.98 {
                            let format_choice = rng.next_u64() % 4;
                            let format = match format_choice {
                                0 => 8u8,
                                1 => 9u8,
                                2 => 10u8,
                                _ => 11u8,
                            };
                            packet.push(format);
                            packet.extend_from_slice(&id_i16.to_le_bytes());
                            if format == 10 || format == 11 {
                                let val_f32 = (rng.next_f64() * 1000.0) as f32;
                                packet.extend_from_slice(&val_f32.to_le_bytes());
                            } else {
                                let val_u32 = rng.next_u64() as u32;
                                packet.extend_from_slice(&val_u32.to_le_bytes());
                            }
                        } else {
                            let format_choice = rng.next_u64() % 4;
                            let format = match format_choice {
                                0 => 12u8,
                                1 => 13u8,
                                2 => 14u8,
                                _ => 15u8,
                            };
                            packet.push(format);
                            packet.extend_from_slice(&id_i16.to_le_bytes());
                            if format == 14 || format == 15 {
                                let val_f64 = rng.next_f64() * 1000.0;
                                packet.extend_from_slice(&val_f64.to_le_bytes());
                            } else {
                                let val_u64 = rng.next_u64();
                                packet.extend_from_slice(&val_u64.to_le_bytes());
                            }
                        }

                        updates += 1;
                        batch_idx = batch_idx.wrapping_add(threads as usize);
                    }
                    prebuilt_packets.push((packet, updates));
                }

                #[cfg(target_os = "linux")]
                let run_linux = true;
                #[cfg(not(target_os = "linux"))]
                let run_linux = false;

                if run_linux {
                    #[cfg(target_os = "linux")]
                    {
                        use std::os::unix::io::AsRawFd;
                        let fd = socket.as_raw_fd();
                        let batch_size = 32;

                        let mut iovecs = vec![libc::iovec { iov_base: std::ptr::null_mut(), iov_len: 0 }; batch_size];
                        let mut mmsgs = vec![libc::mmsghdr {
                            msg_hdr: libc::msghdr {
                                msg_name: std::ptr::null_mut(),
                                msg_namelen: 0,
                                msg_iov: std::ptr::null_mut(),
                                msg_iovlen: 0,
                                msg_control: std::ptr::null_mut(),
                                msg_controllen: 0,
                                msg_flags: 0,
                            },
                            msg_len: 0,
                        }; batch_size];

                        let mut packet_idx = 0;

                        while running_clone.load(Ordering::Relaxed) {
                            for i in 0..batch_size {
                                let p_idx = (packet_idx + i) % num_prebuilt;
                                let (packet_bytes, _) = &prebuilt_packets[p_idx];
                                iovecs[i].iov_base = packet_bytes.as_ptr() as *mut libc::c_void;
                                iovecs[i].iov_len = packet_bytes.len();

                                mmsgs[i].msg_hdr.msg_iov = &mut iovecs[i] as *mut libc::iovec;
                                mmsgs[i].msg_hdr.msg_iovlen = 1;
                            }

                            let res = unsafe {
                                libc::sendmmsg(
                                    fd,
                                    mmsgs.as_mut_ptr(),
                                    batch_size as libc::c_uint,
                                    0,
                                )
                            };

                            if res >= 0 {
                                let sent_count = res as usize;
                                for i in 0..sent_count {
                                    let p_idx = (packet_idx + i) % num_prebuilt;
                                    let (packet_bytes, updates) = &prebuilt_packets[p_idx];
                                    local_metrics += *updates as u64;
                                    local_bytes += packet_bytes.len() as u64;
                                }
                                packet_idx = (packet_idx + sent_count) % num_prebuilt;
                                local_packets += sent_count as u64;
                            } else {
                                let err = std::io::Error::last_os_error();
                                if err.kind() != std::io::ErrorKind::WouldBlock {
                                    break;
                                }
                            }

                            if local_packets % 320 == 0 {
                                packets_clone.fetch_add(local_packets, Ordering::Relaxed);
                                metrics_clone.fetch_add(local_metrics, Ordering::Relaxed);
                                metrics_generated_clone.fetch_add(local_metrics, Ordering::Relaxed);
                                bytes_clone.fetch_add(local_bytes, Ordering::Relaxed);
                                local_packets = 0;
                                local_metrics = 0;
                                local_bytes = 0;
                            }
                        }
                    }
                } else {
                    let mut packet_idx = 0;
                    while running_clone.load(Ordering::Relaxed) {
                        let (packet_bytes, updates) = &prebuilt_packets[packet_idx];

                        if socket.send(packet_bytes).is_ok() {
                            local_packets += 1;
                            local_metrics += *updates as u64;
                            local_bytes += packet_bytes.len() as u64;
                        } else {
                            break;
                        }

                        packet_idx = (packet_idx + 1) % num_prebuilt;

                        if local_packets % 120 == 0 {
                            packets_clone.fetch_add(local_packets, Ordering::Relaxed);
                            metrics_clone.fetch_add(local_metrics, Ordering::Relaxed);
                            metrics_generated_clone.fetch_add(local_metrics, Ordering::Relaxed);
                            bytes_clone.fetch_add(local_bytes, Ordering::Relaxed);
                            local_packets = 0;
                            local_metrics = 0;
                            local_bytes = 0;
                        }
                    }
                }
            }

            if !local_agg {
                packets_clone.fetch_add(local_packets, Ordering::Relaxed);
                metrics_clone.fetch_add(local_metrics, Ordering::Relaxed);
                metrics_generated_clone.fetch_add(local_metrics, Ordering::Relaxed);
                bytes_clone.fetch_add(local_bytes, Ordering::Relaxed);
            }
        });

        worker_handles.push(handle);
    }

    println!("Blasting UDP metric updates to target server...");

    // Wait for the duration of the benchmark (180 seconds)
    thread::sleep(Duration::from_secs(duration_secs as u64));

    // Signal all workers to stop
    running.store(false, Ordering::Relaxed);

    // Join all workers
    for handle in worker_handles {
        let _ = handle.join();
    }

    let elapsed = start_time.elapsed();
    let total_packets = packets_sent.load(Ordering::SeqCst);
    let total_generated = metrics_generated.load(Ordering::SeqCst);
    let total_sent = metrics_sent.load(Ordering::SeqCst);
    let total_bytes = bytes_sent.load(Ordering::SeqCst);

    let pps = total_packets as f64 / elapsed.as_secs_f64();
    let mps = total_sent as u64 / elapsed.as_secs();
    let mb_sec = (total_bytes as f64 / 1_048_576.0) / elapsed.as_secs_f64();

    fn format_float(val: f64) -> String {
        let int_part = val.trunc() as u64;
        let frac_part = (val.fract() * 100.0).round().abs() as u64;
        format!("{}.{:02}", format_number(int_part), frac_part)
    }

    println!("==================================================");
    println!("Benchmark Complete!");
    println!("Elapsed Time                   : {:.2?}", elapsed);
    println!("Total Packets                  : {}", format_number(total_packets));
    println!("Total Metrics Generated (Raw)  : {}", format_number(total_generated));
    println!("Total Client-Aggregated Metrics: {}", format_number(total_sent));
    println!("Total Bytes                    : {:.2} MB", total_bytes as f64 / 1_048_576.0);
    println!("Average PPS                    : {} Batches/sec (~366 updates/batch)", format_float(pps));
    println!("Average MPS                    : {} Metrics/sec (Sent)", format_number(mps));
    println!("Throughput                     : {:.2} MB/sec", mb_sec);
    println!("==================================================");

    // Wait for the SQL background monitoring thread to complete its remaining runs
    println!("Waiting for final background SQL query runs to finish and stabilize...");
    let _ = query_thread.join();

    println!("Forcing final cascading database rollups to complete...");
    let _ = run_psql_query(&host, "SELECT stat_pg_force_rollups();", false);

    // Perform comprehensive final database consistency validation
    println!("==================================================");
    println!("Performing Final Database Consistency Validation...");
    println!("==================================================");

    let tables = ["evt_secs", "evt_mins", "evt_hours", "evt_days", "evt_30s"];
    let mut table_sums = HashMap::new();

    for table in &tables {
        let output = run_psql_query(&host, &format!("SELECT COALESCE(SUM(value::float8), 0.0) FROM {};", table), true);

        if let Ok(out) = output {
            if out.status.success() {
                let stdout = String::from_utf8_lossy(&out.stdout);
                let sum = stdout.trim().parse::<f64>().unwrap_or(0.0);
                table_sums.insert(table.to_string(), sum);
            }
        }
    }

    let mut total_processed: u64 = 0;
    let output_processed = run_psql_query(&host, "SELECT COALESCE(SUM(count), 0) FROM stat_pg_processed;", true);
    if let Ok(out) = output_processed {
        if out.status.success() {
            let stdout = String::from_utf8_lossy(&out.stdout);
            total_processed = stdout.trim().parse::<u64>().unwrap_or(0);
        }
    }

    let mut final_udp_received = 0i64;
    let mut final_udp_dropped = 0i64;
    let mut avg_merge_latency = 0.0f64;
    if let Ok(mut client) = postgres::Client::connect(&db_url, postgres::NoTls) {
        if let Ok(row) = client.query_one("SELECT received, dropped FROM stat_pg_udp_stats();", &[]) {
            final_udp_received = row.get(0);
            final_udp_dropped = row.get(1);
        }
        if let Ok(row) = client.query_one("SELECT COALESCE(SUM(total_latency_ms) / NULLIF(SUM(merge_count), 0), 0.0)::float8 FROM stat_pg_processed WHERE worker_id = 0;", &[]) {
            avg_merge_latency = row.get(0);
        }
    }

    let diff_udp_received = final_udp_received - initial_udp_received;
    let diff_udp_dropped = final_udp_dropped - initial_udp_dropped;

    let target_sum = table_sums.get("evt_secs").cloned().unwrap_or(0.0);
    let delivery_rate = if total_sent > 0 {
        (total_processed as f64 / total_sent as f64) * 100.0
    } else {
        0.0
    };
    println!("Total Metrics Generated (Raw)  : {}", format_number(total_generated));
    println!("Total Client-Aggregated Metrics: {}", format_number(total_sent));
    println!("Total Server-Received Metrics  : {} ({:.2}% delivery rate)", 
        format_number(total_processed), 
        delivery_rate
    );
    println!("Average Coordinator Merge Time : {:.2} ms", avg_merge_latency);
    println!("Kernel Network Ingestion Stats (Container):");
    println!("  - Total UDP Packets Received   : {}", format_number(diff_udp_received as u64));
    println!("  - Total UDP Packets Dropped    : {}", format_number(diff_udp_dropped as u64));
    let udp_success_rate = if diff_udp_received + diff_udp_dropped > 0 {
        (diff_udp_received as f64 / (diff_udp_received + diff_udp_dropped) as f64) * 100.0
    } else {
        0.0
    };
    println!("  - Kernel Ingestion Success Rate: {:.2}%", udp_success_rate);
    println!("Persisted SUM(evt_secs)        : {}", format_number(target_sum as u64));

    let mut mismatch = false;
    for table in &tables {
        let sum = table_sums.get(*table).cloned().unwrap_or(0.0);
        let diff = (sum - target_sum).abs();
        let status = if diff < 0.01 {
            "MATCH"
        } else {
            mismatch = true;
            "MISMATCH"
        };
        println!("  - {:<10} SUM(value): {:<15} | {}", table, format_number(sum as u64), status);
    }

    if mismatch {
        println!("\nWARNING: Aggregation sum mismatch detected across rollup periods!");
    } else {
        println!("\nSUCCESS: All rollup tables are perfectly consistent!");
    }
    
    println!("\nTotal Disk Space Used by Tables (including indexes):");
    let space_query = "
        SELECT 
            table_name, 
            pg_size_pretty(pg_total_relation_size(table_name::regclass)) as total_size
        FROM (
            VALUES 
                ('stat_pg_metrics'),
                ('evt_secs'),
                ('evt_mins'),
                ('evt_hours'),
                ('evt_days'),
                ('evt_30s'),
                ('stat_pg_processed'),
                ('stat_pg_rollups')
        ) as t(table_name);
    ";
    let output = run_psql_query(&host, space_query, false);
    if let Ok(out) = output {
        if out.status.success() {
            let stdout = String::from_utf8_lossy(&out.stdout);
            println!("{}", stdout);
        }
    }
    
    println!("==================================================");
}
