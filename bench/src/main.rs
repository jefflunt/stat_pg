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

const FORMAT_WIDTHS: [usize; 16] = [2, 2, 3, 3, 4, 4, 4, 4, 6, 6, 6, 6, 10, 10, 10, 10];

struct PendingPacket {
    buf: Vec<u8>,
    num_segments: u8,
}

impl PendingPacket {
    fn new() -> Self {
        Self {
            buf: vec![0], // placeholder for num_segments
            num_segments: 0,
        }
    }

    fn clear(&mut self) {
        self.buf.clear();
        self.buf.push(0); // placeholder
        self.num_segments = 0;
    }
}

fn flush_format_buffers<F>(
    format_buffers: &mut [Vec<u8>; 16],
    current_packet: &mut PendingPacket,
    mut send_fn: F,
) where
    F: FnMut(&[u8]),
{
    for f in 0..16 {
        let data = &format_buffers[f];
        if data.is_empty() {
            continue;
        }

        let width = FORMAT_WIDTHS[f];
        let total_updates = data.len() / width;
        let mut remaining_updates = total_updates;
        let mut slice_offset = 0;

        while remaining_updates > 0 {
            let remaining_space = 1440 - current_packet.buf.len();
            if remaining_space < 3 + width {
                if current_packet.num_segments > 0 {
                    current_packet.buf[0] = current_packet.num_segments;
                    send_fn(&current_packet.buf);
                }
                current_packet.clear();
            }

            let max_possible = (1440 - current_packet.buf.len() - 3) / width;
            let updates_to_write = std::cmp::min(remaining_updates, max_possible);

            if updates_to_write > 0 {
                current_packet.buf.push(f as u8);
                current_packet.buf.extend_from_slice(&(updates_to_write as u16).to_le_bytes());
                
                let start_idx = slice_offset;
                let end_idx = slice_offset + updates_to_write * width;
                current_packet.buf.extend_from_slice(&data[start_idx..end_idx]);

                current_packet.num_segments += 1;
                slice_offset = end_idx;
                remaining_updates -= updates_to_write;
            } else {
                if current_packet.num_segments > 0 {
                    current_packet.buf[0] = current_packet.num_segments;
                    send_fn(&current_packet.buf);
                }
                current_packet.clear();
            }
        }
        format_buffers[f].clear();
    }
}

fn print_help() {
    println!(
        "stat_pg_bench - High-speed TCP stress test benchmark tool\n\n\
         Usage: stat_pg_bench [OPTIONS]\n\n\
         Options:\n  \
           --host <str>      Target host IP (default: 127.0.0.1)\n  \
           --port <u16>      Target port (default: 8125)\n  \
           --db-port <u16>   Target PostgreSQL DB port (default: 5432 or 5433)\n  \
           --db-name <str>   Target PostgreSQL DB name (default: postgres)\n  \
           --threads <u32>   Sender threads (default: CPU cores)\n  \
           --readers <u32>   Concurrent SQL reader threads (default: 0)\n  \
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

    let count_i32 = unique_metrics as i32;
    client.execute(
        "INSERT INTO stat_pg_metrics (name)
         SELECT 'bench.metric.' || g
         FROM generate_series(0, $1 - 1) g
         ON CONFLICT (name) DO NOTHING;",
        &[&count_i32]
    ).map_err(|e| format!("Failed bulk metric registration: {}", e))?;

    let rows = client.query("SELECT id, name FROM stat_pg_metrics;", &[])
        .map_err(|e| format!("Failed querying registered metrics: {}", e))?;

    for row in rows {
        let id: i32 = row.get(0);
        let name: String = row.get(1);
        name_to_id.insert(name, id);
    }

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
                "stat_pg_db",
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
                "stat_pg_db",
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
    let mut db_port_opt: Option<u16> = None;
    let mut db_name = "postgres".to_string();
    let mut threads = thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
    let mut readers = 0u32;
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
            "--db-port" if i + 1 < args.len() => {
                db_port_opt = Some(args[i + 1].parse().expect("Invalid db-port number"));
                i += 2;
            }
            "--db-name" if i + 1 < args.len() => {
                db_name = args[i + 1].clone();
                i += 2;
            }
            "--threads" if i + 1 < args.len() => {
                threads = args[i + 1].parse().expect("Invalid threads number");
                i += 2;
            }
            "--readers" if i + 1 < args.len() => {
                readers = args[i + 1].parse().expect("Invalid readers count");
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
    println!("stat_pg_bench Starting UDP Stress Test");
    println!("Target Address : {}", target_addr);
    println!("Sender Threads : {}", threads);
    if readers > 0 {
        println!("Reader Threads : {}", readers);
    }
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

    let db_port = db_port_opt.unwrap_or_else(|| {
        if host == "127.0.0.1" || host == "localhost" { 5433 } else { 5432 }
    });
    let db_url = format!("postgres://postgres:postgres@{}:{}/{}", host, db_port, db_name);

    // Spawn background SQL monitoring thread
    let host_clone = host.clone();
    let db_url_clone = db_url.clone();
    let running_clone = Arc::clone(&running);
    let query_thread = thread::spawn(move || {
        let mut flush_interval = 1.0f64;
        if let Ok(mut client) = postgres::Client::connect(&db_url_clone, postgres::NoTls) {
            if let Ok(row) = client.query_one("SHOW stat_pg.flush_interval;", &[]) {
                let val: String = row.get(0);
                let cleaned = val.trim().trim_end_matches('s');
                flush_interval = cleaned.parse::<f64>().unwrap_or(1.0);
            }
        }
        println!("Background monitor detected database flush interval: {:.1}s", flush_interval);

        let mut run = 1;
        let mut previous_processed = 0u64;
        let mut zero_processed_runs_after_finish = 0u32;
        let mut benchmark_finished_time: Option<Instant> = None;

        loop {
            thread::sleep(Duration::from_secs(15));
            
            let start = Instant::now();
            let output = run_psql_query(&host_clone, "SELECT COALESCE(SUM(value), 0) FROM stat_pg_secs;", true);
            
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
            run += 1;

            let is_running = running_clone.load(Ordering::Relaxed);
            if !is_running && benchmark_finished_time.is_none() {
                benchmark_finished_time = Some(Instant::now());
            }

            if !is_running {
                if current_processed == 0 {
                    zero_processed_runs_after_finish += 1;
                    if zero_processed_runs_after_finish >= 3 {
                        println!("\nNo metric updates detected in database after 3 monitor checks. Exiting monitor loop.");
                        break;
                    }
                } else {
                    zero_processed_runs_after_finish = 0;
                }

                let elapsed_since_finish = benchmark_finished_time.unwrap().elapsed().as_secs_f64();
                if elapsed_since_finish >= (flush_interval + 10.0) {
                    if current_processed == previous_processed && current_processed > 0 {
                        println!("\nDatabase inserts have stabilized. Ingestion queue is fully drained!");
                        break;
                    }
                }
            }
            previous_processed = current_processed;
        }
    });

    let mut initial_udp_received = 0i64;
    let mut initial_udp_dropped = 0i64;

    // Automatically truncate and vacuum database tables before registering metrics
    println!("Database cleanup: Truncating event tables and vacuuming...");
    if let Ok(mut client) = postgres::Client::connect(&db_url, postgres::NoTls) {
        let _ = client.execute("TRUNCATE TABLE stat_pg_metrics, stat_pg_secs, stat_pg_mins, stat_pg_hours, stat_pg_days, stat_pg_30s, stat_pg_staging RESTART IDENTITY CASCADE;", &[]);
        let _ = client.execute("VACUUM ANALYZE stat_pg_metrics, stat_pg_secs, stat_pg_mins, stat_pg_hours, stat_pg_days, stat_pg_30s, stat_pg_staging;", &[]);
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

    let read_queries_count = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let read_queries_latency_us = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let mut reader_handles = Vec::with_capacity(readers as usize);

    for reader_id in 0..readers {
        let running_clone = running.clone();
        let read_queries_clone = read_queries_count.clone();
        let read_latency_clone = read_queries_latency_us.clone();
        let db_url_clone = db_url.clone();
        let unique_metrics = unique_metrics;

        let handle = thread::spawn(move || {
            if let Ok(mut client) = postgres::Client::connect(&db_url_clone, postgres::NoTls) {
                let mut rng = SimpleRng::new(reader_id as u64 + 9999);

                while running_clone.load(Ordering::Relaxed) {
                    let metric_id = (rng.next_u64() % unique_metrics as u64) as i32 + 1;
                    let query_start = Instant::now();
                    let _ = client.query("SELECT metric_id, SUM(value) FROM stat_pg_secs WHERE metric_id = $1 GROUP BY metric_id;", &[&metric_id]);
                    let latency_us = query_start.elapsed().as_micros() as u64;

                    read_queries_clone.fetch_add(1, Ordering::Relaxed);
                    read_latency_clone.fetch_add(latency_us, Ordering::Relaxed);
                }
            }
        });
        reader_handles.push(handle);
    }

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

            let mut rng = SimpleRng::new(12345 + thread_id as u64);
            let mut local_packets = 0u64;
            let mut local_metrics = 0u64;
            let mut local_bytes = 0u64;

            if local_agg {
                // Local Aggregation Mode:
                let mut local_values = vec![0.0f64; unique_metrics as usize];
                let mut touched_set = vec![false; unique_metrics as usize];
                let mut touched = Vec::with_capacity(unique_metrics as usize);

                let batch_size = 5000;
                let mut local_generated = 0u64;

                while running_clone.load(Ordering::Relaxed) {
                    for _ in 0..batch_size {
                        let metric_idx = (rng.next_u64() % unique_metrics as u64) as usize;
                        let rand_val = rng.next_f64();
                        let val: f64;
                        if rand_val < 0.60 {
                            val = if rng.next_u64() % 2 == 0 { 1.0 } else { -1.0 };
                        } else if rand_val < 0.75 {
                            val = (rng.next_u64() % 256) as f64;
                        } else if rand_val < 0.90 {
                            val = (rng.next_u64() % 65536) as f64;
                        } else if rand_val < 0.98 {
                            val = (rng.next_u64() % 4294967296) as f64;
                        } else {
                            val = (rng.next_u64() % 10000000) as f64;
                        }

                        local_values[metric_idx] += val;
                        if !touched_set[metric_idx] {
                            touched_set[metric_idx] = true;
                            touched.push(metric_idx);
                        }
                    }
                    local_generated += batch_size as u64;
                    let mut batch_sum_val = 0u64;
                    for &metric_idx in &touched {
                        let val = local_values[metric_idx];
                        if val > 0.0 {
                            batch_sum_val += val as u64;
                        }
                    }
                    local_metrics += batch_sum_val;

                    // Serialize aggregated values
                    let mut format_buffers = vec![Vec::new(); 16];

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

                        let f_buf = &mut format_buffers[final_format as usize];
                        f_buf.extend_from_slice(&id_i16.to_le_bytes());
                        f_buf.extend_from_slice(&payload_bytes);
                    }

                    let mut format_buffers_array: [Vec<u8>; 16] = format_buffers.try_into().unwrap();
                    let mut current_packet = PendingPacket::new();
                    let mut tx_queue: Vec<Vec<u8>> = Vec::with_capacity(32);

                    flush_format_buffers(
                        &mut format_buffers_array,
                        &mut current_packet,
                        |packet_data| {
                            tx_queue.push(packet_data.to_vec());
                        }
                    );

                    if current_packet.num_segments > 0 {
                        current_packet.buf[0] = current_packet.num_segments;
                        tx_queue.push(current_packet.buf.clone());
                    }

                    for pkt in tx_queue {
                        let _ = socket.send(&pkt);
                        local_packets += 1;
                        local_bytes += pkt.len() as u64;
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

                let mut format_buffers = vec![Vec::new(); 16];
                let mut current_packet = PendingPacket::new();

                while prebuilt_packets.len() < num_prebuilt {
                    // Populate format buffers with random updates
                    for _ in 0..5000 {
                        let metric_idx = batch_idx % unique_metrics as usize;
                        let id = metric_ids_clone[metric_idx];
                        let id_i16 = id as i16;

                        let rand_val = rng.next_f64();
                        let format: u8;
                        let mut val_bytes = Vec::new();

                        if rand_val < 0.60 {
                            format = if rng.next_u64() % 2 == 0 { 0u8 } else { 1u8 };
                        } else if rand_val < 0.75 {
                            format = if rng.next_u64() % 2 == 0 { 2u8 } else { 3u8 };
                            let val_u8 = (rng.next_u64() % 256) as u8;
                            val_bytes.push(val_u8);
                        } else if rand_val < 0.90 {
                            let format_choice = rng.next_u64() % 4;
                            format = match format_choice {
                                0 => 4u8,
                                1 => 5u8,
                                2 => 6u8,
                                _ => 7u8,
                            };
                            if format == 6 || format == 7 {
                                val_bytes.extend_from_slice(&0x4BC0u16.to_le_bytes());
                            } else {
                                let val_u16 = (rng.next_u64() % 65536) as u16;
                                val_bytes.extend_from_slice(&val_u16.to_le_bytes());
                            }
                        } else if rand_val < 0.98 {
                            let format_choice = rng.next_u64() % 4;
                            format = match format_choice {
                                0 => 8u8,
                                1 => 9u8,
                                2 => 10u8,
                                _ => 11u8,
                            };
                            if format == 10 || format == 11 {
                                let val_f32 = (rng.next_f64() * 1000.0) as f32;
                                val_bytes.extend_from_slice(&val_f32.to_le_bytes());
                            } else {
                                let val_u32 = rng.next_u64() as u32;
                                val_bytes.extend_from_slice(&val_u32.to_le_bytes());
                            }
                        } else {
                            let format_choice = rng.next_u64() % 4;
                            format = match format_choice {
                                0 => 12u8,
                                1 => 13u8,
                                2 => 14u8,
                                _ => 15u8,
                            };
                            if format == 14 || format == 15 {
                                let val_f64 = rng.next_f64() * 1000.0;
                                val_bytes.extend_from_slice(&val_f64.to_le_bytes());
                            } else {
                                let val_u64 = rng.next_u64();
                                val_bytes.extend_from_slice(&val_u64.to_le_bytes());
                            }
                        }

                        let f_buf = &mut format_buffers[format as usize];
                        f_buf.extend_from_slice(&id_i16.to_le_bytes());
                        f_buf.extend_from_slice(&val_bytes);

                        batch_idx = batch_idx.wrapping_add(threads as usize);
                    }

                    // Flush format buffers into prebuilt_packets
                    let mut format_buffers_array: [Vec<u8>; 16] = format_buffers.clone().try_into().unwrap();
                    flush_format_buffers(
                        &mut format_buffers_array,
                        &mut current_packet,
                        |packet_data| {
                            if prebuilt_packets.len() < num_prebuilt {
                                let mut updates_in_packet = 0u32;
                                let num_segs = packet_data[0] as usize;
                                let mut off = 1;
                                for _ in 0..num_segs {
                                    if off + 3 > packet_data.len() { break; }
                                    let f = packet_data[off];
                                    let count = unsafe { std::ptr::read_unaligned(packet_data.as_ptr().add(off + 1) as *const u16) } as usize;
                                    updates_in_packet += count as u32;
                                    off += 3 + count * FORMAT_WIDTHS[f as usize];
                                }
                                prebuilt_packets.push((packet_data.to_vec(), updates_in_packet));
                            }
                        }
                    );
                    for f in 0..16 {
                        format_buffers[f] = format_buffers_array[f].clone();
                    }
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
                        let mut local_packets = 0u64;
                        let mut local_metrics = 0u64;
                        let mut local_bytes = 0u64;

                        while running_clone.load(Ordering::Relaxed) {
                            for i in 0..batch_size {
                                let p_idx = (packet_idx + i) % num_prebuilt;
                                let (packet_bytes, _) = &prebuilt_packets[p_idx];
                                iovecs[i].iov_base = packet_bytes.as_ptr() as *mut libc::c_void;
                                iovecs[i].iov_len = packet_bytes.len();
                                mmsgs[i].msg_hdr.msg_iov = &mut iovecs[i];
                                mmsgs[i].msg_hdr.msg_iovlen = 1;
                            }

                            let res = unsafe {
                                libc::sendmmsg(
                                    fd,
                                    mmsgs.as_mut_ptr(),
                                    batch_size as u32,
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
                    let mut local_packets = 0u64;
                    let mut local_metrics = 0u64;
                    let mut local_bytes = 0u64;
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
    for handle in reader_handles {
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
    if readers > 0 {
        let total_reads = read_queries_count.load(Ordering::SeqCst);
        let total_read_us = read_queries_latency_us.load(Ordering::SeqCst);
        let avg_read_latency_ms = if total_reads > 0 { (total_read_us as f64 / total_reads as f64) / 1000.0 } else { 0.0 };
        let qps = if elapsed.as_secs_f64() > 0.0 { total_reads as f64 / elapsed.as_secs_f64() } else { 0.0 };

        println!("--------------------------------------------------");
        println!("Concurrent SQL Reader Performance:");
        println!("  - Reader Threads               : {}", readers);
        println!("  - Total Read Queries Executed  : {}", format_number(total_reads));
        println!("  - Average Read Query Latency   : {:.2} ms", avg_read_latency_ms);
        println!("  - Read Query Throughput        : {} Queries/sec", format_float(qps));
    }
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

    let tables = ["stat_pg_secs", "stat_pg_mins", "stat_pg_hours", "stat_pg_days", "stat_pg_30s"];
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
    let output_processed = run_psql_query(&host, "SELECT COALESCE(SUM(value), 0)::bigint FROM stat_pg_secs;", true);
    if let Ok(out) = output_processed {
        if out.status.success() {
            let stdout = String::from_utf8_lossy(&out.stdout);
            total_processed = stdout.trim().parse::<u64>().unwrap_or(0);
        }
    }

    let mut final_udp_received = 0i64;
    let mut final_udp_dropped = 0i64;
    let mut final_ebpf_items = 0i64;
    let avg_merge_latency = 0.0f64;
    if let Ok(mut client) = postgres::Client::connect(&db_url, postgres::NoTls) {
        if let Ok(row) = client.query_one("SELECT received, dropped, ebpf_items FROM stat_pg_udp_stats();", &[]) {
            final_udp_received = row.get(0);
            final_udp_dropped = row.get(1);
            final_ebpf_items = row.get(2);
        }
    }

    let diff_udp_received = final_udp_received - initial_udp_received;
    let diff_udp_dropped = final_udp_dropped - initial_udp_dropped;

    let target_sum = table_sums.get("stat_pg_secs").cloned().unwrap_or(0.0);
    let items_received = if final_ebpf_items > 0 { final_ebpf_items as u64 } else { total_sent };
    let values_generated = if total_sent > 0 { target_sum as u64 } else { total_generated };

    println!("==================================================");
    println!("Normalized Metric Engine Verification:");
    println!("==================================================");
    println!("1. Items Generated : {:>30}", format_number(total_generated));
    println!("2. Items on Wire   : {:>30}", format_number(items_received));
    println!("3. Values Generated: {:>30}", format_number(values_generated));
    println!("4. Values Persisted: {:>30}", format_number(total_processed));
    
    let item_rate = if total_sent > 0 { (items_received as f64 / total_sent as f64) * 100.0 } else { 0.0 };
    let val_rate = if values_generated > 0 { (total_processed as f64 / values_generated as f64) * 100.0 } else { 0.0 };
    println!("--------------------------------------------------");
    println!("Item Delivery Rate               : {:.2}%", item_rate);
    println!("Value Persistence Rate           : {:.2}%", val_rate);
    println!("Average Coordinator Merge Time   : {:.2} ms", avg_merge_latency);
    println!("Kernel Network Ingestion Stats (Container):");
    println!("  - Total UDP Packets Received   : {}", format_number(diff_udp_received as u64));
    println!("  - Total UDP Packets Dropped    : {}", format_number(diff_udp_dropped as u64));
    let udp_success_rate = if diff_udp_received + diff_udp_dropped > 0 {
        (diff_udp_received as f64 / (diff_udp_received + diff_udp_dropped) as f64) * 100.0
    } else {
        0.0
    };
    println!("  - Kernel Ingestion Success Rate: {:.2}%", udp_success_rate);
    println!("Persisted SUM(stat_pg_secs)   : {}", format_number(target_sum as u64));

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
                ('stat_pg_secs'),
                ('stat_pg_mins'),
                ('stat_pg_hours'),
                ('stat_pg_days'),
                ('stat_pg_30s')
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
