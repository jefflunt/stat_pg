use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::thread;

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
          --help, -h        Print this help menu\n"
    );
}

fn register_metrics(
    stream: &mut TcpStream,
    unique_metrics: u32,
) -> std::io::Result<HashMap<String, i32>> {
    let mut name_to_id = HashMap::new();

    for idx in 0..unique_metrics {
        let name = format!("bench.metric.{}", idx);
        let name_bytes = name.as_bytes();
        let name_len = name_bytes.len() as u16;

        // Packet format: [0x01 (1B)] [Length (2B BE)] [Name]
        let mut req = Vec::with_capacity(3 + name_bytes.len());
        req.push(0x01);
        req.extend_from_slice(&name_len.to_be_bytes());
        req.extend_from_slice(name_bytes);

        stream.write_all(&req)?;

        // Read response: [0x01] [Length (2B)] [Name] [ID (4B LE)]
        let mut header = [0u8; 3];
        stream.read_exact(&mut header)?;
        assert_eq!(header[0], 0x01);
        let resp_name_len = u16::from_be_bytes([header[1], header[2]]) as usize;
        
        let mut body = vec![0u8; resp_name_len + 4];
        stream.read_exact(&mut body)?;
        
        let id_offset = resp_name_len;
        let id_bytes = &body[id_offset .. id_offset + 4];
        let assigned_id = i32::from_le_bytes(id_bytes.try_into().unwrap());
        
        name_to_id.insert(name, assigned_id);
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
            arg => {
                println!("Unknown argument: {}. Use --help for usage details.", arg);
                std::process::exit(1);
            }
        }
    }

    let target_addr = format!("{}:{}", host, port);
    println!("==================================================");
    println!("stat-pg-bench Starting Bidirectional TCP Stress Test");
    println!("Target Address : {}", target_addr);
    println!("Sender Threads : {}", threads);
    println!("Run Duration   : {} seconds ({:.1} minutes)", duration_secs, duration_secs as f64 / 60.0);
    println!("Unique Metrics : {}", unique_metrics);
    println!("Protocol Format: Bidirectional TCP (ID Handshake)");
    println!("==================================================");

    // Atomic states shared across sender threads
    let running = Arc::new(AtomicBool::new(true));
    let packets_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let metrics_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let bytes_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Spawn background SQL monitoring thread
    let host_clone = host.clone();
    let query_thread = thread::spawn(move || {
        let mut run = 1;
        let mut previous_sum = 0.0;
        let mut stable_seconds = 0;

        loop {
            thread::sleep(Duration::from_secs(15));
            
            let start = Instant::now();
            let output = run_psql_query(&host_clone, "SELECT COALESCE(SUM(value::float8), 0.0) FROM evt_secs;", true);
            
            let duration = start.elapsed();
            
            let mut current_sum = 0.0;
            match output {
                Ok(out) => {
                    if out.status.success() {
                        let stdout = String::from_utf8_lossy(&out.stdout);
                        current_sum = stdout.trim().parse().unwrap_or(0.0);
                    }
                }
                Err(_) => {}
            }

            println!(
                "SQL Monitor Run {:<4} | SUM(value): {:<15} (Query took {:.2?})",
                run,
                format_number(current_sum as u64),
                duration
            );

            if current_sum == previous_sum && current_sum > 0.0 {
                stable_seconds += 15;
                if stable_seconds >= 15 {
                    println!("\nDatabase inserts have stabilized. Ingestion queue is fully drained!");
                    break;
                }
            } else {
                stable_seconds = 0;
            }

            previous_sum = current_sum;
            run += 1;
        }
    });

    let start_time = Instant::now();
    let mut worker_handles = Vec::with_capacity(threads as usize);

    // Spawn sender threads
    for thread_id in 0..threads {
        let running_clone = running.clone();
        let packets_clone = packets_sent.clone();
        let metrics_clone = metrics_sent.clone();
        let bytes_clone = bytes_sent.clone();
        let target = target_addr.clone();

        let handle = thread::spawn(move || {
            let mut stream = match TcpStream::connect(&target) {
                Ok(s) => s,
                Err(e) => {
                    println!("Thread {} failed to connect to {}: {}", thread_id, target, e);
                    return;
                }
            };
            
            // Perform metric name registration handshake
            let name_to_id = match register_metrics(&mut stream, unique_metrics) {
                Ok(m) => m,
                Err(e) => {
                    println!("Thread {} registration handshake failed: {}", thread_id, e);
                    return;
                }
            };

            let mut local_packets = 0u64;
            let mut local_metrics = 0u64;
            let mut local_bytes = 0u64;
            
            // Write buffer for batching Type 0x02 packets (21 bytes each, 100 packets = 2100 bytes)
            let mut write_buf = Vec::with_capacity(2100);
            let mut batch_idx = thread_id as usize;

            while running_clone.load(Ordering::Relaxed) {
                write_buf.clear();
                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO)
                    .as_secs() as i64;

                let mut metrics_in_batch = 0;
                for _ in 0..100 {
                    let metric_idx = batch_idx % unique_metrics as usize;
                    let metric_name = format!("bench.metric.{}", metric_idx);
                    let &metric_id = name_to_id.get(&metric_name).unwrap();

                    write_buf.push(0x02);
                    write_buf.extend_from_slice(&metric_id.to_le_bytes());
                    write_buf.extend_from_slice(&1.0f64.to_le_bytes());
                    write_buf.extend_from_slice(&ts.to_le_bytes());

                    batch_idx = batch_idx.wrapping_add(threads as usize);
                    metrics_in_batch += 1;
                }

                if stream.write_all(&write_buf).is_ok() {
                    local_packets += 1;
                    local_metrics += metrics_in_batch;
                    local_bytes += write_buf.len() as u64;
                } else {
                    // TCP write error, socket closed or dropped
                    break;
                }

                if local_packets % 100 == 0 {
                    packets_clone.fetch_add(local_packets, Ordering::Relaxed);
                    metrics_clone.fetch_add(local_metrics, Ordering::Relaxed);
                    bytes_clone.fetch_add(local_bytes, Ordering::Relaxed);
                    local_packets = 0;
                    local_metrics = 0;
                    local_bytes = 0;
                }
            }

            packets_clone.fetch_add(local_packets, Ordering::Relaxed);
            metrics_clone.fetch_add(local_metrics, Ordering::Relaxed);
            bytes_clone.fetch_add(local_bytes, Ordering::Relaxed);
            std::thread::sleep(std::time::Duration::from_millis(100));
            let _ = stream.shutdown(std::net::Shutdown::Both);
        });

        worker_handles.push(handle);
    }

    println!("Blasting TCP metric updates to target server...");

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
    let total_metrics = metrics_sent.load(Ordering::SeqCst);
    let total_bytes = bytes_sent.load(Ordering::SeqCst);

    let pps = total_packets as f64 / elapsed.as_secs_f64();
    let mps = total_metrics as u64 / elapsed.as_secs();
    let mb_sec = (total_bytes as f64 / 1_048_576.0) / elapsed.as_secs_f64();

    fn format_float(val: f64) -> String {
        let int_part = val.trunc() as u64;
        let frac_part = (val.fract() * 100.0).round().abs() as u64;
        format!("{}.{:02}", format_number(int_part), frac_part)
    }

    println!("==================================================");
    println!("Benchmark Complete!");
    println!("Elapsed Time   : {:.2?}", elapsed);
    println!("Total Packets  : {}", format_number(total_packets));
    println!("Total Metrics  : {}", format_number(total_metrics));
    println!("Total Bytes    : {:.2} MB", total_bytes as f64 / 1_048_576.0);
    println!("Average PPS    : {} Batches/sec (100 updates/batch)", format_float(pps));
    println!("Average MPS    : {} Metrics/sec (Ingested)", format_number(mps));
    println!("Throughput     : {:.2} MB/sec", mb_sec);
    println!("==================================================");

    // Wait for the SQL background monitoring thread to complete its remaining runs
    println!("Waiting for final background SQL query runs to finish and stabilize...");
    let _ = query_thread.join();

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

    let target_sum = table_sums.get("evt_secs").cloned().unwrap_or(0.0);
    println!("Total Emitted Metrics   : {}", format_number(total_metrics));
    println!("Persisted (evt_secs)    : {} ({:.2}% delivery rate)", 
        format_number(target_sum as u64), 
        (target_sum / total_metrics as f64) * 100.0
    );

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
                ('evt_30s')
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
