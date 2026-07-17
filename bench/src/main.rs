use std::env;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;

fn print_help() {
    println!(
        "stat-pg-bench - High-speed UDP stress test benchmark tool\n\n\
        Usage: stat-pg-bench [OPTIONS]\n\n\
        Options:\n  \
          --host <str>      Target host IP (default: 127.0.0.1)\n  \
          --port <u16>      Target port (default: 8125)\n  \
          --threads <u32>   Sender threads (default: CPU cores)\n  \
          --duration <u32>  Duration in seconds to run benchmark (default: 10)\n  \
          --metrics <u32>   Number of unique metrics to simulate (default: 1000)\n  \
          --help, -h        Print this help menu\n"
    );
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
    let mut duration_secs = 10u32;
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
    println!("stat-pg-bench Starting Stress Test");
    println!("Target Address : {}", target_addr);
    println!("Sender Threads : {}", threads);
    println!("Run Duration   : {} seconds", duration_secs);
    println!("Unique Metrics : {}", unique_metrics);
    println!("Batching Mode  : MTU-bounded (<1,472 bytes)");
    println!("==================================================");

    // Pre-allocate metric batch payloads in memory to fit strictly under 1,472 bytes
    println!("Pre-allocating metric payloads...");
    let mut payloads: Vec<Vec<u8>> = Vec::new();
    let mut current_batch = String::new();

    for idx in 0..unique_metrics {
        let line = format!("bench.metric.{} 1\n", idx);
        // If adding this line would exceed the safe UDP MTU payload limit (1,472 bytes),
        // push the current batch and start a new one.
        if current_batch.len() + line.len() > 1472 {
            payloads.push(current_batch.into_bytes());
            current_batch = String::new();
        }
        current_batch.push_str(&line);
    }
    if !current_batch.is_empty() {
        payloads.push(current_batch.into_bytes());
    }
    let payloads = Arc::new(payloads);

    // Atomic states shared across sender threads
    let running = Arc::new(AtomicBool::new(true));
    let packets_sent = Arc::new(AtomicU64::new(0));
    let metrics_sent = Arc::new(AtomicU64::new(0));
    let bytes_sent = Arc::new(AtomicU64::new(0));

    let start_time = Instant::now();
    let mut worker_handles = Vec::with_capacity(threads);

    // Spawn sender threads
    for thread_id in 0..threads {
        let running_clone = running.clone();
        let packets_clone = packets_sent.clone();
        let metrics_clone = metrics_sent.clone();
        let bytes_clone = bytes_sent.clone();
        let payloads_clone = payloads.clone();
        let target = target_addr.clone();

        let handle = thread::spawn(move || {
            // Bind thread socket to an ephemeral port
            let socket = UdpSocket::bind("0.0.0.0:0")
                .expect("Failed to bind thread UDP socket");
            
            let mut local_packets = 0u64;
            let mut local_metrics = 0u64;
            let mut local_bytes = 0u64;
            let mut batch_idx = thread_id as usize;

            while running_clone.load(Ordering::Relaxed) {
                let payload = &payloads_clone[batch_idx % payloads_clone.len()];
                
                // Blast UDP packet
                if let Ok(bytes) = socket.send_to(payload, &target) {
                    local_packets += 1;
                    // Count number of newlines to determine actual metrics sent in this batch
                    let metrics_in_batch = payload.iter().filter(|&&b| b == b'\n').count() as u64;
                    local_metrics += metrics_in_batch;
                    local_bytes += bytes as u64;
                }

                batch_idx = batch_idx.wrapping_add(threads);

                // Batch flush atomic updates periodically to minimize atomic contention
                if local_packets % 1000 == 0 {
                    packets_clone.fetch_add(local_packets, Ordering::Relaxed);
                    metrics_clone.fetch_add(local_metrics, Ordering::Relaxed);
                    bytes_clone.fetch_add(local_bytes, Ordering::Relaxed);
                    local_packets = 0;
                    local_metrics = 0;
                    local_bytes = 0;
                }
            }

            // Flush remaining counts
            packets_clone.fetch_add(local_packets, Ordering::Relaxed);
            metrics_clone.fetch_add(local_metrics, Ordering::Relaxed);
            bytes_clone.fetch_add(local_bytes, Ordering::Relaxed);
        });

        worker_handles.push(handle);
    }

    println!("Blasting UDP packets to target server...");

    // Wait for the duration of the benchmark
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
    let mps = total_metrics as f64 / elapsed.as_secs_f64();
    let mb_sec = (total_bytes as f64 / 1_048_576.0) / elapsed.as_secs_f64();

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
    println!("Average PPS    : {} Packets/sec", format_float(pps));
    println!("Average MPS    : {} Metrics/sec (Ingested)", format_float(mps));
    println!("Throughput     : {:.2} MB/sec", mb_sec);
    println!("==================================================");
}
