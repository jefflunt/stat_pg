use postgres::{Client, NoTls};
use std::collections::HashMap;
use std::env;
use std::net::UdpSocket;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::thread;

fn print_help() {
    println!(
        "stat-pg-client - StatsD-compatible UDP proxy daemon\n\n\
        Usage: stat-pg-client [OPTIONS]\n\n\
        Options:\n  \
          --listen <addr>   Address to listen for StatsD metrics (default: 127.0.0.1:8126)\n  \
          --server <addr>   Target stat-pg UDP server address (default: 127.0.0.1:8125)\n  \
          --db <url>        PostgreSQL database connection URL (default: postgres://postgres:postgres@127.0.0.1:5433/postgres)\n  \
          --help, -h        Print this help menu\n"
    );
}

fn register_metric(db_url: &str, name: &str) -> Result<i32, String> {
    let mut client = Client::connect(db_url, NoTls)
        .map_err(|e| format!("Failed to connect to database: {}", e))?;

    let mut transaction = client.transaction()
        .map_err(|e| format!("Failed to start transaction: {}", e))?;

    transaction.execute("LOCK TABLE stat_pg_metrics IN EXCLUSIVE MODE;", &[])
        .map_err(|e| format!("Failed to lock table: {}", e))?;

    let row = transaction.query_one(
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
         RETURNING id;",
        &[&name],
    ).map_err(|e| format!("Failed to register metric: {}", e))?;

    let id: i32 = row.get(0);
    transaction.commit().map_err(|e| format!("Commit failed: {}", e))?;
    Ok(id)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print_help();
        return;
    }

    let mut listen_addr = "127.0.0.1:8126".to_string();
    let mut server_addr = "127.0.0.1:8125".to_string();
    let mut db_url = "postgres://postgres:postgres@127.0.0.1:5433/postgres".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--listen" if i + 1 < args.len() => {
                listen_addr = args[i + 1].clone();
                i += 2;
            }
            "--server" if i + 1 < args.len() => {
                server_addr = args[i + 1].clone();
                i += 2;
            }
            "--db" if i + 1 < args.len() => {
                db_url = args[i + 1].clone();
                i += 2;
            }
            arg => {
                println!("Unknown argument: {}. Use --help for usage details.", arg);
                std::process::exit(1);
            }
        }
    }

    println!("==================================================");
    println!("stat-pg-client StatsD UDP Proxy Starting");
    println!("Listening for StatsD on  : {}", listen_addr);
    println!("Forwarding UDP server to : {}", server_addr);
    println!("Database metadata URL    : {}", db_url);
    println!("==================================================");

    // In-memory aggregates and registration cache
    let aggregates = Arc::new(Mutex::new(HashMap::<String, f64>::new()));
    let cache = Arc::new(Mutex::new(HashMap::<String, i32>::new()));

    // Spawn UDP listener thread to ingest StatsD text format
    let listen_socket = match UdpSocket::bind(&listen_addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to bind listen socket: {}", e);
            std::process::exit(1);
        }
    };

    let aggregates_clone = aggregates.clone();
    thread::spawn(move || {
        let mut buf = vec![0u8; 65536];
        loop {
            match listen_socket.recv_from(&mut buf) {
                Ok((n, _src)) => {
                    let data_str = match std::str::from_utf8(&buf[..n]) {
                        Ok(s) => s,
                        Err(_) => continue,
                    };

                    let mut aggs = aggregates_clone.lock().unwrap();

                    // StatsD packets can contain multiple metrics separated by newlines
                    for line in data_str.lines() {
                        let line = line.trim();
                        if line.is_empty() {
                            continue;
                        }

                        // Parse format: name:value|type
                        let parts: Vec<&str> = line.split(':').collect();
                        if parts.len() != 2 {
                            continue;
                        }

                        let name = parts[0].trim().to_string();
                        let rest = parts[1];
                        let val_type: Vec<&str> = rest.split('|').collect();
                        if val_type.is_empty() {
                            continue;
                        }

                        let val: f64 = match val_type[0].trim().parse() {
                            Ok(v) => v,
                            Err(_) => continue,
                        };

                        let metric_type = if val_type.len() >= 2 {
                            val_type[1].trim()
                        } else {
                            "c" // default to counter
                        };

                        // Timers/ms get converted to counter with `_ms` suffix
                        let final_name = if metric_type == "ms" {
                            format!("{}_ms", name)
                        } else {
                            name
                        };

                        let entry = aggs.entry(final_name).or_insert(0.0);
                        *entry += val;
                    }
                }
                Err(e) => {
                    eprintln!("UDP receive error: {}", e);
                }
            }
        }
    });

    // Spawn sender thread that runs every 1 second
    let server_socket = match UdpSocket::bind("0.0.0.0:0") {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to bind outbound server socket: {}", e);
            std::process::exit(1);
        }
    };

    loop {
        thread::sleep(Duration::from_secs(1));

        let local_aggs = {
            let mut aggs = aggregates.lock().unwrap();
            let copy = aggs.clone();
            aggs.clear();
            copy
        };

        if local_aggs.is_empty() {
            continue;
        }

        let mut payload = Vec::new();

        for (name, val) in local_aggs {
            // Check cache
            let mut cached_id = {
                let cache_guard = cache.lock().unwrap();
                cache_guard.get(&name).cloned()
            };

            if cached_id.is_none() {
                // Perform database lookup/registration
                match register_metric(&db_url, &name) {
                    Ok(id) => {
                        let mut cache_guard = cache.lock().unwrap();
                        cache_guard.insert(name.clone(), id);
                        cached_id = Some(id);
                        println!("Registered new metric name '{}' -> ID {}", name, id);
                    }
                    Err(e) => {
                        eprintln!("Error registering metric '{}': {}", name, e);
                        continue;
                    }
                }
            }

            if let Some(id) = cached_id {
                // Packet format: [ID (4B LE i32)] [Value (8B LE f64)]
                payload.extend_from_slice(&id.to_le_bytes());
                payload.extend_from_slice(&val.to_le_bytes());

                // If payload is getting close to MTU (1,472 bytes for UDP), send it
                if payload.len() >= 1440 {
                    let _ = server_socket.send_to(&payload, &server_addr);
                    payload.clear();
                }
            }
        }

        if !payload.is_empty() {
            let _ = server_socket.send_to(&payload, &server_addr);
        }
    }
}
