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

fn register_metric(db_url: &str, name: &str, metric_type: &str) -> Result<i32, String> {
    let mut client = Client::connect(db_url, NoTls)
        .map_err(|e| format!("Failed to connect to database: {}", e))?;

    let mut transaction = client.transaction()
        .map_err(|e| format!("Failed to start transaction: {}", e))?;

    transaction.execute("LOCK TABLE stat_pg_metrics IN EXCLUSIVE MODE;", &[])
        .map_err(|e| format!("Failed to lock table: {}", e))?;

    let row = transaction.query_one(
        "INSERT INTO stat_pg_metrics (id, name, type)
         VALUES (
             (SELECT COALESCE(MIN(unused.id), 1)
              FROM (
                  SELECT 1 AS id
                  UNION ALL
                  SELECT id + 1 FROM stat_pg_metrics
              ) unused
              WHERE unused.id NOT IN (SELECT id FROM stat_pg_metrics)),
             $1,
             $2
         )
         ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name, type = EXCLUDED.type
         RETURNING id;",
        &[&name, &metric_type],
    ).map_err(|e| format!("Failed to register metric: {}", e))?;

    let id: i32 = row.get(0);
    transaction.commit().map_err(|e| format!("Commit failed: {}", e))?;
    Ok(id)
}

fn parse_statsd_line(line: &str) -> Option<(String, f64, String)> {
    let line = line.trim();
    if line.is_empty() {
        return None;
    }

    // Parse format: name:value|type
    let parts: Vec<&str> = line.split(':').collect();
    if parts.len() != 2 {
        return None;
    }

    let name = parts[0].trim().to_string();
    let rest = parts[1];
    let val_type: Vec<&str> = rest.split('|').collect();
    if val_type.is_empty() {
        return None;
    }

    let val: f64 = match val_type[0].trim().parse() {
        Ok(v) => v,
        Err(_) => return None,
    };

    let metric_type = if val_type.len() >= 2 {
        val_type[1].trim()
    } else {
        "c" // default to counter
    };

    let is_currency = metric_type == "curr" || metric_type == "currency";

    // Timers/ms get converted to counter with `_ms` suffix
    let final_name = if metric_type == "ms" {
        format!("{}_ms", name)
    } else {
        name
    };

    let type_str = if is_currency { "currency" } else { "numeric" };

    Some((final_name, val, type_str.to_string()))
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
    let metric_types = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    // Spawn UDP listener thread to ingest StatsD text format
    let listen_socket = match UdpSocket::bind(&listen_addr) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to bind listen socket: {}", e);
            std::process::exit(1);
        }
    };

    let aggregates_clone = aggregates.clone();
    let metric_types_clone = metric_types.clone();
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
                    let mut types = metric_types_clone.lock().unwrap();

                    // StatsD packets can contain multiple metrics separated by newlines
                    for line in data_str.lines() {
                        if let Some((name, val, mtype)) = parse_statsd_line(line) {
                            let entry = aggs.entry(name.clone()).or_insert(0.0);
                            *entry += val;
                            types.entry(name).or_insert(mtype);
                        }
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

            let mtype = {
                let types_guard = metric_types.lock().unwrap();
                types_guard.get(&name).cloned().unwrap_or_else(|| "numeric".to_string())
            };

            if cached_id.is_none() {
                // Perform database lookup/registration
                match register_metric(&db_url, &name, &mtype) {
                    Ok(id) => {
                        let mut cache_guard = cache.lock().unwrap();
                        cache_guard.insert(name.clone(), id);
                        cached_id = Some(id);
                        println!("Registered new metric name '{}' (type: {}) -> ID {}", name, mtype, id);
                    }
                    Err(e) => {
                        eprintln!("Error registering metric '{}': {}", name, e);
                        continue;
                    }
                }
            }

            if let Some(id) = cached_id {
                let id_i16 = id as i16;
                let mut final_val = val;
                if mtype == "currency" {
                    final_val = val * 10000.0;
                }

                // Dynamically select the most compact format representation for final_val:
                if final_val == 1.0 {
                    payload.push(0); // FORMAT_INC
                    payload.extend_from_slice(&id_i16.to_le_bytes());
                } else if final_val == -1.0 {
                    payload.push(1); // FORMAT_DEC
                    payload.extend_from_slice(&id_i16.to_le_bytes());
                } else if final_val.fract() == 0.0 {
                    if final_val >= -128.0 && final_val <= 127.0 {
                        payload.push(2); // FORMAT_INT8
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.push((final_val as i8) as u8);
                    } else if final_val >= 0.0 && final_val <= 255.0 {
                        payload.push(3); // FORMAT_UINT8
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.push(final_val as u8);
                    } else if final_val >= -32768.0 && final_val <= 32767.0 {
                        payload.push(4); // FORMAT_INT16
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.extend_from_slice(&(final_val as i16).to_le_bytes());
                    } else if final_val >= 0.0 && final_val <= 65535.0 {
                        payload.push(5); // FORMAT_UINT16
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.extend_from_slice(&(final_val as u16).to_le_bytes());
                    } else if final_val >= i32::MIN as f64 && final_val <= i32::MAX as f64 {
                        payload.push(8); // FORMAT_INT32
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.extend_from_slice(&(final_val as i32).to_le_bytes());
                    } else if final_val >= u32::MIN as f64 && final_val <= u32::MAX as f64 {
                        payload.push(9); // FORMAT_UINT32
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.extend_from_slice(&(final_val as u32).to_le_bytes());
                    } else if final_val >= i64::MIN as f64 && final_val <= i64::MAX as f64 {
                        payload.push(12); // FORMAT_INT64
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.extend_from_slice(&(final_val as i64).to_le_bytes());
                    } else if final_val >= u64::MIN as f64 && final_val <= u64::MAX as f64 {
                        payload.push(13); // FORMAT_UINT64
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.extend_from_slice(&(final_val as u64).to_le_bytes());
                    } else {
                        payload.push(14); // FORMAT_FLOAT64
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.extend_from_slice(&final_val.to_le_bytes());
                    }
                } else {
                    let val_f32 = final_val as f32;
                    if val_f32 as f64 == final_val {
                        payload.push(10); // FORMAT_FLOAT32
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.extend_from_slice(&val_f32.to_le_bytes());
                    } else {
                        payload.push(14); // FORMAT_FLOAT64
                        payload.extend_from_slice(&id_i16.to_le_bytes());
                        payload.extend_from_slice(&final_val.to_le_bytes());
                    }
                }

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_statsd_line() {
        assert_eq!(
            parse_statsd_line("foo:10.5|c"),
            Some(("foo".to_string(), 10.5, "numeric".to_string()))
        );
        assert_eq!(
            parse_statsd_line("bar:250|ms"),
            Some(("bar_ms".to_string(), 250.0, "numeric".to_string()))
        );
        assert_eq!(
            parse_statsd_line("baz:42"),
            Some(("baz".to_string(), 42.0, "numeric".to_string()))
        );
        assert_eq!(
            parse_statsd_line("payment:12.50|curr"),
            Some(("payment".to_string(), 12.50, "currency".to_string()))
        );
        assert_eq!(parse_statsd_line("invalid_line"), None);
    }
}
