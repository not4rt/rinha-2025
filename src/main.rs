// HTTP Server based on Xudong-Huang techpower benchmark submission

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::Duration;

use may::go;
use may_minihttp::HttpServiceFactory;

use crate::{models::Processor, worker::ProcessingError};

mod db_init;
mod db_pool;
// mod health_check;
mod models;
mod server;
mod worker;

fn start_workers(
    pool: &db_pool::WorkerDbPool,
    num_workers: usize,
    default_url: &'static str,
    fallback_url: &'static str,
) {
    for i in 0..num_workers {
        let worker_db = pool.get_connection(i);
        let mut worker = worker::Worker::new(worker_db, default_url, fallback_url);
        let _ = go!(
            may::coroutine::Builder::new()
                .name(format!("worker-{i}"))
                .stack_size(0x9999),
            move || {
                println!("Worker {i} started");
                loop {
                    // let processors = worker.wait_healthy_check();
                    let processors = (Processor::Default, Some(Processor::Fallback));
                    match worker.process_batch(processors) {
                        Ok(0) => {
                            // No pending payments
                            may::coroutine::sleep(Duration::from_millis(100));
                        }
                        Ok(_) => {
                            //
                        }
                        Err(ProcessingError::DatabaseError(error)) => {
                            println!("Worker {i} database error: {error}");
                        }
                        Err(_) => {
                            // Both Processors Unavailable
                            eprintln!("Both processors unavailable");
                            may::coroutine::sleep(Duration::from_millis(500));
                        }
                    }
                }
            }
        );
    }
}

// fn start_health_checker(
//     pool: &db_pool::HealthDbPool,
//     default_url: &'static str,
//     fallback_url: &'static str,
// ) {
//     let health_checker =
//         health_check::HealthChecker::new(pool.get_connection(0), default_url, fallback_url);
//     let _ = go!(
//         may::coroutine::Builder::new()
//             .name("health-checker-parent".to_owned())
//             .stack_size(0x4000),
//         move || {
//             loop {
//                 health_checker.check_health();
//                 may::coroutine::sleep(Duration::from_secs(5));
//             }
//         }
//     );
// }

struct HttpServer {
    db_pool: db_pool::ServerDbPool,
}

impl HttpServiceFactory for HttpServer {
    type Service = server::Service;

    fn new_service(&self, id: usize) -> Self::Service {
        let db = self.db_pool.get_connection(id);
        server::Service { db }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mode_server = args.contains(&"--server".to_string());
    let mode_workers = args.contains(&"--workers".to_string());

    if !mode_server && !mode_workers {
        println!("Specify at least 1 mode (--server or --workers)");
        std::process::exit(1);
    }

    may::config().set_pool_capacity(1000).set_stack_size(0x1000);
    let port = std::env::var("PORT").unwrap_or_else(|_| "9999".to_string());
    let db_url = std::env::var("DATABASE_URL").unwrap().leak();
    let default_processor_url = std::env::var("DEFAULT_PROCESSOR_URL").unwrap().leak();
    let fallback_processor_url = std::env::var("FALLBACK_PROCESSOR_URL").unwrap().leak();
    let num_cpus = num_cpus::get();

    println!("Configuration:");
    println!("  CPUs: {}", num_cpus);
    println!("  Port: {}", port);
    println!("  Database: {}", db_url);
    println!("  Default Processor: {}", default_processor_url);
    println!("  Fallback Processor: {}", fallback_processor_url);

    if db_init::DatabaseInitializer::initialize(db_url) {
        println!("Database initialized successfully");
    }

    if mode_workers {
        // println!("Starting health checker...");
        // let health_pool = db_pool::HealthDbPool::new(db_url, 1);
        // start_health_checker(&health_pool, default_processor_url, fallback_processor_url);

        println!("Starting workers...");
        let workers_number = num_cpus;
        let worker_pool = db_pool::WorkerDbPool::new(db_url, workers_number);
        start_workers(
            &worker_pool,
            workers_number,
            default_processor_url,
            fallback_processor_url,
        );
    }

    if mode_server {
        println!("Starting HTTP server on port {port}");
        let server = HttpServer {
            db_pool: db_pool::ServerDbPool::new(db_url, num_cpus),
        };
        server
            .start(format!("0.0.0.0:{port}"))
            .unwrap()
            .join()
            .unwrap();
    } else if mode_workers {
        println!("Started only worker mode...");
        loop {
            may::coroutine::sleep(Duration::from_secs(3600));
        }
    }
}
