#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::Duration;

use may::go;
use may_minihttp::HttpServiceFactory;

use crate::worker::ProcessingSuccess;

mod models;
mod redis_pool;
mod server;
mod worker;

fn start_workers(
    num_workers: usize,
    redis_url: &'static str,
    default_url: &'static str,
    fallback_url: &'static str,
) {
    for i in 0..num_workers {
        let _ = go!(
            may::coroutine::Builder::new()
                .name(format!("worker-{i}"))
                .stack_size(0x2000),
            move || {
                println!("Worker {i} started");

                let pool = redis_pool::RedisPool::new(redis_url, 6);
                let mut worker = worker::Worker::new(pool, default_url, fallback_url);

                loop {
                    match worker.process_batch() {
                        // Ok(0) => {
                        //     // println!("Worker {i}: Processed 0 payments");
                        //     may::coroutine::sleep(Duration::from_millis(50));
                        // }
                        // Ok(n) => {
                        //     // println!("Worker {i}: Processed {n} payments");
                        //     if n > 100 {
                        //         println!("Worker {i}: Processed {n} payments");
                        //     }
                        // }
                        Ok(ProcessingSuccess::PaymentsProcessed) => {
                            // println!("Worker {i}: Processed {n} payments");
                            // may::coroutine::sleep(Duration::from_millis(10));
                        }
                        Ok(ProcessingSuccess::NoPayments) => {
                            // println!("Worker {i}: Processed 0 payments");
                            may::coroutine::sleep(Duration::from_millis(50));
                        }
                        Err(e) => {
                            eprintln!("Worker {i} error: {e:?}");
                            may::coroutine::sleep(Duration::from_millis(100));
                        }
                    }
                }
            }
        );
    }
}

struct HttpServer {
    redis_pool: redis_pool::RedisPool,
}

impl HttpServiceFactory for HttpServer {
    type Service = server::Service<'static>;

    fn new_service(&self, id: usize) -> Self::Service {
        let conn = self.redis_pool.get_connection(id);
        server::Service { conn }
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

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let redis_url = std::env::var("REDIS_URL").unwrap().leak();
    let default_processor_url = std::env::var("DEFAULT_PROCESSOR_URL").unwrap().leak();
    let fallback_processor_url = std::env::var("FALLBACK_PROCESSOR_URL").unwrap().leak();
    let num_cpus = num_cpus::get();

    println!("Configuration:");
    println!("  CPUs: {num_cpus}");
    println!("  Port: {port}");
    println!("  Redis: {redis_url}");
    println!("  Default Processor: {default_processor_url}");
    println!("  Fallback Processor: {fallback_processor_url}");

    redis_pool::init_scripts();

    if mode_workers {
        println!("Starting workers...");
        start_workers(
            num_cpus,
            redis_url,
            default_processor_url,
            fallback_processor_url,
        );
    }

    if mode_server {
        println!("Starting HTTP server on port {port}");
        let server = HttpServer {
            redis_pool: redis_pool::RedisPool::new(redis_url, num_cpus),
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
