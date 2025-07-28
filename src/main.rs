#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::time::Duration;

use may::{
    go,
    sync::mpmc::{self, Receiver, Sender},
};
use may_minihttp::HttpServiceFactory;

mod memory_store;
mod models;
mod server;
mod tcp;
mod worker;

fn start_workers(
    num_workers: usize,
    default_url: &'static str,
    fallback_url: &'static str,
    rx: Receiver<(String, i64)>,
) {
    for i in 0..num_workers {
        let rx = rx.clone();
        let _ = go!(
            may::coroutine::Builder::new()
                .name(format!("worker-{i}"))
                .stack_size(0x1000),
            move || {
                println!("Worker {i} started");
                let mut worker = worker::Worker::new(default_url, fallback_url, rx);
                worker.process_batch();
            }
        );
    }
}

struct HttpServer {
    tx: Sender<(String, i64)>,
    peer_host: &'static str,
}

impl HttpServiceFactory for HttpServer {
    type Service = server::Service;

    fn new_service(&self, _: usize) -> Self::Service {
        server::Service {
            tx: self.tx.clone(),
            peer_host: self.peer_host,
        }
    }
}

fn main() {
    may::config().set_pool_capacity(10000).set_stack_size(0x1000).set_worker_pin(true);

    let args: Vec<String> = std::env::args().collect();
    let mode_server = args.contains(&"--server".to_string());
    let mode_workers = args.contains(&"--workers".to_string());

    if !mode_server && !mode_workers {
        println!("Specify at least 1 mode (--server or --workers)");
        std::process::exit(1);
    }

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let default_processor_url = std::env::var("DEFAULT_PROCESSOR_URL").unwrap().leak();
    let fallback_processor_url = std::env::var("FALLBACK_PROCESSOR_URL").unwrap().leak();
    let peer_host = std::env::var("PEER_HOST").unwrap().leak();
    let num_cpus = num_cpus::get();

    println!("Configuration:");
    println!("  CPUs: {num_cpus}");
    println!("  Port: {port}");
    println!("  Default Processor: {default_processor_url}");
    println!("  Fallback Processor: {fallback_processor_url}");
    println!("  Peer Host: {peer_host}");

    // background worker processor
    let (tx, rx) = mpmc::channel::<(String, i64)>();

    if mode_workers {
        println!("Starting workers...");
        start_workers(
            5,
            default_processor_url,
            fallback_processor_url,
            rx.clone(),
        );
    }

    if mode_server {
        println!("Starting HTTP server on port {port}");
        let server = HttpServer {
            tx: tx.clone(),
            peer_host,
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
