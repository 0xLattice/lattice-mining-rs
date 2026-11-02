use clap::Parser;
use lattice_mining::*;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;
#[derive(Parser)]
#[command(about = "Coordinate Lattice nonce workers against an operator-selected node")]
struct Args {
    #[arg(long)]
    node: String,
    #[arg(long)]
    rpc_cookie_file: Option<PathBuf>,
    #[arg(long)]
    workers: Option<usize>,
    #[arg(long)]
    batch_size: Option<u64>,
    #[arg(long)]
    worker_executable: Option<PathBuf>,
    #[arg(long, allow_hyphen_values = true)]
    worker_arg: Vec<String>,
    #[arg(long)]
    recipient: Vec<String>,
    #[arg(long)]
    min_work: Vec<String>,
    #[arg(long)]
    once: bool,
    #[arg(long)]
    no_stale_probe: bool,
    #[arg(long, default_value_t = 15000)]
    request_timeout_ms: u64,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let a = Args::parse();
    let count = a
        .workers
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get().saturating_sub(1))
                .unwrap_or(1)
        })
        .max(1);
    if count > 1024 {
        return Err("at most 1024 workers supported".into());
    }
    let batch = a.batch_size.unwrap_or(if a.worker_executable.is_some() {
        2_000_000_000
    } else {
        10000
    });
    let request = template_request(&a.recipient, &a.min_work)?;
    let node = Arc::new(HttpNodeClient::new(
        &a.node,
        request,
        a.rpc_cookie_file,
        Duration::from_millis(a.request_timeout_ms),
        16 * 1024 * 1024,
    )?);
    let workers: Vec<Arc<dyn Worker>> = (0..count)
        .map(|i| {
            if let Some(path) = &a.worker_executable {
                let mut w = ProcessWorker::new(format!("worker-{i}"), path.clone());
                w.arguments = a.worker_arg.clone();
                Arc::new(w) as Arc<dyn Worker>
            } else {
                Arc::new(CpuWorker::new(format!("local-{i}"))) as Arc<dyn Worker>
            }
        })
        .collect();
    let mut coordinator = Coordinator::new(
        node,
        workers,
        CoordinatorOptions {
            total_batch_size: if a.once {
                batch
            } else {
                batch.wrapping_mul(count as u64)
            },
            stale_probe: !a.no_stale_probe,
            ..Default::default()
        },
    );
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        signal.cancel();
    });
    if a.once {
        let result = coordinator.run_batch(&cancel).await;
        println!("{}", serde_json::to_string(&result)?);
        if matches!(
            result,
            Cycle::NodeFailed { .. } | Cycle::WorkerFailed { .. }
        ) {
            return Err("coordinator failed; see result".into());
        }
    } else {
        let (tx, mut rx) = tokio::sync::mpsc::channel(16);
        let task = tokio::spawn(coordinator.run(cancel, tx));
        let mut failed = false;
        while let Some(result) = rx.recv().await {
            failed |= matches!(
                result,
                Cycle::NodeFailed { .. } | Cycle::WorkerFailed { .. }
            );
            println!("{}", serde_json::to_string(&result)?);
        }
        let _ = task.await?;
        if failed {
            return Err("coordinator failed; see result".into());
        }
    }
    Ok(())
}
