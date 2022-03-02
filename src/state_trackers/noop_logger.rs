use crate::messages::*;
use tokio::sync::mpsc;

pub async fn start_noop_logger(mut msgs: mpsc::UnboundedReceiver<LoggerMessage>) {
    while let Some(msg) = msgs.recv().await {
        use LoggerMessage::*;

        match msg {
            CreateRun { response, .. } => {
                response.send(Ok(0)).unwrap_or(());
            }
            GetRuns { response, .. } => {
                let records = Vec::new();
                response.send(records).unwrap_or(());
            }
            GetRun { response, .. } => {
                response
                    .send(Err(anyhow!("Noop logger does not support queries")))
                    .unwrap_or(());
            }
            GetState { response, .. } => {
                response
                    .send(Err(anyhow!("Noop logger does not support queries")))
                    .unwrap_or(());
            }
            GetStateUpdates { response, .. } => {
                response
                    .send(Err(anyhow!("Noop logger does not support queries")))
                    .unwrap_or(());
            }
            GetTaskSummary { response, .. } => {
                response
                    .send(Err(anyhow!("Noop logger does not support queries")))
                    .unwrap_or(());
            }
            GetTasks { response, .. } => {
                response
                    .send(Err(anyhow!("Noop logger does not support queries")))
                    .unwrap_or(());
            }
            GetTask { response, .. } => {
                response
                    .send(Err(anyhow!("Noop logger does not support queries")))
                    .unwrap_or(());
            }
            Stop {} => break,
            _ => {}
        }
    }
}

/*
#[cfg(test)]
mod tests {
    use super::*;

    fn test_memory_create() {
        let (log_tx, log_rx) = unbounded();
        tokio::spawn(async move {
            memory_logger(log_rx).await;
        });

        use LoggerMessage::*;

        let (tx, rx) = oneshot::channel();
        log_tx.send(CreateRun { response: tx }).await.unwrap();
        let run_id = rx
            .await
            .expect("Receive error")
            .expect("Unable to create run id");
        assert_eq!(run_id, 0);

        let (tx, rx) = oneshot::channel();
        log_tx
            .send(GetRunState {
                run_id: run_id,
                response: tx,
            })
            .await
            .unwrap();
        let state_change = rx.await.unwrap().unwrap();
        assert_eq!(RunState::Queued, state_change.state);
    }
}
 */
