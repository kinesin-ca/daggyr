use crate::messages::*;
use tokio::sync::mpsc;

pub fn start(msgs: mpsc::UnboundedReceiver<TrackerMessage>) {
    tokio::spawn(async move {
        start_noop_tracker(msgs).await;
    });
}

pub async fn start_noop_tracker(mut msgs: mpsc::UnboundedReceiver<TrackerMessage>) {
    while let Some(msg) = msgs.recv().await {
        use TrackerMessage::*;

        match msg {
            CreateRun { response, .. } => {
                response.send(Ok(0)).unwrap_or(());
            }
            GetRuns { response, .. } => {
                let records = Vec::new();
                response.send(Ok(records)).unwrap_or(());
            }
            GetRun { response, .. } => {
                response
                    .send(Err(anyhow!("Noop tracker does not support queries")))
                    .unwrap_or(());
            }
            GetState { response, .. } => {
                response
                    .send(Err(anyhow!("Noop tracker does not support queries")))
                    .unwrap_or(());
            }
            GetStateUpdates { response, .. } => {
                response
                    .send(Err(anyhow!("Noop tracker does not support queries")))
                    .unwrap_or(());
            }
            GetTaskSummary { response, .. } => {
                response
                    .send(Err(anyhow!("Noop tracker does not support queries")))
                    .unwrap_or(());
            }
            GetTasks { response, .. } => {
                response
                    .send(Err(anyhow!("Noop tracker does not support queries")))
                    .unwrap_or(());
            }
            GetTask { response, .. } => {
                response
                    .send(Err(anyhow!("Noop tracker does not support queries")))
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
        let (trx_tx, trx_rx) = unbounded();
        tokio::spawn(async move {
            memory_tracker(trx_rx).await;
        });

        use TrackerMessage::*;

        let (tx, rx) = oneshot::channel();
        trx_tx.send(CreateRun { response: tx }).await.unwrap();
        let run_id = rx
            .await
            .expect("Receive error")
            .expect("Unable to create run id");
        assert_eq!(run_id, 0);

        let (tx, rx) = oneshot::channel();
        trx_tx
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
