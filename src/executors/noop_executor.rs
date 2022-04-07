use super::*;
use crate::structs::*;
use tokio::sync::{mpsc, oneshot};

pub async fn start_local_executor(mut exe_msgs: mpsc::UnboundedReceiver<ExecutorMessage>) {
    while let Some(msg) = exe_msgs.recv().await {
        use ExecutorMessage::*;

        match msg {
            ValidateTasks { response, .. } => response.send(Ok(())).unwrap_or(()),
            ExpandTasks {
                tasks, response, ..
            } => response.send(Ok(tasks)).unwrap_or(()),
            ExecuteTask {
                task_id,
                response,
                tracker,
                ..
            } => {
                let (upd, _) = oneshot::channel();
                tracker
                    .send(TrackerMessage::UpdateTaskState {
                        task_id: task_id.clone(),
                        state: State::Running,
                        response: upd,
                    })
                    .unwrap_or(());
                let mut attempt = TaskAttempt::new();
                attempt.succeeded = true;
                response
                    .send(RunnerMessage::ExecutionReport {
                        task_id: task_id.clone(),
                        attempt,
                    })
                    .unwrap_or(());
            }
            StopTask { .. } => {}
            Stop {} => {
                break;
            }
        }
    }
}
