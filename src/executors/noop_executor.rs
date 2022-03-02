use super::*;
use crate::structs::*;
use tokio::sync::mpsc;

pub async fn start_local_executor(mut exe_msgs: mpsc::Receiver<ExecutorMessage>) {
    while let Some(msg) = exe_msgs.recv().await {
        use ExecutorMessage::*;

        match msg {
            ValidateTasks { response, .. } => response.send(Ok(())).unwrap_or(()),
            ExpandTasks {
                tasks, response, ..
            } => response.send(Ok(tasks)).unwrap_or(()),
            ExecuteTask {
                run_id,
                task_id,
                response,
                logger,
                ..
            } => {
                logger
                    .send(LoggerMessage::UpdateTaskState {
                        run_id: run_id,
                        task_id: task_id,
                        state: State::Running,
                    })
                    .await
                    .unwrap_or(());
                let mut attempt = TaskAttempt::new();
                attempt.succeeded = true;
                response
                    .send(RunnerMessage::ExecutionReport {
                        run_id,
                        task_id,
                        attempt,
                    })
                    .await
                    .unwrap_or(());
            }
            StopTask { .. } => {}
            Stop {} => {
                break;
            }
        }
    }
}
