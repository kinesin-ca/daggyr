use crate::messages::*;
use crate::structs::*;
use crate::Result;
use tokio::sync::mpsc;

fn range_checker(runs: &Vec<RunRecord>, task_id: TaskID) -> Result<()> {
    if task_id.run_id >= runs.len() {
        return Err(anyhow!("No run with ID {} exists", task_id.run_id));
    }

    if !runs[task_id.run_id].tasks.contains_key(&task_id) {
        return Err(anyhow!("No task with ID {:?}", task_id));
    }

    Ok(())
}

pub fn start(msgs: mpsc::UnboundedReceiver<TrackerMessage>) {
    tokio::spawn(async move {
        start_memory_tracker(msgs).await;
    });
}

pub async fn start_memory_tracker(mut msgs: mpsc::UnboundedReceiver<TrackerMessage>) {
    let mut runs: Vec<RunRecord> = vec![];

    while let Some(msg) = msgs.recv().await {
        use TrackerMessage::*;

        match msg {
            CreateRun {
                tags,
                parameters,
                response,
            } => {
                let new_id = runs.len();
                runs.push(RunRecord::new(tags, parameters));
                response.send(Ok(new_id)).unwrap_or(());
            }
            AddTasks { run_id, tasks } => {
                if run_id < runs.len() {
                    let run = &mut runs[run_id];
                    for (key, task) in tasks.iter() {
                        let mut task_record = TaskRecord::new(task.clone());
                        task_record
                            .state_changes
                            .push(StateChange::new(State::Queued));
                        run.tasks.insert(key.clone(), task_record);
                    }
                }
            }
            UpdateTask { task_id, task } => {
                if range_checker(&runs, task_id.clone()).is_ok() {
                    runs[task_id.run_id].tasks.get_mut(&task_id).unwrap().task = task;
                };
            }
            UpdateState { run_id, state } => {
                if run_id < runs.len() {
                    runs[run_id].state_changes.push(StateChange::new(state));
                };
            }
            UpdateTaskState { task_id, state } => {
                if range_checker(&runs, task_id.clone()).is_ok() {
                    runs[task_id.run_id]
                        .tasks
                        .get_mut(&task_id)
                        .unwrap()
                        .state_changes
                        .push(StateChange::new(state));
                };
            }
            LogTaskAttempt { task_id, attempt } => {
                if range_checker(&runs, task_id.clone()).is_ok() {
                    runs[task_id.run_id]
                        .tasks
                        .get_mut(&task_id)
                        .unwrap()
                        .attempts
                        .push(attempt);
                };
            }
            GetRuns {
                tags,
                states,
                start_time,
                end_time,
                response,
            } => {
                let mut records = Vec::new();
                let default_state = StateChange::new(State::Queued);

                for (i, run) in runs.iter().enumerate() {
                    if !tags.is_empty() && !run.tags.matches(&tags) {
                        continue;
                    }

                    let run_state = run
                        .state_changes
                        .last()
                        .unwrap_or(&default_state)
                        .state
                        .clone();

                    let run_start_time = run
                        .state_changes
                        .first()
                        .unwrap_or(&default_state)
                        .datetime
                        .clone();

                    let end_task_time = run
                        .tasks
                        .iter()
                        .map(|(_, task)| {
                            task.state_changes
                                .last()
                                .unwrap_or(&default_state)
                                .datetime
                                .clone()
                        })
                        .max()
                        .unwrap_or(default_state.datetime.clone());

                    if !(states.is_empty() || states.contains(&run_state)) {
                        continue;
                    }

                    if run_start_time < start_time {
                        continue;
                    }
                    if run_start_time > end_time {
                        continue;
                    }

                    let mut record = RunSummary::new(i, run.tags.clone(), run_state);
                    record.start_time = run_start_time;
                    record.last_update_time = end_task_time;

                    for (_, task_record) in run.tasks.iter() {
                        let task_state = task_record
                            .state_changes
                            .last()
                            .unwrap_or(&default_state)
                            .state
                            .clone();

                        let counter = record.task_states.entry(task_state).or_insert(0);
                        *counter += 1;
                    }

                    records.push(record);
                }
                response.send(records).unwrap_or(());
            }
            GetRun { run_id, response } => {
                let result = if run_id < runs.len() {
                    Ok(runs[run_id].clone())
                } else {
                    Err(anyhow!("No such run"))
                };
                response.send(result).unwrap_or(());
            }
            GetState { run_id, response } => {
                let default_state = StateChange::new(State::Queued);
                let result = if run_id < runs.len() {
                    Ok(runs[run_id]
                        .state_changes
                        .last()
                        .unwrap_or(&default_state)
                        .clone())
                } else {
                    Err(anyhow!("No such run"))
                };
                response.send(result).unwrap_or(());
            }
            GetStateUpdates { run_id, response } => {
                let result = if run_id < runs.len() {
                    Ok(runs[run_id].state_changes.clone())
                } else {
                    Err(anyhow!("No such run"))
                };
                response.send(result).unwrap_or(());
            }
            GetTaskSummary { run_id, response } => {
                let default_state = StateChange::new(State::Queued);
                let result = if run_id < runs.len() {
                    let mut tasks = Vec::new();
                    for (tid, task_rec) in runs[run_id].tasks.iter() {
                        tasks.push(TaskSummary {
                            task_id: tid.clone(),
                            state: task_rec
                                .state_changes
                                .last()
                                .unwrap_or(&default_state)
                                .state,
                        });
                    }
                    Ok(tasks)
                } else {
                    Err(anyhow!("No such run"))
                };
                response.send(result).unwrap_or(());
            }
            GetTasks { run_id, response } => {
                let result = if run_id < runs.len() {
                    Ok(runs[run_id].tasks.clone())
                } else {
                    Err(anyhow!("No such run"))
                };
                response.send(result).unwrap_or(());
            }
            GetTask { task_id, response } => {
                let result = match range_checker(&runs, task_id.clone()) {
                    Ok(()) => Ok(runs[task_id.run_id].tasks.get(&task_id).unwrap().clone()),
                    Err(e) => Err(e),
                };
                response.send(result).unwrap_or(());
            }
            Stop {} => break,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_memory_create() {
        let (trx_tx, trx_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            start_memory_tracker(trx_rx).await;
        });

        use TrackerMessage::*;

        let (tx, rx) = oneshot::channel();
        trx_tx
            .send(CreateRun {
                tags: RunTags::new(),
                parameters: Parameters::new(),
                response: tx,
            })
            .unwrap();
        let run_id = rx
            .await
            .expect("Receive error")
            .expect("Unable to create run id");
        assert_eq!(run_id, 0);

        let (tx, rx) = oneshot::channel();
        trx_tx
            .send(GetState {
                run_id: run_id,
                response: tx,
            })
            .unwrap();
        let state_change = rx.await.unwrap().unwrap();
        assert_eq!(State::Queued, state_change.state);
    }
}
