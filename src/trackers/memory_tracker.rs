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

// Pulling this all out into a structure makes it easier to short-circuit
// message processing if an error occurs.
struct MemoryTracker {
    runs: Vec<RunRecord>,
}

impl MemoryTracker {
    fn new() -> Self {
        MemoryTracker { runs: Vec::new() }
    }

    fn range_checker(&self, task_id: &TaskID) -> Result<()> {
        if task_id.run_id >= self.runs.len() {
            return Err(anyhow!("No run with ID {} exists", task_id.run_id));
        }

        if !self.runs[task_id.run_id].tasks.contains_key(&task_id) {
            return Err(anyhow!("No task with ID {:?}", task_id));
        }

        Ok(())
    }

    fn create_run(&mut self, tags: RunTags, parameters: Parameters) -> Result<RunID> {
        let run_id = self.runs.len();
        self.runs.push(RunRecord::new(tags, parameters));
        Ok(run_id)
    }

    fn add_tasks(&mut self, run_id: RunID, tasks: TaskSet) -> Result<()> {
        if run_id >= self.runs.len() {
            return Err(anyhow!(format!("No such run id: {}", run_id)));
        }

        let run = &mut self.runs[run_id];
        for (key, task) in tasks.iter() {
            let mut task_record = TaskRecord::new(task.clone());
            task_record
                .state_changes
                .push(StateChange::new(State::Queued));
            run.tasks.insert(key.clone(), task_record);
        }
        Ok(())
    }

    fn update_task(&mut self, task_id: TaskID, task: Task) -> Result<()> {
        self.range_checker(&task_id)?;
        self.runs[task_id.run_id]
            .tasks
            .get_mut(&task_id)
            .unwrap()
            .task = task;
        Ok(())
    }

    fn update_state(&mut self, run_id: RunID, state: State) -> Result<()> {
        if run_id < self.runs.len() {
            self.runs[run_id]
                .state_changes
                .push(StateChange::new(state));
            Ok(())
        } else {
            return Err(anyhow!(format!("No such run id: {}", run_id)));
        }
    }

    fn update_task_state(&mut self, task_id: TaskID, state: State) -> Result<()> {
        self.range_checker(&task_id)?;
        self.runs[task_id.run_id]
            .tasks
            .get_mut(&task_id)
            .unwrap()
            .state_changes
            .push(StateChange::new(state));
        Ok(())
    }

    fn log_task_attempt(&mut self, task_id: TaskID, attempt: TaskAttempt) -> Result<()> {
        self.range_checker(&task_id)?;
        self.runs[task_id.run_id]
            .tasks
            .get_mut(&task_id)
            .unwrap()
            .attempts
            .push(attempt);
        Ok(())
    }

    fn get_runs(
        &self,
        tags: Option<RunTags>,
        states: Option<HashSet<State>>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
    ) -> Vec<RunSummary> {
        let mut records = Vec::new();
        let default_state = StateChange::new(State::Queued);

        for (i, run) in self.runs.iter().enumerate() {
            if let Some(filter_tags) = tags.clone() {
                if !filter_tags.is_subset_of(&run.tags) {
                    continue;
                }
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

            if let Some(filter_states) = states.clone() {
                if !(filter_states.contains(&run_state)) {
                    continue;
                }
            }

            if let Some(filter_start_time) = start_time {
                if run_start_time < filter_start_time {
                    continue;
                }
            }

            if let Some(filter_end_time) = end_time {
                if run_start_time > filter_end_time {
                    continue;
                }
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
        records
    }

    fn get_run(&self, run_id: RunID) -> Result<RunRecord> {
        if run_id < self.runs.len() {
            Ok(self.runs[run_id].clone())
        } else {
            Err(anyhow!("No such run"))
        }
    }

    fn get_state(&self, run_id: RunID) -> Result<StateChange> {
        let default_state = StateChange::new(State::Queued);
        if run_id < self.runs.len() {
            Ok(self.runs[run_id]
                .state_changes
                .last()
                .unwrap_or(&default_state)
                .clone())
        } else {
            Err(anyhow!("No such run"))
        }
    }

    fn get_state_updates(&self, run_id: RunID) -> Result<Vec<StateChange>> {
        if run_id < self.runs.len() {
            Ok(self.runs[run_id].state_changes.clone())
        } else {
            Err(anyhow!("No such run"))
        }
    }

    fn get_task_summary(&self, run_id: RunID) -> Result<Vec<TaskSummary>> {
        let default_state = StateChange::new(State::Queued);
        if run_id < self.runs.len() {
            let mut tasks = Vec::new();
            for (tid, task_rec) in self.runs[run_id].tasks.iter() {
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
        }
    }

    fn get_tasks(&self, run_id: RunID) -> Result<HashMap<TaskID, TaskRecord>> {
        if run_id < self.runs.len() {
            Ok(self.runs[run_id].tasks.clone())
        } else {
            Err(anyhow!("No such run"))
        }
    }

    fn get_task(&self, task_id: TaskID) -> Result<TaskRecord> {
        self.range_checker(&task_id)?;
        Ok(self.runs[task_id.run_id]
            .tasks
            .get(&task_id)
            .unwrap()
            .clone())
    }
}

pub async fn start_memory_tracker(mut msgs: mpsc::UnboundedReceiver<TrackerMessage>) {
    let mut tracker = MemoryTracker::new();

    while let Some(msg) = msgs.recv().await {
        use TrackerMessage::*;

        match msg {
            CreateRun {
                tags,
                parameters,
                response,
            } => {
                response
                    .send(tracker.create_run(tags.clone(), parameters.clone()))
                    .unwrap_or(());
            }
            AddTasks {
                run_id,
                tasks,
                response,
            } => {
                response
                    .send(tracker.add_tasks(run_id, tasks.clone()))
                    .unwrap_or(());
            }
            UpdateTask {
                task_id,
                task,
                response,
            } => {
                response
                    .send(tracker.update_task(task_id.clone(), task.clone()))
                    .unwrap_or(());
            }
            UpdateState {
                run_id,
                state,
                response,
            } => {
                response
                    .send(tracker.update_state(run_id, state))
                    .unwrap_or(());
            }
            UpdateTaskState {
                task_id,
                state,
                response,
            } => {
                response
                    .send(tracker.update_task_state(task_id.clone(), state))
                    .unwrap_or(());
            }
            LogTaskAttempt {
                task_id,
                attempt,
                response,
            } => {
                response
                    .send(tracker.log_task_attempt(task_id.clone(), attempt.clone()))
                    .unwrap_or(());
            }
            GetRuns {
                tags,
                states,
                start_time,
                end_time,
                response,
            } => {
                response
                    .send(Ok(tracker.get_runs(
                        tags.clone(),
                        states.clone(),
                        start_time.clone(),
                        end_time.clone(),
                    )))
                    .unwrap_or(());
            }
            GetRun { run_id, response } => {
                response.send(tracker.get_run(run_id)).unwrap_or(());
            }
            GetState { run_id, response } => {
                response.send(tracker.get_state(run_id)).unwrap_or(());
            }
            GetStateUpdates { run_id, response } => {
                response
                    .send(tracker.get_state_updates(run_id))
                    .unwrap_or(());
            }
            GetTaskSummary { run_id, response } => {
                response
                    .send(tracker.get_task_summary(run_id))
                    .unwrap_or(());
            }
            GetTasks { run_id, response } => {
                response.send(tracker.get_tasks(run_id)).unwrap_or(());
            }
            GetTask { task_id, response } => {
                response
                    .send(tracker.get_task(task_id.clone()))
                    .unwrap_or(());
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
