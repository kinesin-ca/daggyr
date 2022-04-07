use crate::messages::*;
use crate::structs::*;
use crate::Result;
use mongodb::{
    bson, bson::doc, options::ClientOptions, options::FindOneAndUpdateOptions,
    options::FindOneOptions, Client,
};
use tokio::sync::mpsc;

use futures::TryStreamExt;

pub fn start(url: String, db_name: String, msgs: mpsc::UnboundedReceiver<TrackerMessage>) {
    tokio::spawn(async move {
        start_mongodb_tracker(url, db_name, msgs).await;
    });
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MongoTask {
    #[serde(default)]
    task_id: TaskID,
    #[serde(default)]
    record: TaskRecord,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MongoRun {
    #[serde(default)]
    _id: RunID,
    #[serde(default)]
    tags: RunTags,
    #[serde(default)]
    parameters: Parameters,
    #[serde(default)]
    state_changes: Vec<StateChange>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct MongoCounter {
    #[serde(default)]
    _id: String,
    #[serde(default)]
    value: usize,
}

#[derive(Clone)]
struct MongoTracker {
    client: mongodb::Client,
    db: mongodb::Database,
    counters: mongodb::Collection<MongoCounter>,
    runs: mongodb::Collection<MongoRun>,
    tasks: mongodb::Collection<MongoTask>,
}

impl MongoTracker {
    async fn new(conn: String, db_name: String) -> Self {
        let mut client_options = ClientOptions::parse(conn)
            .await
            .expect("Unable to parse connection string");
        client_options.app_name = Some("daggyr".to_owned());
        let client =
            Client::with_options(client_options).expect("Unable to initialize mongodb client");

        client
            .database("admin")
            .run_command(doc! {"ping": 1u32}, None)
            .await
            .expect("Unable to ping server");

        let db = client.database(&db_name);

        let counters = db.collection::<MongoCounter>("counters");
        let runs = db.collection::<MongoRun>("runs");
        let tasks = db.collection::<MongoTask>("tasks");

        MongoTracker {
            client,
            db,
            counters,
            runs,
            tasks,
        }
    }

    async fn inc_counter(&self, key: &str) -> Result<usize> {
        let result = self
            .counters
            .find_one_and_update(
                doc! { "_id": key },
                doc! {"$inc": { "value": bson::to_bson(&1usize).unwrap() }},
                FindOneAndUpdateOptions::builder().upsert(true).build(),
            )
            .await?;

        let value = match result {
            Some(counter) => counter.value,
            None => 0usize,
        };
        Ok(value)
    }

    async fn create_run(&self, tags: RunTags, parameters: Parameters) -> Result<RunID> {
        let run_id = self.inc_counter(&"run_id").await?;
        let run = MongoRun {
            _id: run_id,
            tags,
            parameters,
            state_changes: vec![StateChange::new(State::Queued)],
        };
        self.runs.insert_one(run, None).await?;
        Ok(run_id)
    }

    async fn add_tasks(&self, tasks: TaskSet) -> Result<()> {
        let records: Vec<MongoTask> = tasks
            .iter()
            .map(|(task_id, task)| {
                let mut mtask = MongoTask {
                    task_id: task_id.clone(),
                    record: TaskRecord::new(task.clone()),
                };
                mtask
                    .record
                    .state_changes
                    .push(StateChange::new(State::Queued));
                mtask
            })
            .collect();
        self.tasks.insert_many(records, None).await?;
        Ok(())
    }
    async fn update_task(&self, task_id: TaskID, task: Task) -> Result<()> {
        let filter = doc! {"task_id": bson::to_bson(&task_id).unwrap()};
        let update = doc! {
            "$set": {
                "record.task": bson::to_bson(&task)?
            }
        };
        self.tasks.update_one(filter, update, None).await?;
        Ok(())
    }
    async fn update_state(&self, run_id: RunID, state: State) -> Result<()> {
        let new_state = StateChange::new(state);
        let filter = doc! {"_id": bson::to_bson(&run_id)? };
        let update = doc! {
            "$push": {
                "state_changes": bson::to_bson(&new_state)?
            }
        };
        self.runs.update_one(filter, update, None).await?;
        Ok(())
    }

    async fn update_task_state(&self, task_id: TaskID, state: State) -> Result<()> {
        let new_state = StateChange::new(state);
        let filter = doc! {"task_id": bson::to_bson(&task_id)? };
        let update = doc! {
            "$push": {
                "record.state_changes": bson::to_bson(&new_state)?
            }
        };
        self.tasks.update_one(filter, update, None).await?;
        Ok(())
    }
    async fn log_task_attempt(&self, task_id: TaskID, attempt: TaskAttempt) -> Result<()> {
        let filter = doc! {"task_id": bson::to_bson(&task_id)? };
        let update = doc! {
            "$push": {
                "record.attempts": bson::to_bson(&attempt)?
            }
        };
        self.tasks.update_one(filter, update, None).await?;
        Ok(())
    }
    async fn get_runs(
        &self,
        tags: RunTags,
        states: HashSet<State>,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Vec<RunSummary> {
        Vec::new()
    }

    async fn get_run(&self, run_id: RunID) -> Result<RunRecord> {
        let filter = doc! {"_id": bson::to_bson(&run_id)? };
        if let Some(run) = self.runs.find_one(filter, None).await? {
            let mut tasks = HashMap::new();
            let mut cursor = self
                .tasks
                .find(doc! { "run_id": bson::to_bson(&run_id)? }, None)
                .await?;

            while let Some(mtask) = cursor.try_next().await? {
                tasks.insert(mtask.task_id.clone(), mtask.record.clone());
            }
            Ok(RunRecord {
                tags: run.tags,
                parameters: run.parameters,
                tasks,
                state_changes: run.state_changes,
            })
        } else {
            Err(anyhow!(format!("No such run id: {}", run_id)))
        }
    }

    async fn get_state(&self, run_id: RunID) -> Result<StateChange> {
        let filter = doc! {"_id": bson::to_bson(&run_id).unwrap() };
        let options = FindOneOptions::builder()
            .projection(doc! { "state_changes": 1u32})
            .build();
        if let Some(run) = self.runs.find_one(filter, options).await.unwrap() {
            Ok(run.state_changes.last().unwrap().clone())
        } else {
            Err(anyhow!(format!("No such run id: {}", run_id)))
        }
    }

    async fn get_state_updates(&self, run_id: RunID) -> Result<Vec<StateChange>> {
        let filter = doc! {"_id": bson::to_bson(&run_id).unwrap() };
        let options = FindOneOptions::builder()
            .projection(doc! { "state_changes": 1u32})
            .build();
        if let Some(run) = self.runs.find_one(filter, options).await? {
            Ok(run.state_changes)
        } else {
            Err(anyhow!(format!("No such run id: {}", run_id)))
        }
    }
    async fn get_task_summary(&self, run_id: RunID) -> Result<Vec<TaskSummary>> {
        Ok(Vec::new())
    }
    async fn get_tasks(&self, run_id: RunID) -> Result<HashMap<TaskID, TaskRecord>> {
        Ok(HashMap::new())
    }
    async fn get_task(&self, task_id: TaskID) -> Result<TaskRecord> {
        Ok(TaskRecord::new(Task::new()))
    }
}

pub async fn start_mongodb_tracker(
    conn: String,
    db_name: String,
    mut msgs: mpsc::UnboundedReceiver<TrackerMessage>,
) {
    // We get a few different collections here
    let tracker = MongoTracker::new(conn, db_name).await;

    while let Some(msg) = msgs.recv().await {
        use TrackerMessage::*;

        match msg {
            CreateRun {
                tags,
                parameters,
                response,
            } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response
                        .send(t.create_run(tags, parameters).await)
                        .unwrap_or(());
                });
            }
            AddTasks {
                tasks, response, ..
            } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response.send(t.add_tasks(tasks).await).unwrap_or(());
                });
            }
            UpdateTask {
                task_id,
                task,
                response,
            } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response
                        .send(t.update_task(task_id.clone(), task.clone()).await)
                        .unwrap_or(());
                });
            }
            UpdateState {
                run_id,
                state,
                response,
            } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response
                        .send(t.update_state(run_id, state).await)
                        .unwrap_or(());
                });
            }
            UpdateTaskState {
                task_id,
                state,
                response,
            } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response
                        .send(t.update_task_state(task_id.clone(), state).await)
                        .unwrap_or(());
                });
            }
            LogTaskAttempt {
                task_id,
                attempt,
                response,
            } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response
                        .send(t.log_task_attempt(task_id.clone(), attempt.clone()).await)
                        .unwrap_or(());
                });
            }
            GetRuns {
                tags,
                states,
                start_time,
                end_time,
                response,
            } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response
                        .send(
                            t.get_runs(
                                tags.clone(),
                                states.clone(),
                                start_time.clone(),
                                end_time.clone(),
                            )
                            .await,
                        )
                        .unwrap_or(());
                });
            }
            GetRun { run_id, response } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response.send(t.get_run(run_id).await).unwrap_or(());
                });
            }
            GetState { run_id, response } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response.send(t.get_state(run_id).await).unwrap_or(());
                });
            }
            GetStateUpdates { run_id, response } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response
                        .send(t.get_state_updates(run_id).await)
                        .unwrap_or(());
                });
            }
            GetTaskSummary { run_id, response } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response
                        .send(t.get_task_summary(run_id).await)
                        .unwrap_or(());
                });
            }
            GetTasks { run_id, response } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response.send(t.get_tasks(run_id).await).unwrap_or(());
                });
            }
            GetTask { task_id, response } => {
                let t = tracker.clone();
                tokio::spawn(async move {
                    response
                        .send(t.get_task(task_id.clone()).await)
                        .unwrap_or(());
                });
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
    async fn test_mongodb_create() {
        let (trx_tx, trx_rx) = mpsc::unbounded_channel();
        start(
            "mongodb://localhost:27017".to_string(),
            "daggyr_test".to_string(),
            trx_rx,
        );

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
        println!("Run ID is {}", run_id);

        // Verify current state
        {
            let (tx, rx) = oneshot::channel();
            trx_tx
                .send(GetState {
                    run_id: run_id,
                    response: tx,
                })
                .unwrap();
            let state_change = rx
                .await
                .expect("Receive error")
                .expect("Unable to get state");
            assert!(state_change.state == State::Queued);
        }

        // Update State
        {
            let (response, rx) = oneshot::channel();
            trx_tx
                .send(UpdateState {
                    run_id: run_id,
                    state: State::Running,
                    response,
                })
                .unwrap();
            rx.await;
            let (tx, rx) = oneshot::channel();
            trx_tx
                .send(GetState {
                    run_id: run_id,
                    response: tx,
                })
                .unwrap();
            let state_change = rx
                .await
                .expect("Receive error")
                .expect("Unable to get state");
            assert!(state_change.state == State::Running);
        }

        // Adding some tasks
        {
            let tasks_spec: TaskSetSpec = serde_json::from_str(
                r#"
            {
                "simple_task": {
                    "details": {
                        "command": [ "/bin/echo", "hello", "world" ],
                        "env": {
                            "ENVVAR": 16
                        }
                    },
                    "children": [ "other_task" ],
                    "is_generator": true
                },
                "other_task": {
                    "details": {
                        "command": [ "/bin/echo", "task", "$ENVVAR" ],
                        "env": {
                            "ENVVAR": 16
                        }
                    }
                }
            }"#,
            )
            .unwrap();

            let tasks = tasks_spec.to_task_set();

            let (response, rx) = oneshot::channel();
            trx_tx
                .send(AddTasks {
                    run_id: run_id.clone(),
                    tasks,
                    response,
                })
                .unwrap();
            rx.await;
        }

        // Getting entire run
        {
            let (tx, rx) = oneshot::channel();
            trx_tx
                .send(GetRun {
                    run_id,
                    response: tx,
                })
                .unwrap();
            let run = rx.await.unwrap().unwrap();
            assert_eq!(State::Running, run.state_changes.last().unwrap().state);
        };
    }
}
