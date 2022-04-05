use crate::messages::*;
use crate::structs::*;
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

async fn inc_counter(col: &mongodb::Collection<MongoCounter>, key: &str) -> usize {
    let result = col
        .find_one_and_update(
            doc! { "_id": key },
            doc! {"$inc": { "value": bson::to_bson(&1usize).unwrap() }},
            FindOneAndUpdateOptions::builder().upsert(true).build(),
        )
        .await
        .unwrap();

    match result {
        Some(counter) => counter.value,
        None => 0usize,
    }
}

pub async fn start_mongodb_tracker(
    conn: String,
    db_name: String,
    mut msgs: mpsc::UnboundedReceiver<TrackerMessage>,
) {
    let mut client_options = ClientOptions::parse(conn)
        .await
        .expect("Unable to parse connection string");
    client_options.app_name = Some("daggyr".to_owned());
    let client = Client::with_options(client_options).expect("Unable to initialize mongodb client");

    client
        .database("admin")
        .run_command(doc! {"ping": 1u32}, None)
        .await
        .expect("Unable to ping mongodb server");

    let db = client.database(&db_name);

    // We get a few different collections here
    let counters_col = db.collection::<MongoCounter>("counters");
    let runs_col = db.collection::<MongoRun>("runs");
    let tasks_col = db.collection::<MongoTask>("tasks");

    while let Some(msg) = msgs.recv().await {
        use TrackerMessage::*;

        match msg {
            CreateRun {
                tags,
                parameters,
                response,
            } => {
                let counters_col_ref = counters_col.clone();
                let runs_col_ref = runs_col.clone();
                tokio::spawn(async move {
                    // Get the new RunID
                    let run_id = inc_counter(&counters_col_ref, &"run_id").await;

                    // Insert it
                    let run = MongoRun {
                        _id: run_id,
                        tags,
                        parameters,
                        state_changes: vec![StateChange::new(State::Queued)],
                    };
                    runs_col_ref.insert_one(run, None).await.unwrap();
                    response.send(Ok(run_id)).unwrap_or(());
                });
            }
            AddTasks { tasks, .. } => {
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
                let tasks_col_ref = tasks_col.clone();
                tokio::spawn(async move {
                    tasks_col_ref.insert_many(records, None).await.unwrap();
                });
            }
            UpdateTask { task_id, task } => {
                let filter = doc! {"task_id": bson::to_bson(&task_id).unwrap()};
                let update = doc! {
                    "$set": {
                        "record.task": bson::to_bson(&task).unwrap()
                    }
                };
                let tasks_col_ref = tasks_col.clone();
                tokio::spawn(async move {
                    tasks_col_ref
                        .update_one(filter, update, None)
                        .await
                        .unwrap();
                });
            }
            UpdateState { run_id, state } => {
                let new_state = StateChange::new(state);
                let filter = doc! {"_id": bson::to_bson(&run_id).unwrap()};
                let update = doc! {
                    "$push": {
                        "state_changes": bson::to_bson(&new_state).unwrap()
                    }
                };
                let runs_col_ref = runs_col.clone();
                tokio::spawn(async move {
                    runs_col_ref.update_one(filter, update, None).await.unwrap();
                });
            }
            UpdateTaskState { task_id, state } => {
                let new_state = StateChange::new(state);
                let filter = doc! {"task_id": bson::to_bson(&task_id).unwrap() };
                let update = doc! {
                    "$push": {
                        "record.state_changes": bson::to_bson(&new_state).unwrap()
                    }
                };
                let tasks_col_ref = runs_col.clone();
                tokio::spawn(async move {
                    tasks_col_ref
                        .update_one(filter, update, None)
                        .await
                        .unwrap();
                });
            }
            LogTaskAttempt { task_id, attempt } => {
                let filter = doc! {"task_id": bson::to_bson(&task_id).unwrap() };
                let update = doc! {
                    "$push": {
                        "record.attempts": bson::to_bson(&attempt).unwrap()
                    }
                };
                let tasks_col_ref = runs_col.clone();
                tokio::spawn(async move {
                    tasks_col_ref
                        .update_one(filter, update, None)
                        .await
                        .unwrap();
                });
            }
            GetRuns { ..
                /*
                tags,
                states,
                start_time,
                end_time,
                response,
                */
            } => {}
            GetRun { run_id, response } => {
                let filter = doc! {"_id": bson::to_bson(&run_id).unwrap() };
                let runs_col_ref = runs_col.clone();
                let tasks_col_ref = tasks_col.clone();
                tokio::spawn(async move {
                    if let Some(run) = runs_col_ref.find_one(filter, None).await.unwrap() {
                        let mut tasks = HashMap::new();
                        let mut cursor = tasks_col_ref
                            .find(doc! { "run_id": bson::to_bson(&run_id).unwrap() }, None)
                            .await
                            .unwrap();

                        while let Some(mtask) = cursor.try_next().await.unwrap() {
                            tasks.insert(mtask.task_id.clone(), mtask.record.clone());
                        }
                        response
                            .send(Ok(RunRecord {
                                tags: run.tags,
                                parameters: run.parameters,
                                tasks,
                                state_changes: run.state_changes,
                            }))
                            .unwrap_or(());
                    } else {
                        response.send(Err(anyhow!("No such run"))).unwrap_or(());
                    }
                });
            }
            GetState { run_id, response } => {
                let runs_col_ref = runs_col.clone();
                let filter = doc! {"_id": bson::to_bson(&run_id).unwrap() };
                let options = FindOneOptions::builder()
                    .projection(doc! { "state_changes": 1u32})
                    .build();
                tokio::spawn(async move {
                    if let Some(run) = runs_col_ref.find_one(filter, options).await.unwrap() {
                        response
                            .send(Ok(run.state_changes.last().unwrap().clone()))
                            .unwrap_or(());
                    } else {
                        response.send(Err(anyhow!("No such run"))).unwrap_or(());
                    }
                });
            }
            GetStateUpdates { run_id, response } => {
                let runs_col_ref = runs_col.clone();
                let filter = doc! {"_id": bson::to_bson(&run_id).unwrap() };
                let options = FindOneOptions::builder()
                    .projection(doc! { "state_changes": 1u32})
                    .build();
                let runs_col_ref = runs_col.clone();
                tokio::spawn(async move {
                    if let Some(run) = runs_col_ref.find_one(filter, options).await.unwrap() {
                        response.send(Ok(run.state_changes)).unwrap_or(());
                    } else {
                        response.send(Err(anyhow!("No such run"))).unwrap_or(());
                    }
                });
            }
            GetTaskSummary { run_id, response } => {}
            GetTasks { run_id, response } => {}
            GetTask { task_id, response } => {
                //let runs_col_ref = runs_col.clone();
                //let filter = doc! {"_id": run_id.clone() };
                // tokio::spawn(async move { //let mut cursor = runs_col_ref.find(filter); });
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
            trx_tx
                .send(UpdateState {
                    run_id: run_id,
                    state: State::Running,
                })
                .unwrap();
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

            trx_tx
                .send(AddTasks {
                    run_id: run_id.clone(),
                    tasks,
                })
                .unwrap();
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
        }
    }
}
