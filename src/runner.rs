use super::Result;
use crate::dag::DAG;
use crate::messages::{ExecutorMessage, RunnerMessage, TrackerMessage};
use crate::structs::{
    Parameters, RunID, RunTags, State, Task, TaskAttempt, TaskDetails, TaskID, TaskSet, TaskType,
};
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

/// A Run comprises all of the runtime information for an
/// executing task DAG.
struct Run {
    run_id: RunID,
    tasks: TaskSet,
    dag: DAG<TaskID>,
    state: State,
    parameters: Parameters,
    tracker: mpsc::UnboundedSender<TrackerMessage>,
    executor: mpsc::UnboundedSender<ExecutorMessage>,
    runner: mpsc::UnboundedSender<RunnerMessage>,
}

impl Run {
    async fn new(
        tags: RunTags,
        tasks: TaskSet,
        parameters: Parameters,
        tracker: mpsc::UnboundedSender<TrackerMessage>,
        executor: mpsc::UnboundedSender<ExecutorMessage>,
        runner: mpsc::UnboundedSender<RunnerMessage>,
    ) -> Result<Self> {
        let mut run = Run {
            run_id: 0,
            tasks: TaskSet::new(),
            dag: DAG::new(),
            state: State::Queued,
            parameters,
            tracker: tracker.clone(),
            executor,
            runner,
        };

        // Expand the tasks
        let expanded_tasks = run.expand_tasks(tasks).await?;

        // Create the run ID and update the tracker
        let (tx, rx) = oneshot::channel();
        tracker
            .send(TrackerMessage::CreateRun {
                tags,
                parameters: run.parameters.clone(),
                response: tx,
            })
            .unwrap();
        run.run_id = rx.await??;

        // Set the RunID on the tasks
        let mut updated_tasks = TaskSet::new();
        for (task_id, mut task) in expanded_tasks {
            task.run_id = run.run_id;
            updated_tasks.insert(task_id, task);
        }

        run.add_tasks(&updated_tasks)?;

        // Let the tracker know
        let (response, rx) = oneshot::channel();
        run.tracker
            .send(TrackerMessage::AddTasks {
                run_id: run.run_id,
                tasks: updated_tasks,
                response,
            })
            .unwrap();
        rx.await.unwrap()?;

        run.update_state(State::Running).await?;

        Ok(run)
    }

    async fn expand_tasks(&self, tasks: TaskSet) -> Result<TaskSet> {
        if self.parameters.is_empty() {
            return Ok(tasks);
        }
        let mut expanded_tasks = TaskSet::new();
        for (task_id, mut task) in tasks {
            let mut task_parameters = self.parameters.clone();
            task_parameters.extend(task.parameters.clone().into_iter());
            let (tx, rx) = oneshot::channel();
            self.executor
                .send(ExecutorMessage::ExpandTaskDetails {
                    details: task.details.clone(),
                    parameters: task_parameters,
                    response: tx,
                })
                .unwrap();
            let exp_tasks = rx.await??;

            // If this is a simple task, add it directly
            if exp_tasks.len() == 1 {
                let (details, exp_values) = exp_tasks.first().unwrap();
                task.details = details.clone();
                task.expansion_values = exp_values.clone();
                expanded_tasks.insert(task_id, task);
            } else {
                // Need to create a head and tail node

                // Parents go into head, children go into tail
                let mut head = Task::new();
                head.task_type = TaskType::Structural;
                head.parents = task.parents.clone();

                let head_id = task_id.clone();

                // The tail task is the collector
                let mut tail = Task::new();
                tail.task_type = TaskType::Structural;
                tail.children = task.children.clone();
                let tail_id = format!("{}.tail", task_id);
                expanded_tasks.insert(tail_id.clone(), tail);

                // Build out the interior jobs
                let template = Task {
                    children: vec![tail_id.clone()],
                    parents: Vec::new(),
                    task_type: TaskType::Normal,
                    ..task
                };

                for (details, expansion_values) in exp_tasks {
                    let interior = Task {
                        expansion_values,
                        details,
                        ..template.clone()
                    };

                    // Name are: {task_id}[.PARAM:VALUE]+
                    let name = format!(
                        "{}.{}",
                        task_id,
                        interior
                            .expansion_values
                            .iter()
                            .map(|(k, v)| format!("{}:{}", k, v))
                            .collect::<Vec<_>>()
                            .join(".")
                    );

                    head.children.push(name.clone());
                    expanded_tasks.insert(name.clone(), interior);
                }

                // Insert the head
                expanded_tasks.insert(head_id, head);
            }
        }
        Ok(expanded_tasks)
    }

    /// Retrieve an existing run from the tracker and reset it
    /// to get it ready to run
    async fn from_tracker(
        run_id: RunID,
        tracker: mpsc::UnboundedSender<TrackerMessage>,
        executor: mpsc::UnboundedSender<ExecutorMessage>,
        runner: mpsc::UnboundedSender<RunnerMessage>,
    ) -> Result<Self> {
        let (tx, rx) = oneshot::channel();
        tracker
            .send(TrackerMessage::GetRun {
                run_id,
                response: tx,
            })
            .unwrap();
        let run_record = rx.await??;

        // Build the set of tasks and states
        let mut tasks = TaskSet::new();

        // States for previously run tasks are reset to queued
        let mut states = HashMap::new();

        for (task_id, tr) in run_record.tasks {
            tasks.insert(task_id.clone(), tr.task);
            let new_state = match tr.state_changes.last() {
                Some(change) => match change.state {
                    State::Completed => State::Completed,
                    _ => State::Queued,
                },
                None => State::Queued,
            };

            states.insert(task_id, new_state);
        }

        // Build the run
        let mut run = Run {
            run_id,
            tasks: TaskSet::new(),
            dag: DAG::new(),
            state: State::Running,
            parameters: run_record.parameters,
            tracker,
            executor,
            runner,
        };

        run.add_tasks(&tasks)?;

        // Update the task states
        let mut responses = Vec::new();
        for (task_id, state) in states {
            run.dag.set_vertex_state(&task_id, state)?;
            let (response, rx) = oneshot::channel();
            run.tracker
                .send(TrackerMessage::UpdateTaskState {
                    run_id,
                    task_id,
                    state,
                    response,
                })
                .unwrap();
            responses.push(rx);
        }
        for rx in responses {
            rx.await??;
        }

        run.update_state(State::Running).await?;
        Ok(run)
    }

    async fn update_state(&mut self, state: State) -> Result<()> {
        let (response, rx) = oneshot::channel();
        self.tracker
            .send(TrackerMessage::UpdateState {
                run_id: self.run_id,
                state,
                response,
            })
            .unwrap();
        rx.await??;
        self.state = state;
        Ok(())
    }

    /// Adds the tasks and sets up the DAG
    fn add_tasks(&mut self, tasks: &TaskSet) -> Result<()> {
        let task_ids: Vec<TaskID> = tasks.keys().cloned().collect();
        // Add vertices
        self.dag.add_vertices(&task_ids)?;

        // Insert edges
        for (task_id, task) in tasks.iter() {
            for child in &task.children {
                self.dag.add_edge(task_id, child)?;
            }

            // Same for parents, but in the other direction
            for parent in &task.parents {
                self.dag.add_edge(parent, task_id)?;
            }
        }

        self.tasks.extend(tasks.clone());

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    fn submit_task(&self, task_id: TaskID, details: TaskDetails) -> Result<()> {
        if let Err(e) = self.executor.send(ExecutorMessage::ExecuteTask {
            run_id: self.run_id,
            task_id,
            details,
            response: self.runner.clone(),
            tracker: self.tracker.clone(),
        }) {
            Err(anyhow!(e))
        } else {
            Ok(())
        }
    }

    async fn update_task_state(&self, task_id: TaskID, state: State) -> Result<()> {
        let (response, rx) = oneshot::channel();
        self.tracker.send(TrackerMessage::UpdateTaskState {
            run_id: self.run_id,
            task_id,
            state,
            response,
        })?;
        rx.await?
    }

    /// Enqueues as many tasks as possible, returns end state
    pub async fn run(&mut self) -> Result<State> {
        if !(self.state == State::Queued || self.state == State::Running) {
            return Ok(self.state);
        }
        if !self.dag.can_progress() {
            self.state = if self.dag.is_complete() {
                State::Completed
            } else {
                State::Errored
            };

            let (response, rx) = oneshot::channel();
            self.tracker
                .send(TrackerMessage::UpdateState {
                    run_id: self.run_id,
                    state: self.state,
                    response,
                })
                .unwrap_or(());
            rx.await.unwrap().unwrap_or(());
            return Ok(self.state);
        }

        // Enqueue as many tasks as possible
        while let Some(task_id) = self.dag.visit_next() {
            let task = self.tasks.get(&task_id).unwrap();
            match task.task_type {
                TaskType::Normal => {
                    self.submit_task(task_id.clone(), task.details.clone())?;
                }
                TaskType::Structural => {
                    let mut attempt = TaskAttempt::new();
                    attempt.succeeded = true;
                    self.runner
                        .send(RunnerMessage::ExecutionReport {
                            run_id: self.run_id,
                            task_id,
                            attempt,
                        })
                        .unwrap_or(());
                }
            }
        }
        Ok(self.state)
    }

    pub async fn handle_generator(&mut self, task_id: TaskID, attempt: &TaskAttempt) -> Result<()> {
        let tasks = serde_json::from_str::<TaskSet>(&attempt.output)?;

        let mut exp_tasks = self.expand_tasks(tasks).await?;
        let gen_task = self.tasks.get(&task_id).unwrap();
        let children = gen_task.children.clone();
        let parents = vec![task_id.clone()];
        for task in exp_tasks.values_mut() {
            task.children = children.clone();
            task.parents = parents.clone();
        }

        // Set the parent and children for each task
        self.add_tasks(&exp_tasks)?;

        // Let the tracker know
        let (response, rx) = oneshot::channel();
        self.tracker
            .send(TrackerMessage::AddTasks {
                run_id: self.run_id,
                tasks: exp_tasks,
                response,
            })
            .unwrap();
        rx.await??;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        for vertex in &self.dag.vertices {
            if vertex.state == State::Running {
                let (response, cancel_rx) = oneshot::channel();
                self.executor
                    .send(ExecutorMessage::StopTask {
                        run_id: self.run_id,
                        task_id: vertex.id.clone(),
                        response,
                    })
                    .unwrap();
                cancel_rx.await.unwrap_or(());
                self.update_task_state(vertex.id.clone(), State::Killed)
                    .await?;
            }
        }
        let (response, rx) = oneshot::channel();
        self.tracker
            .send(TrackerMessage::UpdateState {
                run_id: self.run_id,
                state: State::Killed,
                response,
            })
            .unwrap();
        rx.await??;
        Ok(())
    }

    async fn handle_killed_task(&mut self, task_id: TaskID) -> Result<()> {
        if self.dag.get_vertex(&task_id).unwrap().state == State::Killed {
            return Ok(());
        }

        self.dag.set_vertex_state(&task_id, State::Killed).unwrap();
        self.update_task_state(task_id.clone(), State::Killed)
            .await?;
        Ok(())
    }

    async fn complete_task(&mut self, task_id: &TaskID, attempt: TaskAttempt) -> Result<()> {
        let (response, rx) = oneshot::channel();
        self.tracker
            .send(TrackerMessage::LogTaskAttempt {
                run_id: self.run_id,
                task_id: task_id.clone(),
                attempt: attempt.clone(),
                response,
            })
            .unwrap();
        rx.await??;

        let mut new_state = if attempt.succeeded {
            State::Completed
        } else if attempt.killed {
            State::Killed
        } else {
            State::Errored
        };

        if new_state == State::Completed && self.tasks[task_id].is_generator {
            if let Err(e) = self.handle_generator(task_id.clone(), &attempt).await {
                new_state = State::Errored;
                let mut generator_attempt = TaskAttempt::new();
                generator_attempt.executor.push(format!("{:?}", e));
                let (response, rx) = oneshot::channel();
                self.tracker
                    .send(TrackerMessage::LogTaskAttempt {
                        run_id: self.run_id,
                        task_id: task_id.clone(),
                        attempt: generator_attempt,
                        response,
                    })
                    .unwrap();
                rx.await??;
            }
        }

        // Update the state
        self.update_task_state(task_id.clone(), new_state).await?;

        // TODO implement retry logic here
        let task = self.tasks.get(task_id).unwrap().clone();
        if new_state == State::Errored && task.retries < task.max_retries {
            self.tasks.get_mut(task_id).unwrap().retries += 1;
            self.submit_task(task_id.clone(), task.details.clone())?;
        } else {
            self.dag
                .complete_visit(task_id, new_state != State::Completed)?;
        }
        Ok(())
    }
}

pub fn start(
    msg_tx: mpsc::UnboundedSender<RunnerMessage>,
    msg_rx: mpsc::UnboundedReceiver<RunnerMessage>,
) {
    tokio::spawn(async move {
        start_dag_runner(msg_tx, msg_rx).await;
    });
}

async fn start_dag_runner(
    msg_tx: mpsc::UnboundedSender<RunnerMessage>,
    mut msg_rx: mpsc::UnboundedReceiver<RunnerMessage>,
) {
    let mut runs = HashMap::<RunID, Run>::new();

    while let Some(msg) = msg_rx.recv().await {
        use RunnerMessage::{ExecutionReport, Retry, Start, Stop, StopRun};
        match msg {
            Start {
                tags,
                tasks,
                response,
                parameters,
                tracker,
                executor,
            } => {
                // Queue all pending tasks
                let result = match Run::new(
                    tags,
                    tasks,
                    parameters,
                    tracker,
                    executor,
                    msg_tx.clone(),
                )
                .await
                {
                    Ok(mut run) => {
                        let run_id = run.run_id;

                        // Update the state of the run
                        match run.run().await {
                            Ok(State::Running) => {
                                runs.insert(run_id, run);
                                Ok(run_id)
                            }
                            Ok(state) => Err(anyhow!("Run in state {:?} after enqueuing", state)),
                            Err(e) => Err(anyhow!("Error enqueing run: {:?}", e)),
                        }
                    }
                    Err(e) => Err(e),
                };
                response.send(result).unwrap_or(());
            }
            StopRun { run_id, response } => match runs.get_mut(&run_id) {
                Some(run) => {
                    run.stop().await.unwrap_or(());
                    runs.remove(&run_id);
                    response.send(()).unwrap_or(());
                }
                None => {
                    response.send(()).unwrap_or(());
                }
            },
            Retry {
                run_id,
                tracker,
                executor,
                response,
            } => {
                //if let std::collections::hash_map::Entry::Vacant(e) = runs.entry(run_id) {
                use std::collections::hash_map::Entry::{Occupied, Vacant};
                let result = match runs.entry(run_id) {
                    Occupied(_) => Err(anyhow!("Run ID is currently running, cannot retry.")),
                    Vacant(e) => {
                        match Run::from_tracker(run_id, tracker, executor, msg_tx.clone()).await {
                            Ok(mut run) => match run.run().await {
                                Ok(State::Running) => {
                                    e.insert(run);
                                    Ok(())
                                }
                                Ok(state) => {
                                    Err(anyhow!("Run in state {:?} after enqueuing", state))
                                }
                                Err(e) => Err(anyhow!("Error enqueing run: {:?}", e)),
                            },
                            Err(e) => Err(e),
                        }
                    }
                };
                response.send(result).unwrap_or(());
            }
            ExecutionReport {
                run_id,
                task_id,
                attempt,
            } => {
                if let Some(run) = runs.get_mut(&run_id) {
                    run.complete_task(&task_id, attempt).await.unwrap_or(());
                    // TODO NOT SURE ABOUT THIS
                    if let Ok(state) = run.run().await {
                        if state != State::Running {
                            runs.remove(&run_id);
                        }
                    }
                }
            }
            Stop {} => {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executors::local_executor;
    use crate::trackers::memory_tracker;

    async fn run(
        tasks: &TaskSet,
        parameters: &Parameters,
    ) -> (RunID, mpsc::UnboundedSender<TrackerMessage>) {
        let (log_tx, log_rx) = mpsc::unbounded_channel();
        memory_tracker::start(log_rx);

        let (exe_tx, exe_rx) = mpsc::unbounded_channel();
        local_executor::start(10, exe_rx);

        let (run_tx, run_rx) = mpsc::unbounded_channel();
        let rtx = run_tx.clone();
        super::start(rtx, run_rx);

        let (tx, rx) = oneshot::channel();
        run_tx
            .send(RunnerMessage::Start {
                tags: RunTags::new(),
                tasks: tasks.clone(),
                response: tx,
                parameters: parameters.clone(),
                tracker: log_tx.clone(),
                executor: exe_tx.clone(),
            })
            .unwrap();

        let run_id = rx.await.unwrap().unwrap();

        // Need some way to get
        loop {
            let (tx, rx) = oneshot::channel();

            log_tx
                .send(TrackerMessage::GetState {
                    run_id,
                    response: tx,
                })
                .unwrap();
            let state_change = rx.await.unwrap().unwrap();

            if state_change.state == State::Completed || state_change.state == State::Errored {
                break;
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Close off everything except the tracker
        exe_tx.send(ExecutorMessage::Stop {}).unwrap();
        run_tx.send(RunnerMessage::Stop {}).unwrap();

        (run_id, log_tx)
    }

    #[tokio::test]
    async fn test_simple_dag_run() {
        let tasks: TaskSet = serde_json::from_str(
            r#"
            {
                "simple_task": {
                    "details": {
                        "command": [ "/bin/echo", "hello", "world" ],
                        "env": {
                            "ENVVAR": 16
                        }
                    },
                    "children": [ "other_task" ]
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

        let parameters = HashMap::new();

        let (run_id, log_tx) = run(&tasks, &parameters).await;

        for (task_id, task) in tasks {
            let (tx, rx) = oneshot::channel();
            log_tx
                .send(TrackerMessage::GetTask {
                    run_id,
                    task_id: task_id.clone(),
                    response: tx,
                })
                .unwrap();

            let task_record = rx.await.unwrap().unwrap();
            assert_eq!(task, task_record.task);
            assert_eq!(task_record.attempts.len(), 1);
            assert_eq!(
                task_record.state_changes.last().unwrap().state,
                State::Completed
            );
        }

        // Close off tracker
        log_tx.send(TrackerMessage::Stop {}).unwrap();
    }

    #[tokio::test]
    async fn test_dag_with_expansion() {
        let tasks: TaskSet = serde_json::from_str(
            r#"
            {
                "A": {
                    "details": {
                        "command": [ "/bin/echo", "A" ]
                    },
                    "children": [ "B" ]
                },
                "B": {
                    "details": {
                        "command": [ "/bin/echo", "B", "DATE" ]
                    },
                    "children": [ "C" ]
                },
                "C": {
                    "details": {
                        "command": [ "/bin/echo", "NAME" ]
                    },
                    "parameters": {
                        "NAME": [ "ABCD", "DEFG" ]
                    }
                }
            }"#,
        )
        .unwrap();

        let parameters: Parameters = serde_json::from_str(
            r#"{
                "DATE": [ "20200101", "20200102", "20200103" ]
                }"#,
        )
        .unwrap();

        let (run_id, log_tx) = run(&tasks, &parameters).await;

        let (tx, rx) = oneshot::channel();
        log_tx
            .send(TrackerMessage::GetRun {
                run_id,
                response: tx,
            })
            .unwrap();

        let rec = rx.await.unwrap().unwrap();
        assert_eq!(rec.parameters, parameters);
        assert_eq!(
            rec.tasks.len(),
            1 + parameters["DATE"].len() + 2 + tasks["C"].parameters["NAME"].len() + 2
        ); // 5 actual tasks, 2 control tasks
        assert_eq!(rec.state_changes.last().unwrap().state, State::Completed);

        // Close off tracker
        log_tx.send(TrackerMessage::Stop {}).unwrap();
    }

    #[tokio::test]
    async fn test_failing_generating_dag_run() {
        let tasks: TaskSet = serde_json::from_str(
            r#"{
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

        let parameters = HashMap::new();

        let (run_id, log_tx) = run(&tasks, &parameters).await;

        for (task_id, task) in tasks {
            let (tx, rx) = oneshot::channel();
            log_tx
                .send(TrackerMessage::GetTask {
                    run_id,
                    task_id: task_id.clone(),
                    response: tx,
                })
                .unwrap();

            let task_record = rx.await.unwrap().unwrap();
            assert_eq!(task, task_record.task);

            match task_id.as_ref() {
                "simple_task" => {
                    assert_eq!(task_record.state_changes.len(), 3);
                    assert_eq!(
                        task_record.state_changes.last().unwrap().state,
                        State::Errored
                    );
                }
                "other_task" => {
                    assert_eq!(task_record.state_changes.len(), 1);
                    assert_eq!(
                        task_record.state_changes.last().unwrap().state,
                        State::Queued
                    );
                }
                _ => {}
            }
        }

        log_tx.send(TrackerMessage::Stop {}).unwrap();
    }

    #[tokio::test]
    async fn test_successful_generating_dag_run() {
        use serde_json::json;

        let mut tasks: TaskSet = serde_json::from_str(
            r#"{
                "other_task": {
                    "details": {
                        "command": [ "/bin/echo", "task", "$ENVVAR" ]
                    }
                }
            }"#,
        )
        .unwrap();

        let new_task = r#"{
            "generated_task": {
                "details": {
                    "command": [ "/bin/echo", "hello", "world" ]
                }
            }
        }"#;

        let parameters = HashMap::new();

        let task_id = "fancy_generator".to_owned();

        let mut gen_task = Task::new();
        gen_task.is_generator = true;
        gen_task.children.push("other_task".to_owned());
        gen_task.details = json!({
            "command": [ "/bin/echo", new_task ]
        });

        tasks.insert(task_id, gen_task);

        // This will run the tasks and process the generators
        let (run_id, log_tx) = run(&tasks, &parameters).await;

        // Make tasks match the expected output
        let mut new_tasks: TaskSet = serde_json::from_str(new_task).unwrap();
        for (task_id, task) in &mut new_tasks {
            task.children.push("other_task".to_owned());
            task.parents.push("fancy_generator".to_owned());
            tasks.insert(task_id.clone(), task.clone());
        }
        assert_eq!(tasks.len(), 3);

        for (task_id, task) in tasks {
            let (tx, rx) = oneshot::channel();
            log_tx
                .send(TrackerMessage::GetTask {
                    run_id,
                    task_id,
                    response: tx,
                })
                .unwrap();

            let task_record = rx.await.unwrap().unwrap();
            assert_eq!(task, task_record.task);

            assert_eq!(task_record.state_changes.len(), 3);
            assert_eq!(
                task_record.state_changes.last().unwrap().state,
                State::Completed
            );
        }

        // Close off everything
        log_tx.send(TrackerMessage::Stop {}).unwrap();
    }

    #[tokio::test]
    async fn test_task_retries() {
        use serde_json::json;
        use std::fs;
        use std::io::prelude::*;
        use std::path::Path;
        use State::{Completed, Errored, Queued, Running};

        // Write out a shell script that will fail first, then succeed
        let script_file = Path::new("./runner_retry_test.sh");
        let test_file = Path::new("./runner_retry_test");
        let script = r#"#!/usr/bin/env bash
        export TEST_FILE="$1"
        if [[ -e "$TEST_FILE" ]]; then
            exit 0
        else
            /usr/bin/touch "$TEST_FILE"
            exit 1
        fi
        "#;

        if test_file.exists() {
            fs::remove_file(test_file).unwrap();
        }

        // In its own scope to force the sync/close
        {
            let mut script_fh = fs::File::create(script_file).unwrap();
            script_fh.write_all(script.as_bytes()).unwrap();
        }

        let mut tasks = TaskSet::new();

        let mut retry_task = Task::new();
        retry_task.details = json!({
            "command": [ "/bin/bash", script_file, test_file ]
        });
        retry_task.max_retries = 3;
        tasks.insert("retry_task".to_owned(), retry_task);

        let parameters = HashMap::new();

        let (run_id, log_tx) = run(&tasks, &parameters).await;

        for (task_id, task) in tasks {
            let (tx, rx) = oneshot::channel();
            log_tx
                .send(TrackerMessage::GetTask {
                    run_id,
                    task_id: task_id.clone(),
                    response: tx,
                })
                .unwrap();

            let task_record = rx.await.unwrap().unwrap();
            assert_eq!(task, task_record.task);
            assert_eq!(task_record.attempts.len(), 2);
            let states: Vec<State> = task_record.state_changes.iter().map(|x| x.state).collect();
            assert_eq!(states, vec![Queued, Running, Errored, Running, Completed]);
        }

        // Close off tracker
        log_tx.send(TrackerMessage::Stop {}).unwrap();
        fs::remove_file(script_file).unwrap();
        fs::remove_file(test_file).unwrap();
    }
}
