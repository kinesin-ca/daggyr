use super::*;
use crate::dag::DAG;
use crate::messages::*;
use crate::structs::*;
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};

/// A Run comprises all of the runtime information for an
/// executing task DAG.
struct Run {
    run_id: RunID,
    tasks: TaskSet,
    dag: DAG<TaskID>,
    state: State,
    name_tasks: HashMap<String, HashSet<TaskID>>,
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
            name_tasks: HashMap::new(),
            parameters,
            tracker: tracker.clone(),
            executor,
            runner,
        };

        // Expand the tasks
        let (tx, rx) = oneshot::channel();
        run.executor
            .send(ExecutorMessage::ExpandTasks {
                tasks,
                parameters: run.parameters.clone(),
                response: tx,
            })
            .unwrap();
        let exp_tasks = rx.await??;

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
        for (mut task_id, task) in exp_tasks {
            task_id.set_run_id(run.run_id);
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
            run_id: run_id,
            tasks: TaskSet::new(),
            dag: DAG::new(),
            state: State::Running,
            name_tasks: HashMap::new(),
            parameters: run_record.parameters,
            tracker,
            executor,
            runner,
        };

        run.add_tasks(&tasks)?;

        // Update the task states
        let mut responses = Vec::new();
        for (task_id, state) in states {
            run.dag.set_vertex_state(task_id.clone(), state)?;
            let (response, rx) = oneshot::channel();
            run.tracker
                .send(TrackerMessage::UpdateTaskState {
                    task_id,
                    state,
                    response,
                })
                .unwrap();
            responses.push(rx);
        }
        for rx in responses {
            rx.await??
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

        // Figure out the name maps
        for task_id in tasks.keys() {
            match self.name_tasks.get_mut(&task_id.name().to_owned()) {
                Some(tids) => {
                    tids.insert(task_id.clone());
                }
                None => {
                    let mut tids = HashSet::new();
                    tids.insert(task_id.clone());
                    self.name_tasks.insert(task_id.name().to_owned(), tids);
                }
            }
        }

        // Insert edges
        for (task_id, task) in tasks.iter() {
            for child in task.children.iter() {
                match self.name_tasks.get(child) {
                    Some(dests) => {
                        for tid in dests {
                            self.dag.add_edge(task_id, tid)?;
                        }
                    }
                    None => {
                        return Err(anyhow!(
                            "Task {} has child {} which does not exist in the DAG",
                            task_id.name(),
                            child
                        ));
                    }
                }
            }

            // Same for parents, but in the other direction
            for parent in task.parents.iter() {
                match self.name_tasks.get(parent) {
                    Some(srcs) => {
                        for tid in srcs {
                            self.dag.add_edge(tid, task_id)?;
                        }
                    }
                    None => {
                        return Err(anyhow!(
                            "Task {} has parent {} which does not exist in the DAG",
                            task_id.name(),
                            parent
                        ));
                    }
                }
            }
        }

        self.tasks.extend(tasks.clone());

        Ok(())
    }

    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Enqueues as many tasks as possible, returns end state
    pub async fn run(&mut self) -> State {
        if !(self.state == State::Queued || self.state == State::Running) {
            return self.state;
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
            return self.state;
        }

        // Enqueue as many tasks as possible
        loop {
            if let Some(task_id) = self.dag.visit_next() {
                // Start the executor
                self.executor
                    .send(ExecutorMessage::ExecuteTask {
                        task_id: task_id.clone(),
                        task: self.tasks.get(&task_id).unwrap().clone(),
                        response: self.runner.clone(),
                        tracker: self.tracker.clone(),
                    })
                    .unwrap_or(());
                // TODO this should probably be an error
            } else {
                break;
            }
        }
        self.state
    }

    pub async fn handle_generator(&mut self, task_id: TaskID, attempt: &TaskAttempt) -> Result<()> {
        let mut tasks = TaskSet::new();
        match serde_json::from_str::<TaskSetSpec>(&attempt.output) {
            Ok(result) => {
                for (name, task) in result.iter() {
                    let new_id = TaskID::new(task_id.run_id(), name, 0);
                    tasks.insert(new_id, task.clone());
                }
            }
            Err(e) => {
                return Err(anyhow!("Unable to parse generator output: {}", e));
            }
        }

        let (tx, rx) = oneshot::channel();
        self.executor
            .send(ExecutorMessage::ExpandTasks {
                tasks,
                parameters: self.parameters.clone(),
                response: tx,
            })
            .unwrap();
        let mut exp_tasks = rx.await??;
        let gen_task = self.tasks.get(&task_id).unwrap();
        let children = gen_task.children.clone();
        let parents = vec![task_id.name().to_owned()];
        for (_, task) in exp_tasks.iter_mut() {
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
        for vertex in self.dag.vertices.iter() {
            if vertex.state == State::Running {
                let (response, cancel_rx) = oneshot::channel();
                self.executor
                    .send(ExecutorMessage::StopTask {
                        task_id: vertex.id.clone(),
                        response,
                    })
                    .unwrap();
                cancel_rx.await.unwrap_or(());
                let (response, rx) = oneshot::channel();
                self.tracker
                    .send(TrackerMessage::UpdateTaskState {
                        task_id: vertex.id.clone(),
                        state: State::Killed,
                        response,
                    })
                    .unwrap();
                rx.await??;
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

        self.dag
            .set_vertex_state(task_id.clone(), State::Killed)
            .unwrap();
        let (response, rx) = oneshot::channel();
        self.tracker
            .send(TrackerMessage::UpdateTaskState {
                task_id: task_id.clone(),
                state: State::Killed,
                response,
            })
            .unwrap();
        rx.await??;
        Ok(())
    }

    async fn complete_task(&mut self, task_id: &TaskID, attempt: TaskAttempt) -> Result<()> {
        let (response, rx) = oneshot::channel();
        self.tracker
            .send(TrackerMessage::LogTaskAttempt {
                task_id: task_id.clone(),
                attempt: attempt.clone(),
                response,
            })
            .unwrap();
        rx.await??;

        let mut new_state = if attempt.succeeded {
            State::Completed
        } else {
            if attempt.killed {
                State::Killed
            } else {
                State::Errored
            }
        };

        if new_state == State::Completed && self.tasks[&task_id].is_generator {
            if let Err(e) = self.handle_generator(task_id.clone(), &attempt).await {
                new_state = State::Errored;
                let mut generator_attempt = TaskAttempt::new();
                generator_attempt.executor.push(format!("{:?}", e));
                let (response, rx) = oneshot::channel();
                self.tracker
                    .send(TrackerMessage::LogTaskAttempt {
                        task_id: task_id.clone(),
                        attempt: generator_attempt,
                        response,
                    })
                    .unwrap();
                rx.await??;
            }
        }

        // Update the state
        let (response, rx) = oneshot::channel();
        self.tracker
            .send(TrackerMessage::UpdateTaskState {
                task_id: task_id.clone(),
                state: new_state,
                response,
            })
            .unwrap();
        rx.await??;

        self.dag
            .complete_visit(&task_id, new_state != State::Completed)?;
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
        use RunnerMessage::*;
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
                match Run::new(tags, tasks, parameters, tracker, executor, msg_tx.clone()).await {
                    Ok(mut run) => {
                        let run_id = run.run_id;

                        // Update the state of the run
                        if run.run().await == State::Running {
                            runs.insert(run_id, run);
                        }
                        response.send(Ok(run_id)).unwrap_or(());
                    }
                    Err(e) => {
                        response.send(Err(e)).unwrap_or(());
                    }
                }
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
                if runs.contains_key(&run_id) {
                    response
                        .send(Err(anyhow!("Run ID is currently running, cannot retry.")))
                        .unwrap_or(());
                } else {
                    match Run::from_tracker(run_id, tracker, executor, msg_tx.clone()).await {
                        Ok(mut run) => {
                            if run.run().await == State::Running {
                                runs.insert(run_id, run);
                            }
                            response.send(Ok(())).unwrap_or(());
                        }
                        Err(e) => {
                            response.send(Err(e)).unwrap_or(());
                        }
                    }
                }
            }
            ExecutionReport { task_id, attempt } => {
                let run_id = task_id.run_id();
                if let Some(run) = runs.get_mut(&run_id) {
                    run.complete_task(&task_id, attempt).await.unwrap_or(());
                    if run.run().await != State::Running {
                        runs.remove(&run_id);
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
    async fn test_simple_dag_run() -> () {
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

        let tasks = tasks_spec.to_task_set(0);

        let parameters = HashMap::new();

        let (run_id, log_tx) = run(&tasks, &parameters).await;

        for (mut task_id, task) in tasks {
            task_id.set_run_id(run_id);
            let (tx, rx) = oneshot::channel();
            log_tx
                .send(TrackerMessage::GetTask {
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
    async fn test_failing_generating_dag_run() -> () {
        let tasks_spec: TaskSetSpec = serde_json::from_str(
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

        let tasks = tasks_spec.to_task_set(0);

        let parameters = HashMap::new();

        let (run_id, log_tx) = run(&tasks, &parameters).await;

        for (mut task_id, task) in tasks {
            task_id.set_run_id(run_id);
            let (tx, rx) = oneshot::channel();
            log_tx
                .send(TrackerMessage::GetTask {
                    task_id: task_id.clone(),
                    response: tx,
                })
                .unwrap();

            let task_record = rx.await.unwrap().unwrap();
            assert_eq!(task, task_record.task);

            match task_id.name() {
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
    async fn test_successful_generating_dag_run() -> () {
        use serde_json::json;

        let tasks_spec: TaskSetSpec = serde_json::from_str(
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

        let mut tasks = tasks_spec.to_task_set(0);
        let parameters = HashMap::new();

        let task_id = TaskID::new(0, &"fancy_generator".to_owned(), 0);

        let mut gen_task = Task::new();
        gen_task.is_generator = true;
        gen_task.children.push("other_task".to_owned());
        gen_task.details = json!({
            "command": [ "/bin/echo", new_task ]
        });

        tasks.insert(task_id, gen_task);

        let (run_id, log_tx) = run(&tasks, &parameters).await;

        let new_tasks_spec: TaskSetSpec = serde_json::from_str(new_task).unwrap();
        let mut new_tasks = new_tasks_spec.to_task_set(0);
        for (task_id, task) in new_tasks.iter_mut() {
            task.children.push("other_task".to_owned());
            task.parents.push("fancy_generator".to_owned());
            let new_id = TaskID::new(run_id, &task_id.name().to_owned(), 0);
            tasks.insert(new_id, task.clone());
        }

        assert_eq!(tasks.len(), 3);

        for (mut task_id, task) in tasks {
            task_id.set_run_id(run_id);
            let (tx, rx) = oneshot::channel();
            log_tx
                .send(TrackerMessage::GetTask {
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
}
