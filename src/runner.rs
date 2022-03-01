use super::*;
use crate::dag::DAG;
use crate::messages::*;
use crate::structs::{Parameters, RunID, State, Tags, Task, TaskAttempt, TaskID};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, oneshot};

/// A Run comprises all of the runtime information for an
/// executing task DAG.
struct Run {
    run_id: RunID,
    tasks: Vec<Task>,
    dag: DAG,
    state: State,
    class_tasks: HashMap<String, HashSet<usize>>,
    parameters: Parameters,
    logger: mpsc::Sender<LoggerMessage>,
    executor: mpsc::Sender<ExecutorMessage>,
    runner: mpsc::Sender<RunnerMessage>,
}

impl Run {
    async fn new(
        tags: Tags,
        tasks: Vec<Task>,
        parameters: Parameters,
        logger: mpsc::Sender<LoggerMessage>,
        executor: mpsc::Sender<ExecutorMessage>,
        runner: mpsc::Sender<RunnerMessage>,
    ) -> Result<Self> {
        let mut run = Run {
            run_id: 0,
            tasks: Vec::new(),
            dag: DAG::new(),
            state: State::Queued,
            class_tasks: HashMap::new(),
            parameters,
            logger: logger.clone(),
            executor,
            runner,
        };

        // Expand the tasks
        let (tx, rx) = oneshot::channel();
        run.executor
            .send(ExecutorMessage::ExpandTasks {
                tasks: tasks,
                parameters: run.parameters.clone(),
                response: tx,
            })
            .await?;
        let tasks = rx.await??;

        run.add_tasks(&tasks).await?;

        // Create the run ID and update the logger
        let (tx, rx) = oneshot::channel();
        logger
            .send(LoggerMessage::CreateRun {
                tags,
                parameters: run.parameters.clone(),
                response: tx,
            })
            .await?;
        run.run_id = rx.await??;

        // Let the logger know
        run.logger
            .send(LoggerMessage::AddTasks {
                run_id: run.run_id,
                tasks: tasks,
                offset: 0,
            })
            .await?;

        run.update_state(State::Running).await?;

        Ok(run)
    }

    /// Retrieve an existing run from the logger and reset it
    /// to get it ready to run
    async fn from_logger(
        run_id: RunID,
        logger: mpsc::Sender<LoggerMessage>,
        executor: mpsc::Sender<ExecutorMessage>,
        runner: mpsc::Sender<RunnerMessage>,
    ) -> Result<Self> {
        let (tx, rx) = oneshot::channel();
        logger
            .send(LoggerMessage::GetRun {
                run_id,
                response: tx,
            })
            .await?;
        let run_record = rx.await??;

        // Build the set of tasks and states
        let mut tasks = Vec::new();

        // States for previously run tasks are reset to queued
        let mut states = Vec::new();

        for tr in run_record.tasks {
            tasks.push(tr.task);
            let new_state = match tr.state_changes.last() {
                Some(change) => match change.state {
                    State::Completed => State::Completed,
                    _ => State::Queued,
                },
                None => State::Queued,
            };

            states.push(new_state);
        }

        // Build the run
        let mut run = Run {
            run_id: run_id,
            tasks: Vec::new(),
            dag: DAG::new(),
            state: State::Running,
            class_tasks: HashMap::new(),
            parameters: run_record.parameters,
            logger,
            executor,
            runner,
        };

        run.add_tasks(&tasks).await?;

        // Update the task states
        for (task_id, state) in states.iter().enumerate() {
            run.dag.set_vertex_state(task_id, *state)?;
            run.logger
                .send(LoggerMessage::UpdateTaskState {
                    run_id: run.run_id,
                    task_id,
                    state: *state,
                })
                .await?
        }

        run.update_state(State::Running).await?;
        Ok(run)
    }

    async fn update_state(&mut self, state: State) -> Result<()> {
        self.logger
            .send(LoggerMessage::UpdateState {
                run_id: self.run_id,
                state: state,
            })
            .await?;
        self.state = state;
        Ok(())
    }

    /// Adds the tasks and sets up the DAG
    async fn add_tasks(&mut self, tasks: &Vec<Task>) -> Result<()> {
        let offset = self.tasks.len();

        // Add vertices
        self.dag.add_vertices(tasks.len());

        // Figure out the class maps
        for (i, task) in tasks.iter().enumerate() {
            let tid = i + offset;
            match self.class_tasks.get_mut(&task.class) {
                Some(tids) => {
                    tids.insert(tid);
                }
                None => {
                    let mut tids = HashSet::new();
                    tids.insert(tid);
                    self.class_tasks.insert(task.class.clone(), tids);
                }
            }
        }

        // Insert edges
        for (i, task) in tasks.iter().enumerate() {
            let src = i + offset;
            for child in task.children.iter() {
                match self.class_tasks.get(child) {
                    Some(dests) => {
                        for tid in dests {
                            self.dag.add_edge(src, *tid)?;
                        }
                    }
                    None => {
                        return Err(anyhow!(
                            "Task {} has child {} which does not exist in the DAG",
                            task.class,
                            child
                        ));
                    }
                }
            }

            // Same for parents, but in the other direction
            for parent in task.parents.iter() {
                match self.class_tasks.get(parent) {
                    Some(srcs) => {
                        for tid in srcs {
                            self.dag.add_edge(*tid, src)?;
                        }
                    }
                    None => {
                        return Err(anyhow!(
                            "Task {} has parent {} which does not exist in the DAG",
                            task.class,
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

            self.logger
                .send(LoggerMessage::UpdateState {
                    run_id: self.run_id,
                    state: self.state,
                })
                .await
                .unwrap_or(());
            return self.state;
        }

        // Enqueue as many tasks as possible
        loop {
            match self.dag.visit_next() {
                Some(task_id) => {
                    // Start the executor
                    self.executor
                        .send(ExecutorMessage::ExecuteTask {
                            run_id: self.run_id,
                            task_id: task_id,
                            task: self.tasks[task_id].clone(),
                            response: self.runner.clone(),
                            // logger: self.logger.clone(),
                        })
                        .await
                        .unwrap_or(());
                    // TODO this should probably be an error
                }
                None => break,
            }
        }
        self.state
    }

    pub async fn handle_generator(&mut self, task_id: usize, attempt: &TaskAttempt) -> Result<()> {
        let tasks;
        match serde_json::from_str::<Vec<Task>>(&attempt.output) {
            Ok(result) => {
                tasks = result;
            }
            Err(e) => {
                return Err(anyhow!("Unable to parse generator output: {}", e));
            }
        }

        let (tx, rx) = oneshot::channel();
        self.executor
            .send(ExecutorMessage::ExpandTasks {
                tasks: tasks,
                parameters: self.parameters.clone(),
                response: tx,
            })
            .await?;
        let mut exp_tasks = rx.await??;
        let children = self.tasks[task_id].children.clone();
        let parents = vec![self.tasks[task_id].class.clone()];
        for task in exp_tasks.iter_mut() {
            task.children = children.clone();
            task.parents = parents.clone();
        }

        // Set the parent and children for each task
        let offset = self.tasks.len();
        self.add_tasks(&exp_tasks).await?;

        // Let the logger know
        self.logger
            .send(LoggerMessage::AddTasks {
                run_id: self.run_id,
                tasks: exp_tasks,
                offset: offset,
            })
            .await?;
        Ok(())
    }

    async fn stop(&mut self) -> Result<()> {
        for (task_id, vertex) in self.dag.vertices.iter().enumerate() {
            if vertex.state == State::Running {
                self.executor
                    .send(ExecutorMessage::StopTask {
                        run_id: self.run_id,
                        task_id,
                    })
                    .await?;
                self.logger
                    .send(LoggerMessage::UpdateTaskState {
                        run_id: self.run_id,
                        task_id: task_id,
                        state: State::Killed,
                    })
                    .await?;
            }
        }
        self.logger
            .send(LoggerMessage::UpdateState {
                run_id: self.run_id,
                state: State::Killed,
            })
            .await?;
        Ok(())
    }

    async fn handle_killed_task(&mut self, task_id: TaskID) -> Result<()> {
        if self.dag.vertices[task_id].state == State::Killed {
            return Ok(());
        }

        self.dag.set_vertex_state(task_id, State::Killed).unwrap();
        self.logger
            .send(LoggerMessage::UpdateTaskState {
                run_id: self.run_id,
                task_id: task_id,
                state: State::Killed,
            })
            .await?;
        Ok(())
    }

    async fn complete_task(&mut self, task_id: TaskID, attempt: TaskAttempt) -> Result<()> {
        self.logger
            .send(LoggerMessage::LogTaskAttempt {
                run_id: self.run_id,
                task_id,
                attempt: attempt.clone(),
            })
            .await?;

        let mut new_state = if attempt.succeeded {
            State::Completed
        } else {
            if attempt.killed {
                State::Killed
            } else {
                State::Errored
            }
        };

        if new_state == State::Completed && self.tasks[task_id].is_generator {
            if let Err(e) = self.handle_generator(task_id, &attempt).await {
                new_state = State::Errored;
                let mut generator_attempt = TaskAttempt::new();
                generator_attempt.executor.push(format!("{:?}", e));
                self.logger
                    .send(LoggerMessage::LogTaskAttempt {
                        run_id: self.run_id,
                        task_id: task_id,
                        attempt: generator_attempt,
                    })
                    .await?;
            }
        }

        // Update the state
        self.logger
            .send(LoggerMessage::UpdateTaskState {
                run_id: self.run_id,
                task_id: task_id,
                state: new_state,
            })
            .await?;

        self.dag
            .complete_visit(task_id, new_state != State::Completed);
        Ok(())
    }
}

pub async fn start_dag_runner(
    msg_tx: mpsc::Sender<RunnerMessage>,
    mut msg_rx: mpsc::Receiver<RunnerMessage>,
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
                logger,
                executor,
            } => {
                // Queue all pending tasks
                match Run::new(tags, tasks, parameters, logger, executor, msg_tx.clone()).await {
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
                logger,
                executor,
                response,
            } => {
                if runs.contains_key(&run_id) {
                    response
                        .send(Err(anyhow!("Run ID is currently running, cannot retry.")))
                        .unwrap_or(());
                } else {
                    match Run::from_logger(run_id, logger, executor, msg_tx.clone()).await {
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
            ExecutionReport {
                run_id,
                task_id,
                attempt,
            } => {
                if let Some(run) = runs.get_mut(&run_id) {
                    run.complete_task(task_id, attempt).await.unwrap_or(());
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
