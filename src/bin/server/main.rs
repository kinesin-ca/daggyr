use actix_cors::Cors;
use actix_web::{error, middleware::Logger, web, App, HttpResponse, HttpServer, Responder};
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

use daggyr_async::prelude::*;
use tokio::sync::{mpsc, oneshot};

#[derive(Serialize)]
struct SimpleError {
    error: String,
}

#[derive(Serialize)]
struct RunIDResponse {
    run_id: RunID,
}

#[derive(Clone, Deserialize)]
struct RunSpec {
    tags: Tags,
    tasks: Vec<Task>,
    parameters: Parameters,
}

fn min_datetime() -> DateTime<Utc> {
    chrono::MIN_DATETIME
}

fn max_datetime() -> DateTime<Utc> {
    chrono::MAX_DATETIME
}

#[derive(Clone, Deserialize)]
struct RunsSelection {
    #[serde(default)]
    tags: Tags,

    #[serde(default)]
    states: HashSet<State>,

    #[serde(default = "min_datetime")]
    start_time: DateTime<Utc>,

    #[serde(default = "max_datetime")]
    end_time: DateTime<Utc>,
}

async fn get_runs(
    criteria: web::Query<RunsSelection>,
    data: web::Data<AppState>,
) -> impl Responder {
    let (response, rx) = oneshot::channel();

    data.log_tx
        .send(LoggerMessage::GetRuns {
            tags: criteria.tags.clone(),
            states: criteria.states.clone(),
            start_time: criteria.start_time,
            end_time: criteria.end_time,
            response,
        })
        .unwrap();

    HttpResponse::Ok().json(rx.await.unwrap_or(Vec::new()))
}

async fn submit_run(spec: web::Json<RunSpec>, data: web::Data<AppState>) -> impl Responder {
    let (tx, rx) = oneshot::channel();

    data.run_tx
        .send(RunnerMessage::Start {
            tags: spec.tags.clone(),
            tasks: spec.tasks.clone(),
            response: tx,
            parameters: spec.parameters.clone(),
            logger: data.log_tx.clone(),
            executor: data.exe_tx.clone(),
        })
        .unwrap();

    match rx.await.unwrap() {
        Ok(run_id) => HttpResponse::Ok().json(RunIDResponse { run_id }),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
}

async fn get_run(path: web::Path<RunID>, data: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    data.log_tx
        .send(LoggerMessage::GetRun { run_id, response })
        .unwrap();

    match rx.await.unwrap() {
        Ok(run) => HttpResponse::Ok().json(run),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
}

async fn get_task_summary(path: web::Path<RunID>, data: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    data.log_tx
        .send(LoggerMessage::GetTaskSummary { run_id, response })
        .unwrap();

    match rx.await.unwrap() {
        Ok(tasks) => HttpResponse::Ok().json(tasks),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
}

async fn get_run_state(path: web::Path<RunID>, data: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    data.log_tx
        .send(LoggerMessage::GetState { run_id, response })
        .unwrap();

    match rx.await.unwrap() {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
}

async fn get_run_tasks(path: web::Path<RunID>, data: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    data.log_tx
        .send(LoggerMessage::GetTaskSummary { run_id, response })
        .unwrap();

    match rx.await.unwrap() {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(_) => HttpResponse::BadRequest().json(SimpleError {
            error: "No such run".to_owned(),
        }),
    }
}

async fn get_run_task(
    path: web::Path<(RunID, TaskID)>,
    data: web::Data<AppState>,
) -> impl Responder {
    let (run_id, task_id) = path.into_inner();
    let (response, rx) = oneshot::channel();

    data.log_tx
        .send(LoggerMessage::GetTask {
            run_id,
            task_id,
            response,
        })
        .unwrap();

    match rx.await.unwrap() {
        Ok(task) => HttpResponse::Ok().json(task),
        Err(_) => HttpResponse::BadRequest().json(SimpleError {
            error: "No such run".to_owned(),
        }),
    }
}

async fn submit_task_attempt(
    payload: web::Json<AttemptReport>,
    data: web::Data<AppState>,
) -> impl Responder {
    data.run_tx
        .send(RunnerMessage::ExecutionReport {
            run_id: payload.run_id,
            task_id: payload.task_id,
            attempt: payload.attempt.clone(),
        })
        .unwrap();

    HttpResponse::Ok()
}

async fn stop_run(path: web::Path<RunID>, data: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    data.run_tx
        .send(RunnerMessage::StopRun { run_id, response })
        .unwrap();

    rx.await.unwrap();
    HttpResponse::Ok()
}

async fn retry_run(path: web::Path<RunID>, data: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    data.run_tx
        .send(RunnerMessage::Retry {
            run_id,
            logger: data.log_tx.clone(),
            executor: data.exe_tx.clone(),
            response,
        })
        .unwrap();

    match rx.await.unwrap() {
        Ok(()) => HttpResponse::Ok().json(RunIDResponse { run_id }),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
}

async fn ready() -> impl Responder {
    HttpResponse::Ok()
}

struct AppState {
    log_tx: mpsc::UnboundedSender<LoggerMessage>,
    exe_tx: mpsc::UnboundedSender<ExecutorMessage>,
    run_tx: mpsc::UnboundedSender<RunnerMessage>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let (log_tx, log_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        start_memory_logger(log_rx).await;
    });

    let (exe_tx, exe_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        start_local_executor(10, exe_rx).await;
    });

    let (run_tx, run_rx) = mpsc::unbounded_channel();
    let rtx = run_tx.clone();
    tokio::spawn(async move {
        start_dag_runner(rtx, run_rx).await;
    });

    let l_tx = log_tx.clone();
    let e_tx = exe_tx.clone();
    let r_tx = run_tx.clone();

    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let res = HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_header()
            .allow_any_method()
            .allow_any_origin()
            .send_wildcard();

        let json_config = web::JsonConfig::default()
            .limit(1048576)
            .error_handler(|err, _req| {
                use actix_web::error::JsonPayloadError;
                let payload = match &err {
                    JsonPayloadError::OverflowKnownLength { length, limit } => SimpleError {
                        error: format!("Payload too big ({} > {})", length, limit),
                    },
                    JsonPayloadError::Overflow { limit } => SimpleError {
                        error: format!("Payload too big (> {})", limit),
                    },
                    JsonPayloadError::ContentType => SimpleError {
                        error: "Unsupported Content-Type".to_owned(),
                    },
                    JsonPayloadError::Deserialize(e) => SimpleError {
                        error: format!("Parsing error: {}", e),
                    },
                    JsonPayloadError::Serialize(e) => SimpleError {
                        error: format!("JSON Generation error: {}", e),
                    },
                    JsonPayloadError::Payload(payload) => SimpleError {
                        error: format!("Payload error: {}", payload),
                    },
                    _ => SimpleError {
                        error: "Unknown error".to_owned(),
                    },
                };

                error::InternalError::from_response(err, HttpResponse::Conflict().json(payload))
                    .into()
            });

        App::new()
            .wrap(cors)
            .app_data(web::Data::new(AppState {
                log_tx: l_tx.clone(),
                exe_tx: e_tx.clone(),
                run_tx: r_tx.clone(),
            }))
            .wrap(Logger::new(
                r#"%a "%r" %s %b "%{Referer}i" "%{User-Agent}i" %T"#,
            ))
            .app_data(json_config)
            .route("/ready", web::get().to(ready))
            .route("/task/attempt", web::post().to(submit_task_attempt))
            .service(
                web::scope("/api/v1/runs")
                    .route("", web::get().to(get_runs))
                    .route("", web::post().to(submit_run))
                    .service(
                        web::scope("/{run_id}")
                            .route("", web::get().to(get_task_summary))
                            .route("", web::delete().to(stop_run))
                            .route("", web::patch().to(retry_run))
                            .route("/state", web::get().to(get_run_state))
                            .route("/full", web::get().to(get_run))
                            .route("/tasks", web::get().to(get_run_tasks))
                            .route("/tasks/{task_id}", web::get().to(get_run_task)),
                    ),
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await;

    run_tx.send(RunnerMessage::Stop {}).unwrap();
    exe_tx.send(ExecutorMessage::Stop {}).unwrap();
    log_tx.send(LoggerMessage::Stop {}).unwrap();

    res
}
