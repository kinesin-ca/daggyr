mod config;

use actix_cors::Cors;
use actix_web::{error, middleware::Logger, web, App, HttpResponse, HttpServer, Responder};
use chrono::prelude::*;
use clap::Parser;
use config::*;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use daggyr::prelude::*;
use tokio::sync::oneshot;

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

    #[serde(default)]
    pool: Option<String>,
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
    state: web::Data<AppState>,
) -> impl Responder {
    let (response, rx) = oneshot::channel();

    state
        .config
        .tracker
        .send(TrackerMessage::GetRuns {
            tags: criteria.tags.clone(),
            states: criteria.states.clone(),
            start_time: criteria.start_time,
            end_time: criteria.end_time,
            response,
        })
        .unwrap();

    HttpResponse::Ok().json(rx.await.unwrap_or(Vec::new()))
}

async fn submit_run(spec: web::Json<RunSpec>, state: web::Data<AppState>) -> impl Responder {
    let pool = match &spec.pool {
        Some(name) => name.clone(),
        None => state.config.default_pool.clone(),
    };

    if !state.config.pools.contains_key(&pool) {
        return HttpResponse::BadRequest().json(SimpleError {
            error: format!("Pool {} is not defined", pool),
        });
    }

    // Validate the tasks
    let (response, rx) = oneshot::channel();
    state.config.pools[&pool]
        .send(ExecutorMessage::ValidateTasks {
            tasks: spec.tasks.clone(),
            response,
        })
        .expect("Unable to contact executor");
    if let Err(e) = rx.await.unwrap() {
        return HttpResponse::BadRequest().json(SimpleError {
            error: format!("Invalid tasks for pool {}: {:?}", pool, e),
        });
    }

    let (tx, rx) = oneshot::channel();
    state
        .config
        .runner
        .send(RunnerMessage::Start {
            tags: spec.tags.clone(),
            tasks: spec.tasks.clone(),
            response: tx,
            parameters: spec.parameters.clone(),
            tracker: state.config.tracker.clone(),
            executor: state.config.pools.get(&pool).unwrap().clone(),
        })
        .unwrap();

    match rx.await.unwrap() {
        Ok(run_id) => HttpResponse::Ok().json(RunIDResponse { run_id }),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
}

async fn get_run(path: web::Path<RunID>, state: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    state
        .config
        .tracker
        .send(TrackerMessage::GetRun { run_id, response })
        .unwrap();

    match rx.await.unwrap() {
        Ok(run) => HttpResponse::Ok().json(run),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
}

async fn get_task_summary(path: web::Path<RunID>, state: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    state
        .config
        .tracker
        .send(TrackerMessage::GetTaskSummary { run_id, response })
        .unwrap();

    match rx.await.unwrap() {
        Ok(tasks) => HttpResponse::Ok().json(tasks),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
}

async fn get_run_state(path: web::Path<RunID>, state: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    state
        .config
        .tracker
        .send(TrackerMessage::GetState { run_id, response })
        .unwrap();

    match rx.await.unwrap() {
        Ok(state) => HttpResponse::Ok().json(state),
        Err(error) => HttpResponse::BadRequest().json(SimpleError {
            error: format!("{:?}", error),
        }),
    }
}

async fn get_run_tasks(path: web::Path<RunID>, state: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    state
        .config
        .tracker
        .send(TrackerMessage::GetTaskSummary { run_id, response })
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
    state: web::Data<AppState>,
) -> impl Responder {
    let (run_id, task_id) = path.into_inner();
    let (response, rx) = oneshot::channel();

    state
        .config
        .tracker
        .send(TrackerMessage::GetTask {
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
    state: web::Data<AppState>,
) -> impl Responder {
    state
        .config
        .runner
        .send(RunnerMessage::ExecutionReport {
            run_id: payload.run_id,
            task_id: payload.task_id,
            attempt: payload.attempt.clone(),
        })
        .unwrap();

    HttpResponse::Ok()
}

async fn stop_run(path: web::Path<RunID>, state: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    state
        .config
        .runner
        .send(RunnerMessage::StopRun { run_id, response })
        .unwrap();

    rx.await.unwrap();
    HttpResponse::Ok()
}

async fn retry_run(path: web::Path<RunID>, state: web::Data<AppState>) -> impl Responder {
    let run_id = path.into_inner();
    let (response, rx) = oneshot::channel();

    let pool = {
        let pools = state.run_pools.lock().unwrap();
        pools
            .get(&run_id)
            .unwrap_or(&state.config.default_pool)
            .clone()
    };

    state
        .config
        .runner
        .send(RunnerMessage::Retry {
            run_id,
            tracker: state.config.tracker.clone(),
            executor: state.config.pools.get(&pool).unwrap().clone(),
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

fn init(config_file: &str) -> GlobalConfig {
    let spec: GlobalConfigSpec = if config_file.is_empty() {
        serde_json::from_str("{}").unwrap()
    } else {
        let json = std::fs::read_to_string(config_file)
            .expect(&format!("Unable to open {} for reading", config_file));
        serde_json::from_str(&json).expect("Error parsing config json")
    };

    GlobalConfig::new(&spec)
}

#[derive(Parser, Debug)]
#[clap(author, version, about)]
struct Args {
    /// Configuration File
    #[clap(short, long, default_value = "")]
    config: String,

    /// Enable verbose logging
    #[clap(short, long)]
    verbose: bool,
}

#[derive(Clone, Debug)]
struct AppState {
    config: GlobalConfig,
    run_pools: Arc<Mutex<HashMap<RunID, String>>>,
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let config = init(args.config.as_ref());

    if args.verbose {
        println!("{:?}", config);
    }

    let data = web::Data::new(AppState {
        config: config.clone(),
        run_pools: Arc::new(Mutex::new(HashMap::new())),
    });

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
            .app_data(data.clone())
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
    .bind(config.listen_spec())?
    .run()
    .await;

    config.runner.send(RunnerMessage::Stop {}).unwrap();
    for exe_tx in config.pools.values() {
        exe_tx.send(ExecutorMessage::Stop {}).unwrap();
    }
    config.tracker.send(TrackerMessage::Stop {}).unwrap();

    res
}
