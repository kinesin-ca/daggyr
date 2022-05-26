mod config;

use actix_cors::Cors;
use actix_web::{error, middleware::Logger, web, App, HttpResponse, HttpServer, Responder};
use clap::Parser;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};

use config::*;
use daggyr::messages::*;
use daggyr::prelude::*;

#[derive(Serialize)]
struct SimpleError {
    error: String,
}

async fn get_resources(data: web::Data<GlobalConfig>) -> impl Responder {
    HttpResponse::Ok().json(data.resources.clone())
}

async fn submit_task(
    path: web::Path<(RunID, TaskID)>,
    task: web::Json<Task>,
    data: web::Data<GlobalConfig>,
) -> impl Responder {
    let (run_id, task_id) = path.into_inner();
    let (response, mut rx) = mpsc::unbounded_channel();

    let trx = data.tracker.clone();

    data.executor
        .send(ExecutorMessage::ExecuteTask {
            run_id,
            task_id,
            task: task.into_inner(),
            tracker: trx,
            response,
        })
        .unwrap();

    match rx.recv().await.unwrap() {
        RunnerMessage::ExecutionReport { attempt, .. } => HttpResponse::Ok().json(attempt),
        other => HttpResponse::BadRequest().json(SimpleError {
            error: format!("Unexpected message {:?}", other),
        }),
    }
}

async fn stop_task(
    path: web::Path<(RunID, TaskID)>,
    data: web::Data<GlobalConfig>,
) -> impl Responder {
    let (run_id, task_id) = path.into_inner();
    let (response, rx) = oneshot::channel();

    data.executor
        .send(ExecutorMessage::StopTask {
            run_id,
            task_id,
            response,
        })
        .unwrap();

    rx.await.unwrap();
    HttpResponse::Ok()
}

async fn ready() -> impl Responder {
    HttpResponse::Ok()
}

fn init(config_file: &str) -> GlobalConfig {
    let spec: GlobalConfigSpec = if config_file.is_empty() {
        GlobalConfigSpec::default()
    } else {
        let json = std::fs::read_to_string(config_file)
            .unwrap_or_else(|_| panic!("Unable to open {} for reading", config_file));
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

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    let data = web::Data::new(init(args.config.as_ref()));
    let config = data.clone();

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
            .service(
                web::scope("/api/v1")
                    .route("/resources", web::get().to(get_resources))
                    .route("/{run_id}/{task_id}", web::post().to(submit_task))
                    .route("/{run_id}/{task_id}", web::delete().to(stop_task)),
            )
    })
    .bind(config.listen_spec())?
    .run()
    .await;

    config.executor.send(ExecutorMessage::Stop {}).unwrap();
    config.tracker.send(TrackerMessage::Stop {}).unwrap();

    res
}
