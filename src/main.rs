use actix_web::{get, middleware::Logger, post, web, App, HttpServer, Responder, put, delete};
use serde::{Deserialize, Serialize};
use env_logger;
use log;
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{Connection, Result};
use clap::Parser;

mod config;
use config::Config;
use config::Cli;

mod job;
use job::{
    post_jobs,
    get_jobs,
    get_jobs_id,
    put_jobs,
    delete_jobs
};

mod user;
use user::{
    post_users,
    get_users
};

mod contest;
use contest::{
    post_contests,
    get_contests,
    get_contests_id,
    get_contests_id_ranklist
};

/// error message
#[derive(Clone, Deserialize, Serialize)]
struct ErrorMessage {
    code: i32,
    reason: String,
    message: String
}

#[get("/hello/{name}")]
async fn greet(name: web::Path<String>) -> impl Responder {
    log::info!(target: "greet_handler", "Greeting {}", name);
    format!("Hello {name}!")
}

// DO NOT REMOVE: used in automatic testing
#[post("/internal/exit")]
#[allow(unreachable_code)]
async fn exit() -> impl Responder {
    log::info!("Shutdown as requested");
    std::process::exit(0);
    format!("Exited")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cli = Cli::parse();
    let config =  cli.init_config()?;
    // user database
    let manager = SqliteConnectionManager::file("database/data.db");
    let pool = Pool::builder().build(manager).unwrap();
    let conn = pool.get().unwrap();
    flush_user_table(&conn, cli.flush_data).unwrap();
    create_user_table(&conn).unwrap();
    user::init_root_user(&conn)?;


    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    
    
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(config.clone()))
            .app_data(web::Data::new(pool.clone()))
            .wrap(Logger::default())
            .route("/hello", web::get().to(|| async { "Hello World!" }))
            .service(greet)
            // DO NOT REMOVE: used in automatic testing
            .service(exit)
            .service(post_jobs)
            .service(get_jobs)
            .service(get_jobs_id)
            .service(put_jobs)
            .service(delete_jobs)
            .service(post_users)
            .service(get_users)
            .service(post_contests)
            .service(get_contests)
            .service(get_contests_id)
            .service(get_contests_id_ranklist)
            
    })
    .bind(("127.0.0.1", 12345))?
    .run()
    .await
}

fn create_user_table(conn: &Connection) -> Result<()> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL
        )",
        [],
    )?;
    // problem_ids and user_ids need vec to test
    // when read, we need vec to test
    conn.execute(
        "CREATE TABLE IF NOT EXISTS contests (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            'from' TEXT NOT NULL,
            'to' TEXT NOT NULL,
            problem_ids TEXT NOT NULL,
            user_ids TEXT NOT NULL,
            submission_limit INTEGER NOT NULL
        )",
        [],
    )?;
    // case need vec to test
    // when read, we need vec to test
    // sunmission divide into 5 parts
    conn.execute(
        "CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY,
            created_time TEXT NOT NULL,
            updated_time TEXT NOT NULL,
            source_code TEXT NOT NULL,
            language TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            contest_id INTEGER NOT NULL,
            problem_id INTEGER NOT NULL,
            state TEXT NOT NULL,
            result TEXT NOT NULL,
            score REAL NOT NULL,
            cases TEXT NOT NULL
        )",
        [],
    )?;
    Ok(())
}

fn flush_user_table(conn: &Connection, flush: bool) -> Result<()>{
    if flush {
        conn.execute(
            "DROP TABLE IF EXISTS users" , 
            [],
        )?;
        conn.execute(
            "DROP TABLE IF EXISTS contests" , 
            [],
        )?;
        conn.execute(
            "DROP TABLE IF EXISTS jobs" , 
            [],
        )?;
    }
    Ok(())
}