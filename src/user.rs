use serde::{Deserialize, Serialize};
use actix_web::HttpResponse;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, Result};
use chrono::*;
use std::{
    sync::{Arc, Mutex}
};

use lazy_static::lazy_static;

use crate::{
    post, get, 
    web, Responder, 
    ErrorMessage, job::JobContent
};

// static user list 
lazy_static! {
    pub static ref USER_LIST: Arc<Mutex<Vec<User>>> = Arc::new(Mutex::new(Vec::new()));
}

#[derive(Clone, Deserialize, Serialize)]
pub struct PostUser{
    id: Option<usize>,
    name: String
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct User{
    pub id: usize,
    pub name: String
}
impl User {
    pub fn get_submisson_time(&self, job_list: &Vec<JobContent>) -> NaiveDateTime {
        let s = "2022-08-27T02:05:29.000Z";
        let fmt = "%Y-%m-%dT%H:%M:%S%.3fZ";
        let mut latest_time: NaiveDateTime = NaiveDateTime::parse_from_str(s, fmt)
        .expect("failed to get time");
        for job in job_list {
            if job.submission.user_id == self.id {
                let s = job.created_time.as_str();
                let current_time: NaiveDateTime = NaiveDateTime::parse_from_str(s, fmt)
                .expect("failed to get time");
                if current_time > latest_time {
                    latest_time = current_time;
                }
            }
        }
        if self.get_submisson_count(job_list) == 0  {
            let s = "3022-08-27T02:05:29.000Z";
            latest_time = NaiveDateTime::parse_from_str(s, fmt)
            .expect("failed to get time");
        }
        latest_time
    }   

    pub fn get_submisson_count(&self, job_list: &Vec<JobContent>) -> u64 {
        let mut submission_count: u64 = 0;
        for job in job_list {
            if job.submission.user_id == self.id {
                submission_count += 1;
        
            }
        }
        submission_count
    }

}

#[post("/users")]
#[allow(unreachable_code)]
async fn post_users(
    body: web::Json<PostUser>,
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Post user {:?}", body.name);

    let conn = pool.get().unwrap();
    let user_list = query_users(&conn).unwrap();
    // check same name 
    for user in & user_list {
        if user.name == body.name {
            return HttpResponse::BadRequest().json({
                ErrorMessage{
                    code: (1),
                    reason: ("ERR_INVALID_ARGUMENT".to_string()),
                    message: ("User name '".to_string() + &user.name + "' already exists.")
                }
            });
        }
    }
    match body.id {
        Some(id) =>{
            // and check id
            match query_user(&conn, id) {
                Ok(_) => {
                    let user = User{
                        id: (id),
                        name: (body.name.clone())
                    };
                    update_user(&conn, &user).unwrap();
                    HttpResponse::Ok().json(user)
                }
                Err(_) => {
                    HttpResponse::NotFound().json({
                        ErrorMessage{
                            code: (3),
                            reason: ("ERR_NOT_FOUND".to_string()),
                            message: ("User ".to_string() + &id.to_string() + " not found.")
                        }
                    })
                }
            }
        }
        None => {
            // new user
            let user = User{
                id: (user_list.len()),
                name: (body.name.clone())
            };
            insert_user(&conn, &user).unwrap();
            HttpResponse::Ok().json(user)
        }
    }


}


#[get("/users")]
#[allow(unreachable_code)]
async fn get_users(
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Get user");
    let conn = pool.get().unwrap();
    let user_list = query_users(&conn).unwrap();
    HttpResponse::Ok().json(user_list)
}

pub fn init_root_user(
    conn: &PooledConnection<SqliteConnectionManager>
) -> std::io::Result<()> {
    let root = User{
        id: (0),
        name: ("root".to_string())
    };
    let user_list = query_users(conn).unwrap();
    if user_list.len() == 0 {
        insert_user(conn, &root).unwrap();
    }
    Ok(())
}

fn insert_user(conn: &Connection, user: &User) -> Result<()> {
    conn.execute(
        "INSERT INTO users (id, name) VALUES (?, ?)",
        params![user.id, user.name],
    )?;
    Ok(())
}

pub fn query_users(conn: &Connection) -> Result<Vec<User>> {
    let mut stmt = conn.prepare("SELECT id, name FROM users")?;
    let users = stmt.query_map([], |row| {
        Ok(User {
            id: row.get(0)?,
            name: row.get(1)?,
        })
    })?;
    users.collect()
}

fn update_user(conn: &Connection, user: &User) -> Result<()> {
    conn.execute(
        "UPDATE users SET name = ? WHERE id = ?",
        params![user.name, user.id],
    )?;
    Ok(())
}

pub fn query_user(conn: &Connection, id: usize) -> Result<User> {
    let mut stmt = conn.prepare("SELECT id, name FROM users WHERE id = ?")?;
    let mut users = stmt.query_map(params![id], |row| {
        Ok(User {
            id: row.get(0)?,
            name: row.get(1)?,
        })
    })?;
    users.next().unwrap_or(Err(rusqlite::Error::QueryReturnedNoRows))
}