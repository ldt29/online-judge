use actix_web::HttpResponse;
use serde::{Deserialize, Serialize};
use r2d2::{Pool};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, Result};
use chrono::NaiveDateTime;
use std::{
    cmp::Ordering,
    collections::HashSet,
    str::FromStr
};

use crate::{
    get, post,
    web, Responder, 
    ErrorMessage, 
    user::{User, query_users},
    job::{query_jobs, JobContent},
    Config
};


#[derive(Clone, Deserialize, Serialize)]
struct QueryRanklist {
    scoring_rule: Option<String>,
    tie_breaker: Option<String>
}

#[derive(Clone, Deserialize, Serialize)]
struct RankContent {
    user: User,
    rank: u64,
    scores: Vec<f64>
}
impl RankContent {
    fn check_self(&self, user_id: usize) -> bool {
        if self.user.id == user_id {
            true
        }else{
            false
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct PostContest {
    id: Option<usize>,
    name: String,
    from: String,
    to: String,
    problem_ids: Vec<usize>,
    user_ids: Vec<usize>,
    submission_limit: u64
}


#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Contest {
    id: usize,
    name: String,
    pub from: String,
    pub to: String,
    pub problem_ids: Vec<usize>,
    pub user_ids: Vec<usize>,
    pub submission_limit: u64
}



#[post("contests")]
#[allow(unreachable_code)]
async fn post_contests(
    post_contest: web::Json<PostContest>, 
    config: web::Data<Config>,
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Post contests {}", post_contest.name);
    let conn = pool.get().unwrap();
    // check problem ids
    for problem_id in &post_contest.problem_ids {
        if problem_id >= &config.problems.len() {        
            return HttpResponse::NotFound().json({
                ErrorMessage{
                    code: (3),
                    reason: ("ERR_NOT_FOUND".to_string()),
                    message: ("Problem ".to_string() + &problem_id.to_string() + " not found.")
                }
            });
        }
    }

    // check user ids
    let user_list = query_users(&conn).unwrap();
    for user_id in &post_contest.user_ids {
        if user_id >= &user_list.len() {
            return HttpResponse::NotFound().json({
                ErrorMessage{
                    code: (3),
                    reason: ("ERR_NOT_FOUND".to_string()),
                    message: ("User ".to_string() + &user_id.to_string() + " not found.")
                }
            });
        }
    }
    
    let contest_list = query_contests(&conn).unwrap();
    match post_contest.id {
        Some( id ) => {
            // check contest id
            if id == 0 {
                HttpResponse::NotFound().json({
                    ErrorMessage{
                        code: (0),
                        reason: ("ERR_INVALID_ARGUMENT".to_string()),
                        message: ("Invalid contest id".to_string())
                    }
                })
            }
            else {
                match query_contest(&conn, id) {
                    Ok(_) => {
                        let contest = Contest{
                            id: (id),
                            name: (post_contest.name.clone()),
                            from: (post_contest.from.clone()),
                            to: (post_contest.to.clone()),
                            problem_ids: (post_contest.problem_ids.clone()),
                            user_ids: (post_contest.user_ids.clone()),
                            submission_limit: (post_contest.submission_limit.clone())
                        };
                        update_contest(&conn, &contest).unwrap();
                        HttpResponse::Ok().json(contest)
                    }
                    Err(_) => {
                        HttpResponse::NotFound().json({
                            ErrorMessage{
                                code: (3),
                                reason: ("ERR_NOT_FOUND".to_string()),
                                message: ("Problem ".to_string() + &id.to_string() + " not found.")
                            }
                        })
                    }
                }
            }
        }
        None => {
            let contest = Contest{
                id: (contest_list.len() + 1),
                name: (post_contest.name.clone()),
                from: (post_contest.from.clone()),
                to: (post_contest.to.clone()),
                problem_ids: (post_contest.problem_ids.clone()),
                user_ids: (post_contest.user_ids.clone()),
                submission_limit: (post_contest.submission_limit.clone())
            };
            insert_contest(&conn, &contest).unwrap();
            HttpResponse::Ok().json(contest)
        }
    }

}


#[get("contests")]
#[allow(unreachable_code)]
async fn get_contests(
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Get contests");
    let conn = pool.get().unwrap();
    let contest_list = query_contests(&conn).unwrap();
    HttpResponse::Ok().json(contest_list)
}

#[get("contests/{contestId}")]
#[allow(unreachable_code)]
async fn get_contests_id(
    contest_id: web::Path<usize>,
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Get contests {contest_id}");
    let conn = pool.get().unwrap();
    let query_contest = query_contest(&conn, *contest_id);
    match query_contest {
        Ok(contest) => HttpResponse::Ok().json(contest),
        Err(_) => HttpResponse::NotFound().json({
            ErrorMessage{
                code: (3),
                reason: ("ERR_NOT_FOUND".to_string()),
                message: ("Job ".to_string() + &contest_id.to_string() + " not found.")
            }
        })
    }

}

#[get("/contests/{contestId}/ranklist")]
#[allow(unreachable_code)]
async fn get_contests_id_ranklist(
    contest_id: web::Path<usize>, 
    request: web::Query<QueryRanklist>, 
    config: web::Data<Config>,
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Get contests {} ranklist", contest_id);
    let conn = pool.get().unwrap();
    let contest_list = query_contests(&conn).unwrap();
    if *contest_id <= contest_list.len() {
        
        let mut rank_contents = Vec::new();
        let user_list = query_users(&conn).unwrap();
        let mut user_ids: Vec<usize> = (0..user_list.len()).collect();
        let mut problem_ids: Vec<usize> = (0..config.problems.len()).collect();
        if  *contest_id > 0 {
            user_ids = contest_list[*contest_id - 1].user_ids.clone();
            problem_ids = contest_list[*contest_id - 1].problem_ids.clone();
        }

        // init rank
        for user in &user_list {
            if user_ids.contains(&user.id) {
                let rank_content = RankContent{
                    user: (user.clone()),
                    rank: (0),
                    scores: (vec!(0.0; problem_ids.len()))
                };
                rank_contents.push(rank_content);
            }
        }
        
        
        let job_list_raw = query_jobs(&conn).unwrap();
        let mut job_list =Vec::new();
        if *contest_id == 0 {
            job_list = job_list_raw.clone();
        }else {
            for job in &*job_list_raw {
                if check_job(job, &contest_list[*contest_id - 1]) {
                    job_list.push(job.clone());
                }
            }
        }
        

        // remember user id and problem id
        let mut user_problem_id: HashSet<(usize, usize)> = HashSet::new();
        let mut job_in_use: Vec<JobContent> = Vec::new();
        // compute scores
        for job in &job_list {
            let user_id = job.submission.user_id;
            let problem_id = job.submission.problem_id;

            let mut rank_content_id = 0;
            for index in 0..rank_contents.len() {
                if rank_contents[index].check_self(user_id) {
                    rank_content_id = index;
                    break;
                }
            }
            let mut score_id: usize = 0;
            for index in 0..problem_ids.len() {
                if problem_id == problem_ids[index] {
                    score_id = index;
                }
            }

            let mut is_update = false;
            match &request.scoring_rule {
                Some(rule) => {
                    if rule == "latest" {
                        is_update = true;
                    }
                    else {
                    
                        if  rank_contents[rank_content_id].scores[score_id] < job.score{
                            is_update = true;
                        }
                        if !user_problem_id.contains(&(user_id, problem_id)) {
                            is_update = true;
                        }
                    }
                }
                None => is_update = true
            }
            if is_update {
                // no need to delete
                rank_contents[rank_content_id].scores[score_id] = job.score;
                job_in_use.push(job.clone());
                user_problem_id.insert((user_id, problem_id));
            }
        }

        // sort
        let mut compare: Box<dyn Fn(&RankContent, &RankContent) -> Ordering> = Box::new(|a, b| {
            b.scores.iter().sum::<f64>()
            .partial_cmp(&a.scores.iter().sum::<f64>())
            .unwrap_or(Ordering::Equal)
        });
        match &request.tie_breaker {
            Some(tie_breaker) => {
                match tie_breaker.as_str() {
                    "submission_time" => {
                        compare = Box::new(|a, b| {
                            b.scores.iter().sum::<f64>()
                            .partial_cmp(&a.scores.iter().sum::<f64>())
                            .unwrap_or(Ordering::Equal)
                            .then(a.user.get_submisson_time(&job_in_use)
                                .cmp(&b.user.get_submisson_time(&job_in_use))
                            )
                        });
                    }
                    "submission_count" => {
                        compare = Box::new(|a, b| {
                            b.scores.iter().sum::<f64>()
                            .partial_cmp(&a.scores.iter().sum::<f64>())
                            .unwrap_or(Ordering::Equal)
                            .then(a.user.get_submisson_count(&job_list)
                                .cmp(&b.user.get_submisson_count(&job_list))
                            )
                        });
                    }
                    _ => {
                        compare = Box::new(|a, b| {
                            b.scores.iter().sum::<f64>()
                            .partial_cmp(&a.scores.iter().sum::<f64>())
                            .unwrap_or(Ordering::Equal)
                            .then(a.user.id.cmp(&b.user.id))
                        });
                    }
                }
            }
            None => {
                rank_contents.sort_by_key(|a| a.user.id);
   
            }
        }
        rank_contents.sort_by(&compare);
        // assign rank
        let mut rank = 1;
        rank_contents[0].rank = rank;
        for index in 1..rank_contents.len() {
            rank += 1;
            if compare(&rank_contents[index], &rank_contents[index - 1]) == Ordering::Equal {
                rank_contents[index].rank = rank_contents[index - 1].rank;
            }else {
                rank_contents[index].rank = rank;
            }

        }

        HttpResponse::Ok().json(rank_contents)
    }
    else {
        HttpResponse::NotFound().json({
            ErrorMessage{
                code: (3),
                reason: ("ERR_NOT_FOUND".to_string()),
                message: ("Contest ".to_string() + &contest_id.to_string() + " not found.")
            }
        })
    }
}


pub fn check_job(job: &JobContent, contest: &Contest) -> bool {
    let contest_id = job.submission.contest_id;
    let user_id = job.submission.user_id;
    let problem_id = job.submission.problem_id;
    if contest.id != contest_id {
        return false;
    }
    if !contest.user_ids.contains(&user_id) {
        return false;
    }
    if !contest.problem_ids.contains(&problem_id) {
        return false;
    }
    let fmt = "%Y-%m-%dT%H:%M:%S%.3fZ";
    let from_time = NaiveDateTime::parse_from_str(&contest.from, fmt)
    .expect("failed to get time");
    let to_time = NaiveDateTime::parse_from_str(&contest.to, fmt)
    .expect("failed to get time");
    let creat_time = NaiveDateTime::parse_from_str(&job.created_time, fmt)
    .expect("failed to get time");
    if to_time < creat_time {
        return false;
    }
    if from_time > creat_time {
        return false;
    }
    if job.state != "Finished" {
        return false;
    }

    true
}


pub fn format_vec(v: &Vec<usize>) -> String {
    v.iter()
        .map(|x| x.to_string())
        .collect::<Vec<String>>()
        .join(",")
}

pub fn parse_vec(s: String) -> Vec<usize> {
    s.split(',')
        .map(|x| usize::from_str(x).unwrap())
        .collect()
}

fn insert_contest(conn: &Connection, contest: &Contest) -> Result<()> {
    conn.execute(
        "INSERT INTO contests 
        (id, name, 'from', 'to', problem_ids, user_ids, submission_limit) 
        VALUES (?, ?, ?, ?, ?, ?, ?)",
        params![
            contest.id, 
            contest.name,
            contest.from,
            contest.to,
            format_vec(&contest.problem_ids),
            format_vec(&contest.user_ids),
            contest.submission_limit
        ],
    )?;
    Ok(())
}

fn update_contest(conn: &Connection, contest: &Contest) -> Result<()> {
    conn.execute(
        "UPDATE contests SET 
                name = ?, 
                'from' = ?, 
                'to' = ?, 
                problem_ids = ?, 
                user_ids = ?, 
                submission_limit = ? 
                WHERE id = ?",
        params![ 
            contest.name,
            contest.from,
            contest.to,
            format_vec(&contest.problem_ids),
            format_vec(&contest.user_ids),
            contest.submission_limit,
            contest.id
        ],
    )?;
    Ok(())
}

pub fn query_contests(conn: &Connection) -> Result<Vec<Contest>> {
    let mut stmt = conn.prepare("SELECT * FROM contests")?;

    let rows = stmt.query_map([], |row| {
        Ok(Contest {
            id: row.get(0)?,
            name: row.get(1)?,
            from: row.get(2)?,
            to: row.get(3)?,
            problem_ids: parse_vec(row.get(4)?),
            user_ids: parse_vec(row.get(5)?),
            submission_limit: row.get(6)?
        })
    })?;

    let contests = rows.collect();
    contests
}


pub fn query_contest(conn: &Connection, id: usize) -> Result<Contest> {
    let mut stmt = conn.prepare("SELECT * FROM contests WHERE id = ?")?;
    let mut contests = stmt.query_map(params![id], |row| {
        Ok(Contest {
            id: row.get(0)?,
            name: row.get(1)?,
            from: row.get(2)?,
            to: row.get(3)?,
            problem_ids: parse_vec(row.get(4)?),
            user_ids: parse_vec(row.get(5)?),
            submission_limit: row.get(6)?
        })
    })?;
    contests.next().unwrap_or(Err(rusqlite::Error::QueryReturnedNoRows))
}