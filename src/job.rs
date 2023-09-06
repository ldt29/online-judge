use actix_web::HttpResponse;
use chrono::*;
use serde::{Deserialize, Serialize};
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Connection, Result};
use std::{
    process::{Stdio}, 
    fs::{self, File},
    str::FromStr
};

use tokio::process::Command;

use tokio::time::{timeout, Duration};

use crate::{
    post, get, put, delete,
    web, Responder, 
    Config, config::{Language, self, Problem},
    ErrorMessage,
    user::{query_users},
    contest::{check_job, query_contests}
};

// id time memory should be u64
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostJob {
    source_code: String,
    language: String,
    pub user_id: usize,
    pub contest_id: usize,
    pub problem_id: usize,
}
impl PostJob {
    fn new() -> PostJob{
        PostJob { source_code: (String::new()), language: (String::new()), user_id: (0), contest_id: (0), problem_id: (0) }
    }
}

#[derive(Clone, Deserialize, Serialize)]
struct Response {
    status: i32,
    content: JobContent,
}



#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobContent {
    id: usize,
    pub created_time: String,
    updated_time: String,
    pub submission: PostJob,
    pub state: String,
    result: String,
    pub score: f64,
    cases: Vec<Case>,
}

impl JobContent {
    fn new(len: usize) -> JobContent{
        JobContent {
            id: (len), 
            created_time: (String::new()),
            updated_time: (String::new()),
            submission: (PostJob::new()),
            state: ("Queueing".to_string()), 
            result: ("Waiting".to_string()), 
            score: (0.0), 
            cases: (Vec::new()) 
        }
    }

}


#[derive(Debug, Clone, Deserialize, Serialize)]
struct Case {
    id: usize,
    result: String,
    time: u64,
    memory: u64,
    info: String
}
impl Case {
    fn new(case_id: usize) -> Case {
        Case { 
            id: (case_id), 
            result: ("Waiting".to_string()), 
            time: (0), 
            memory: (0), 
            info: (String::new()) 
        }
    }
}


/// Temporary directories
struct Tempdir {
    path: String,
    src_path: String,
    app_path: String,
}


impl Tempdir {
    /// init temporary directory
    async fn init_tempdir(&self){
         // init temporary directory
         let _mkdir_status = Command::new("mkdir")
         .arg(&self.path)
         .status()
         .await
         .expect("failed to execute process");
    }
    /// delete temporary directory
    async fn delete_tempdir(&self){
        let _mkdir_status = Command::new("rm")
        .args(["-rf", &self.path])
        .status()
        .await
        .expect("failed to execute process");
    }

    /// compilate source code
    async fn compilate(&mut self, language: &Language, source_code: &String) -> bool {

        // write source code to file
        self.src_path = self.path.clone() + &language.file_name;
        let src_file = File::create(self.src_path.clone())
        .expect("failed to creat file");

        let _write_status = Command::new("echo")
        .arg(source_code.to_string())
        .stdout(Stdio::from(src_file))
        .status()
        .await;

        // compilate
        self.app_path = self.path.clone() + "test";
        let mut args = language.command[1..].to_vec();
        args.iter_mut().for_each(|x| {if *x == "%OUTPUT%" {*x = self.app_path.clone()}});
        args.iter_mut().for_each(|x| {if *x == "%INPUT%" {*x = self.src_path.clone()}});
        
        let compilate_status = Command::new(language.command[0].clone())
        .args(args)
        .status()
        .await;
        compilate_status.unwrap().success()

        
                            
    }

    /// judge case
    async fn judge(&self, case: &config::Case, ty: &String) -> String {


        let in_file = File::open(case.input_file.clone()).expect("failed to open file");
        let out_file = File::create(self.path.clone() + "test.out").expect("failed to creat file");

        let run_status = Command::new(self.app_path.clone())
        .stdin(Stdio::from(in_file))
        .stdout(Stdio::from(out_file))
        .status();


        let mut wait_timeout = Duration::MAX; 
        if case.time_limit > 0 {
            wait_timeout = Duration::from_micros(case.time_limit);
        }
        let time_status = timeout(wait_timeout, run_status)
        .await;

        match time_status {
            Ok(status) => {
                match status {
                    Ok(_) => {
                        if self.compare_out_ans(&case.answer_file, ty) {
                            "Accepted".to_string()
                        }else {
                            "Wrong Answer".to_string()
                        }
                    }
                    Err(_) => "Runtime Error".to_string()
                }
            }
            Err(_) => {
                "Time Limit Exceeded".to_string()
            }
        }
    }

    /// compare output file and answer file
    fn compare_out_ans(&self, ans_path: &String, ty: &String) -> bool{
        let output_content = fs::read_to_string(self.path.clone() + "test.out")
        .expect("failed to read file");
        let answer_content = fs::read_to_string(ans_path.clone())
        .expect("failed to read file");
        match ty.as_str() {
            "standard" => {
                
                // the same lines
                let mut lines_count = 0;
                for (line1, line2) in output_content.lines().zip(answer_content.lines()){
                    let trimmed1 = line1.trim_end();
                    let trimmed2 = line2.trim_end();
                    lines_count += 2;
                    if trimmed1 != trimmed2 {
                        println!("same lines wrong");
                        return false;
                    }
                } 

                // more lines
                for line in output_content.lines().chain(answer_content.lines()).skip(lines_count){
                    let trimmed = line.trim_end();
                    if trimmed.len() > 0 {
                        println!("line_count {} more lines wrong:{:?}",lines_count,trimmed);
                        return false;
                    }
                }
                for line in answer_content.lines().chain(output_content.lines()).skip(lines_count){
                    let trimmed = line.trim_end();
                    if trimmed.len() > 0 {
                        println!("line_count {} more lines wrong:{:?}",lines_count,trimmed);
                        return false;
                    }
                }
                true
            }
            "strict" => {
                output_content == answer_content
            }
            _ => false
        }
    }

}

async fn judge_job(
    job_id: usize,
    pool: web::Data<Pool<SqliteConnectionManager>>,
    problems: Vec<Problem>,
    languages: Vec<Language>
) {
    let conn = pool.get().unwrap();
    match query_job(&conn, job_id) {
        Ok(mut job) => {
            //init temporary directory
            let mut tempdir = Tempdir {
                path: ("tempdir".to_string()+ &job_id.to_string() + "/"),
                src_path: (String::new()),
                app_path: ("tempdir".to_string()+ &job_id.to_string() + "/test")
            };
            tempdir.init_tempdir().await;
            
            let problem = &problems[job.submission.problem_id];
            let mut language = &Language::new();
            for index in 0..languages.len() {
                language = &languages[index];
                if job.submission.language == language.name {
                    break;
                }
            }

            let cases = problem.cases.clone();
            // init
            job.state = "Running".to_string();
            
            update_job(&conn, &job).unwrap();
            // init cases state
            let old_len = job.cases.len();
            for _cases_count in 1..old_len {
                job.cases.pop();
            }
            for case_id in 1..=cases.len() {
                job.cases.push(Case::new(case_id));
            }
            // compilate

            if tempdir.compilate(language, &job.submission.source_code).await{
                job.cases[0].result = "Compilation Success".to_string();
            }else {
                job.cases[0].result = "Compilation Error".to_string();
                job.result = "Compilation Error".to_string();
            }

            update_job(&conn, &job).unwrap();
            // judge
            if job.cases[0].result == "Compilation Success" {
                let mut cases_count = 0;
                for case in &cases {
                    let result = tempdir.judge(case, &problem.ty).await;
                    cases_count += 1;
                    
                    job.cases[cases_count].result = result.clone();
                    if result == "Accepted" {
                        job.score += case.score;
                    }
                    if job.result == "Waiting" || job.result == "Accepted" {
                        job.result = result;
                    }
                    update_job(&conn, &job).unwrap();
                }
            }
            job.state = "Finished".to_string();
            update_job(&conn, &job).unwrap();

            tempdir.delete_tempdir().await;
            
        }
        Err(_) => ()
    }
}

#[post("/jobs")]
#[allow(unreachable_code)]
async fn post_jobs(
    body: web::Json<PostJob>, 
    config: web::Data<Config>,
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {

    let conn = pool.get().unwrap();
    let job_list_raw = query_jobs(&conn).unwrap();


    // init content
    let mut content = JobContent::new(job_list_raw.len());

    log::info!("Post job {}", content.id);

    content.submission = body.clone();
    // time
    let fmt = "%Y-%m-%dT%H:%M:%S%.3fZ";
    let now: DateTime<Utc> = Utc::now();
    content.created_time = now.format(fmt).to_string();
    content.updated_time = content.created_time.clone();
    content.state = "Queueing".to_string();
    
    let problems = &config.problems;
    let languages = &config.languages;

    // check language
    let mut check_language = false;
    for index in 0..languages.len() {
        if body.language == languages[index].name {
            check_language = true;
            break;
        }
    }
    if !check_language {
        return HttpResponse::NotFound().json({
            ErrorMessage{
                code: (3),
                reason: ("ERR_NOT_FOUND".to_string()),
                message: ("Language ".to_string() + &body.language + " not found.")
            }
        });
    }


    // check problem id
    let mut check_problem_id = false;
    let mut problem = &Problem::new();
    for index in 0..problems.len() {
        problem = &problems[index];
        if body.problem_id == problem.id {
            check_problem_id = true;
            break;
        }
    }
    if !check_problem_id {
        return HttpResponse::NotFound().json({
            ErrorMessage{
                code: (3),
                reason: ("ERR_NOT_FOUND".to_string()),
                message: ("Problem ".to_string() + &body.problem_id.to_string() + " not found.")
            }
        });
    }

    // check user id
    let user_list = query_users(&conn).unwrap();
    if body.user_id >= user_list.len() {
        return HttpResponse::NotFound().json({
            ErrorMessage{
                code: (3),
                reason: ("ERR_NOT_FOUND".to_string()),
                message: ("User ".to_string() + &body.user_id.to_string() + " not found.")
            }
        });
    }

    // check contest id
    let contest_list = query_contests(&conn).unwrap();
    if body.contest_id > contest_list.len() {
        return HttpResponse::NotFound().json({
            ErrorMessage{
                code: (3),
                reason: ("ERR_NOT_FOUND".to_string()),
                message: ("Contest ".to_string() + &body.contest_id.to_string() + " not found.")
            }
        });
    }

    

    // check contest
    if body.contest_id > 0 {
        //user problem not in contest
        let contest = &contest_list[body.contest_id - 1];
        if !contest.user_ids.contains(&body.user_id) {
            return HttpResponse::BadRequest().json({
                ErrorMessage{
                    code: (1),
                    reason: ("ERR_INVALID_ARGUMENT".to_string()),
                    message: ("User ".to_string() + &body.user_id.to_string() + " not in contest.")
                }
            });
        }
        if !contest.problem_ids.contains(&body.problem_id) {
            return HttpResponse::BadRequest().json({
                ErrorMessage{
                    code: (1),
                    reason: ("ERR_INVALID_ARGUMENT".to_string()),
                    message: ("Problem ".to_string() + &body.problem_id.to_string() + " not in contest.")
                }
            });
        }
        // contest time
        let created_time = NaiveDateTime::parse_from_str(&content.created_time, fmt).expect("failed to get time");
        let contest_from_time = NaiveDateTime::parse_from_str(&contest.from, fmt).expect("failed to get time");
        let contest_to_time = NaiveDateTime::parse_from_str(&contest.to, fmt).expect("failed to get time");
        if contest_from_time > created_time {
            return HttpResponse::BadRequest().json({
                ErrorMessage{
                    code: (1),
                    reason: ("ERR_INVALID_ARGUMENT".to_string()),
                    message: ("Contest has not started yet.".to_string())
                }
            });
        }

        if contest_to_time < created_time {
            return HttpResponse::BadRequest().json({
                ErrorMessage{
                    code: (1),
                    reason: ("ERR_INVALID_ARGUMENT".to_string()),
                    message: ("Contest is over.".to_string())
                }
            });
        }

        // check submission count
        if contest.submission_limit > 0 {
            let user = &user_list[body.user_id];
            let mut job_list =Vec::new();
            for job in &*job_list_raw {
                if check_job(job, contest) {
                    job_list.push(job.clone());
                }
            }
            if user.get_submisson_count(&job_list) >= contest.submission_limit {
                return HttpResponse::BadRequest().json({
                    ErrorMessage{
                        code: (4),
                        reason: ("ERR_RATE_LIMIT".to_string()),
                        message: ("Submission limit exceeded.".to_string())
                    }
                });
            }
        }
    }   

    let cases = problem.cases.clone();
    // init cases state
    for case_id in 0..=cases.len() {
        content.cases.push(Case::new(case_id));
    }
    // run job
    actix_web::rt::spawn(judge_job(content.id, pool.clone(), problems.clone(), languages.clone()));

    // save all jobs
    insert_job(&conn, &content).unwrap();

    HttpResponse::Ok().json(content)
}

#[derive(Clone, Deserialize, Serialize)]
struct QueryJob {
    user_id: Option<usize>,
    user_name: Option<String>,
    contest_id: Option<usize>,
    problem_id: Option<usize>,
    language: Option<String>,
    from: Option<String>,
    to: Option<String>,
    state: Option<String>,
    result: Option<String>
}
impl QueryJob {
    fn match_job(&self, job: &JobContent, conn: &PooledConnection<SqliteConnectionManager>) -> bool{
        match self.user_id {
            Some(user_id) => {
                if user_id != job.submission.user_id {
                    return false;
                }
            }
            None => ()
        }
        match &self.user_name{
            Some(user_name) => {
                let user_list = query_users(conn).unwrap();
                let user_id = job.submission.user_id;
                if user_name != &user_list[user_id].name {
                    return false;
                }
            }
            None => ()
        }
        match self.contest_id {
            Some(contest_id) => {
                if contest_id != job.submission.contest_id {
                    return false;
                }
            }
            None => ()
        }
        match self.problem_id {
            Some(problem_id) => {
                if problem_id != job.submission.problem_id {
                    return false;
                }
            }
            None => ()
        }
        match &self.language {
            Some(language) => {
                if language != &job.submission.language {
                    return false;
                }
            }
            None => ()
        }
        match &self.state {
            Some(state) => {
                if state != &job.state {
                    return false;
                }
            }
            None => ()
        }
        match &self.result {
            Some(result) => {
                if result != &job.result {
                    return false;
                }
            }
            None => ()
        }
        let fmt = "%Y-%m-%dT%H:%M:%S%.3fZ";
        match &self.from {
            Some(from) => {
                let from_time = NaiveDateTime::parse_from_str(from, fmt)
                    .expect("failed to get time");
                let creat_time = NaiveDateTime::parse_from_str(&job.created_time, fmt)
                    .expect("failed to get time");
                if from_time > creat_time {
                    return false;
                }
            }
            None => ()
        }   
        match &self.to {
            Some(to) => {
                let to_time = NaiveDateTime::parse_from_str(to, fmt)
                    .expect("failed to get time");
                let creat_time = NaiveDateTime::parse_from_str(&job.created_time, fmt)
                    .expect("failed to get time");
                if to_time < creat_time {
                    return false;
                }
            }
            None => ()
        }   
        true
    }

}

#[get("/jobs")]
#[allow(unreachable_code)]
async fn get_jobs(
    request: web::Query<QueryJob>,
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Get jobs");
    let conn = pool.get().unwrap();
    let job_list = query_jobs(&conn).unwrap();
    let mut query_jobs: Vec<JobContent> = Vec::new();
    for job in &job_list {
        if request.match_job(job, &conn) {
            query_jobs.push(job.clone());
        }
    }
    HttpResponse::Ok().json(query_jobs)
}

#[get("/jobs/{jobId}")]
#[allow(unreachable_code)]
async fn get_jobs_id(
    job_id: web::Path<usize>,
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Get jobs {}",job_id);
    let conn = pool.get().unwrap();
    match query_job(&conn, *job_id) {
        Ok(job) => {
            HttpResponse::Ok().json(job)
        }
        Err(_) => HttpResponse::NotFound().json({
                    ErrorMessage{
                        code: (3),
                        reason: ("ERR_NOT_FOUND".to_string()),
                        message: ("Job ".to_string() + &job_id.to_string() + " not found.")
                    }
                })
    }
}

#[put("/jobs/{jobId}")]
#[allow(unreachable_code)]
async fn put_jobs(
    job_id: web::Path<usize>, 
    config: web::Data<Config>,
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Put jobs {}", job_id);
    let conn = pool.get().unwrap();
    match query_job(&conn, *job_id) {
        Ok(mut job) => {
            if job.state != "Finished" {
                return  HttpResponse::BadRequest().json({
                    ErrorMessage{
                        code: (2),
                        reason: ("ERR_INVALID_STATE".to_string()),
                        message: ("Job ".to_string() + &job_id.to_string() + " not finished.")
                    }
                });
            }
            else {
                job.state = "Queueing".to_string();
                // time
                let now: DateTime<Utc> = Utc::now();
                job.updated_time = now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
                job.result = "Waiting".to_string();
                job.score = 0.0;
                actix_web::rt::spawn(judge_job(*job_id, pool.clone(), config.problems.clone(), config.languages.clone()));
                return  HttpResponse::Ok().json(job);
            }
        }
        Err(_) => HttpResponse::NotFound().json({
                    ErrorMessage{
                        code: (3),
                        reason: ("ERR_NOT_FOUND".to_string()),
                        message: ("Job ".to_string() + &job_id.to_string() + " not found.")
                    }
                })
    }
    

}


#[delete("/jobs/{jobId}")]
#[allow(unreachable_code)]
async fn delete_jobs(
    job_id: web::Path<usize>,
    pool: web::Data<Pool<SqliteConnectionManager>>
) -> impl Responder {
    log::info!("Delete jobs {}",job_id);
    let conn = pool.get().unwrap();
    match query_job(&conn, *job_id) {
        Ok(job) => {
            if job.state != "Queueing" {
                HttpResponse::NotFound().json({
                    ErrorMessage{
                        code: (2),
                        reason: ("ERR_INVALID_STATE".to_string()),
                        message: ("Job ".to_string() + &job_id.to_string() + " not queuing.")
                    }
                })
            }else {
                HttpResponse::Ok().json({})
            }
        }
        Err(_) => HttpResponse::NotFound().json({
                    ErrorMessage{
                        code: (3),
                        reason: ("ERR_NOT_FOUND".to_string()),
                        message: ("Job ".to_string() + &job_id.to_string() + " not found.")
                    }
                })
    }

}


fn format_cases(v: &Vec<Case>) -> String {
    v.iter()
        .map(|x| format!("{}:{}:{}:{}:{}", x.id, x.result, x.time, x.memory, x.info))
        .collect::<Vec<String>>()
        .join(",")
}

fn parse_cases(s: String) -> Vec<Case> {
    s.split(',')
        .map(|x| {
            let v: Vec<&str> = x.split(':').collect();
            Case {
                id: usize::from_str(v[0]).unwrap(),
                result: v[1].to_string(),
                time: u64::from_str(v[2]).unwrap(),
                memory: u64::from_str(v[3]).unwrap(),
                info: v[4].to_string()
            }
        })
        .collect()
}

pub fn query_jobs(conn: &Connection) -> Result<Vec<JobContent>> {

    let mut stmt = conn.prepare("SELECT * FROM jobs")?;

    let rows = stmt.query_map([], |row| {
        Ok(JobContent {
            id: row.get(0)?,
            created_time: row.get(1)?,
            updated_time: row.get(2)?,
            submission: PostJob {
                source_code: row.get(3)?,
                language: row.get(4)?,
                user_id: row.get(5)?,
                contest_id: row.get(6)?,
                problem_id: row.get(7)?
            },
            state: row.get(8)?,
            result: row.get(9)?,
            score: row.get(10)?,
            cases: parse_cases(row.get(11)?)
        })
    })?;
    let jobs = rows.collect();
    jobs
}

pub fn query_job(conn: &Connection, id: usize) -> Result<JobContent> {
    let mut stmt = conn.prepare("SELECT * FROM jobs WHERE id = ?")?;
    let mut jobs = stmt.query_map(params![id], |row| {
        Ok(JobContent {
            id: row.get(0)?,
            created_time: row.get(1)?,
            updated_time: row.get(2)?,
            submission: PostJob {
                source_code: row.get(3)?,
                language: row.get(4)?,
                user_id: row.get(5)?,
                contest_id: row.get(6)?,
                problem_id: row.get(7)?
            },
            state: row.get(8)?,
            result: row.get(9)?,
            score: row.get(10)?,
            cases: parse_cases(row.get(11)?)
        })
    })?;
    jobs.next().unwrap_or(Err(rusqlite::Error::QueryReturnedNoRows))
}

fn insert_job(conn: &Connection, job: &JobContent) -> Result<()> {

    conn.execute("INSERT INTO jobs (
        id, 
        created_time,
        updated_time, 
        source_code, 
        language, 
        user_id, 
        contest_id, 
        problem_id, 
        state, 
        result, 
        score, 
        cases) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        params![job.id,
        job.created_time,
        job.updated_time,
        job.submission.source_code,
        job.submission.language,
        job.submission.user_id,
        job.submission.contest_id,
        job.submission.problem_id,
        job.state,
        job.result,
        job.score,
        format_cases(&job.cases)
    ])?;
    Ok(())
}

fn update_job(conn: &Connection, job: &JobContent) -> Result<()> {

    conn.execute("UPDATE jobs SET 
        created_time = ?, 
        updated_time = ?, 
        source_code = ?, 
        language = ?, user_id = ?, 
        contest_id = ?, 
        problem_id = ?, 
        state = ?, 
        result = ?, 
        score = ?, 
        cases = ? 
        WHERE id = ?",
        params![job.created_time,
        job.updated_time,
        job.submission.source_code,
        job.submission.language,
        job.submission.user_id,
        job.submission.contest_id,
        job.submission.problem_id,
        job.state,
        job.result,
        job.score,
        format_cases(&job.cases),
        job.id
    ])?;
    Ok(())
}