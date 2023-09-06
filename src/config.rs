use serde::{Deserialize, Serialize};
use clap::Parser;
use std::fs::File;
use std::io::Read;

#[derive(Clone, Deserialize, Serialize)]
pub struct Config {
    server: Server,
    pub problems: Vec<Problem>,
    pub languages: Vec<Language>
}

#[derive(Clone, Deserialize, Serialize)]
struct Server {
    bind_address: Option<String>,
    bind_port: Option<i32>
}
#[derive(Clone, Deserialize, Serialize)]
pub struct Problem {
    pub id: usize,
    name: String,
    #[serde(rename = "type")]
    pub ty: String,
    misc: Option<Misc>,
    pub cases: Vec<Case>
}
impl Problem {
    pub fn new() -> Problem {
        Problem { 
            id: (0), 
            name: (String::new()), 
            ty: (String::new()), 
            misc: (None), 
            cases: (Vec::new()) 
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
pub struct Misc{

}

#[derive(Clone, Deserialize, Serialize)]
pub struct Case {
    pub score: f64,
    pub input_file: String,
    pub answer_file: String,
    /// The unit is us, 0 means no limit
    pub time_limit: u64,
    /// The unit is byte, 0 means no limit
    pub memory_limit: u64,
}

#[derive(Clone, Deserialize, Serialize)]
enum ProblemType{
    Standard,
    Strict,
    Spj,
    DynamicRanking
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Language {
    pub name: String,
    pub file_name: String,
    pub command: Vec<String>,
}

impl Language {
    pub fn new() -> Language {
        Language { 
            name: (String::new()), 
            file_name: (String::new()),
            command: (Vec::new()) 
        }
    }
}

/// the Cli struct is for command lines args
#[derive(Parser)]
#[command(name = "oj")]
#[command(author = "ldt20 <ldt20@mails.tsinghua.edu.cn>")]
#[command(version = "1.0")]
#[command(about = "Attention is all you need", long_about = None)]
pub struct Cli {
    /// config json
    #[arg(short, long)]
    pub config: String,
    #[arg(short = 'f', long = "flush-data")]
    pub flush_data: bool

}
impl Cli {
    pub fn init_config(&self) -> std::io::Result<Config> {
        let mut file = File::open(&self.config)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let config: Config = serde_json::from_str(&contents)?;
        Ok(config)
    }
}