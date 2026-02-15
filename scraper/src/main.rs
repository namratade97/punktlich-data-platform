use chrono::{Utc, NaiveDateTime};
use quick_xml::de::from_str;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use arrow::array::{StringArray, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;

use std::env;
use duckdb::Connection;


#[derive(Debug)]


struct Departure {
    trip_id: String,
    train: String,
    destination: String,
    path: String,
    scheduled_time: String,
    platform: String, // NEW
    delay: i32,
    disturbances: String, 
}

#[derive(Debug, Deserialize, Clone)]
struct Timetable {
    #[serde(rename = "s", default)]
    stops: Vec<Stop>,
}

#[derive(Debug, Deserialize, Clone)]

struct Stop {
    #[serde(rename = "@id")] 
    id: String,
    #[serde(rename = "m", default)] 
    messages: Vec<Message>, 
    #[serde(rename = "ar")]
    arrivals: Option<TrainEvent>,
    #[serde(rename = "dp")]
    departures: Option<TrainEvent>,
    #[serde(rename = "tl")]
    train_line: Option<TrainLine>,
}

#[derive(Debug, Deserialize, Clone)]
struct Message {
    #[serde(rename = "@t")] 
    msg_type: Option<String>,      
    #[serde(rename = "@cat")] 
    category: Option<String>,     
    #[serde(rename = "@from")] 
    valid_from: Option<String>,
    #[serde(rename = "@to")] 
    valid_to: Option<String>,
    #[serde(rename = "@ts")] 
    timestamp: Option<String>,    
    #[serde(rename = "@ts-tts")] 
    ts_tts: Option<String>,       
}



#[derive(Debug, Deserialize, Clone)]
struct TrainEvent {
    #[serde(rename = "@ct")]  actual_time: Option<String>,
    #[serde(rename = "@pt")]  planned_time: Option<String>,
    #[serde(rename = "@ppth")] planned_path: Option<String>,
    #[serde(rename = "@cpth")] changed_path: Option<String>,
    #[serde(rename = "@pp")]   platform: Option<String>,
    #[serde(rename = "@l")]    line: Option<String>,
    #[serde(rename = "@c")]    category: Option<String>,
    #[serde(rename = "@n")]    number: Option<String>,
    #[serde(rename = "m", default)] messages: Vec<Message>,
}

impl TrainEvent {
    fn path(&self) -> Option<String> {
        self.changed_path.clone().or(self.planned_path.clone())
    }
}

#[derive(Debug, Deserialize, Clone)]
struct TrainLine {
    #[serde(rename = "@c")] // Category (ICE)
    category: Option<String>,
    #[serde(rename = "@n")] // Number (147)
    number: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let limit = 500;

    let conn = Connection::open("../data/dbt.duckdb")?;
    let count: i32 = conn.query_row(
        "SELECT count(*) FROM silver_departures", 
        [], 
        |row| row.get::<_, String>(0)
    ).unwrap_or(0);

    if count >= limit {
        println!("Limit of {} rows reached. Stopping ingestion.", limit);
        return Ok(());
    }

    println!("Current count is {}. Starting burst ingestion...", count);



    let client = Client::new();
    let station_id = "8011160"; // Berlin Hbf
    let client_id = env::var("DB_CLIENT_ID")
        .expect("DB_CLIENT_ID not set in GitHub Secrets");
    let api_key = env::var("DB_API_KEY")
        .expect("DB_API_KEY not set in GitHub Secrets");

    // 2. Ensure the log and data directories exist
    std::fs::create_dir_all("logs")?;
    std::fs::create_dir_all("../data/bronze")?; 
    let heartbeat_file = "logs/heartbeat.log";

    println!("Starting Enriched DB Ingestion Service for: {}", station_id);

    println!("Building Plan Lookup Map...");
    
    let mut plan_map = fetch_plan_map(&client, station_id, &client_id, &api_key).await?;
    // let mut last_refresh_hour = Utc::now().hour();
    println!("Lookup Map ready with {} train definitions.", plan_map.len());

    // let json_map = serde_json::to_string_pretty(&plan_map)?;
    // let mut file = File::create("plan_map.json")?;
    // file.write_all(json_map.as_bytes())?;
    // println!("Lookup map exported to plan_map.json");

    // loop {
    //     let now = Utc::now();
        
    //     if now.hour() != last_refresh_hour {
    //         println!("Hour changed! Refreshing Plan Lookup Map...");
    //         if let Ok(new_map) = fetch_plan_map(&client, station_id, client_id, api_key).await {
    //             plan_map = new_map;
    //             last_refresh_hour = now.hour();
    //         }
    //     }

        match fetch_departures(&client, station_id, &client_id, &api_key, &plan_map).await {
            Ok(departures) => {
                println!("Fetched {} departures", departures.len());

                if !departures.is_empty() {
                    if let Err(e) = write_parquet(&departures) {
                        eprintln!("Failed to write Parquet: {}", e);
                    } else {
                        println!("Enriched Parquet file written.");
                    }
                }

                let now = Utc::now();
                let log_line = format!("{} | Fetched {} departures\n", now, departures.len());
                if let Ok(mut file) = OpenOptions::new().append(true).create(true).open(heartbeat_file) {
                    let _ = file.write_all(log_line.as_bytes());
                }
            }
            Err(e) => {
                eprintln!("Failed to fetch departures: {}", e);
            }
        }
        // sleep(Duration::from_secs(60)).await;
    // }
    Ok(())
}

async fn fetch_plan_map(
    client: &Client,
    station_id: &str,
    client_id: &str,
    api_key: &str,
) -> Result<HashMap<String, String>, Box<dyn std::error::Error>> {
    let mut map = HashMap::new();
    let now = Utc::now();
    
    for i in -2..6 {
        let offset = chrono::Duration::try_hours(i).unwrap_or_else(|| chrono::Duration::zero());
        let target_time = now + offset;
        
        let date = target_time.format("%y%m%d").to_string();
        let hour = target_time.format("%H").to_string();
        
        println!("Fetching plan for {} hour: {}", if i < 0 { "past" } else { "future/current" }, hour);

   
        let url = format!(
    "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/{}/{}/{}?sub=yes", // Added ?sub=yes
    station_id, date, hour
);

        let resp = client.get(&url)
            .header("DB-Client-Id", client_id)
            .header("DB-Api-Key", api_key)
            .header("Accept", "application/xml")
            .send().await?;

        if resp.status().is_success() {
            let text = resp.text().await?;
            let timetable: Timetable = from_str(&text)?;
            

            

            for stop in timetable.stops {
                    let mut train_name = None;

                    let event = stop.departures.as_ref().or(stop.arrivals.as_ref());
                    if let Some(ev) = event {
                        if let Some(l) = &ev.line {
                            if !l.is_empty() {
                                train_name = Some(l.clone());
                            }
                        }
                    }

                    if train_name.is_none() {
                        if let Some(tl) = &stop.train_line {
                            match (&tl.category, &tl.number) {
                                (Some(c), Some(n)) => train_name = Some(format!("{} {}", c, n)),
                                (Some(c), None) => train_name = Some(c.clone()),
                                _ => {}
                            }
                        }
                    }

                    let final_name = train_name.unwrap_or_else(|| {
                        format!("ID:{}", &stop.id.chars().take(8).collect::<String>())
                    });

                    map.insert(stop.id, final_name);
                }
        } else {
            eprintln!(" Could not fetch hour {}: Status {}", hour, resp.status());
        }
        
     
        sleep(Duration::from_millis(500)).await;
    }
    Ok(map)
}


async fn fetch_departures(
    client: &Client,
    station_id: &str,
    client_id: &str,
    api_key: &str,
    plan_map: &HashMap<String, String>,
) -> Result<Vec<Departure>, Box<dyn std::error::Error>> {
    let url = format!(
        "https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{}?sub=yes",
        station_id
    );

    let resp = client.get(&url)
        .header("DB-Client-Id", client_id)
        .header("DB-Api-Key", api_key)
        .header("Accept", "application/xml")
        .send().await?;

    let text = resp.text().await?;
    let timetable: Timetable = from_str(&text)?;
    let mut flat_departures = vec![];

    for stop in timetable.stops {
        if let Some(dp) = &stop.departures {
            
            let raw_time = dp.actual_time.clone().unwrap_or_else(|| {
                dp.planned_time.clone().unwrap_or_default()
            });
            let formatted_time = format_db_time(&raw_time);

            let delay_min = if let (Some(p), Some(a)) = (&dp.planned_time, &dp.actual_time) {
                calculate_delay_minutes(p, a)
            } else {
                0
            };

            
            let train_name = {
                let change_name = match (&dp.line, &dp.category, &dp.number) {
                    (Some(l), _, _) if !l.is_empty() => Some(l.clone()),
                    (_, Some(c), Some(n)) => Some(format!("{} {}", c, n)),
                    _ => None,
                };

                change_name
                    .or_else(|| plan_map.get(&stop.id).cloned())
                    .or_else(|| {
                        stop.train_line.as_ref().and_then(|tl| {
                            match (&tl.category, &tl.number) {
                                (Some(c), Some(n)) => Some(format!("{} {}", c, n)),
                                (Some(c), None) => Some(c.clone()),
                                _ => None,
                            }
                        })
                    })
                    .unwrap_or_else(|| "Unknown".to_string())
            };

            
            let arrival_path = stop.arrivals.as_ref()
                .and_then(|ar| ar.path())
                .unwrap_or_default();

            let departure_path = dp.path().unwrap_or_default(); 

            let full_route = if arrival_path.is_empty() && departure_path.is_empty() {
                "Berlin Hbf".to_string()
            } else if arrival_path.is_empty() {
                format!("Berlin Hbf|{}", departure_path)
            } else if departure_path.is_empty() {
                format!("{}|Berlin Hbf", arrival_path)
            } else {
                format!("{}|Berlin Hbf|{}", arrival_path, departure_path)
            };

            let dest = departure_path.split('|')
                .last()
                .map(|s: &str| s.to_string())
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "Berlin Hbf".to_string());

            
            let mut categories: Vec<String> = stop.messages.iter()
                .chain(dp.messages.iter())
                .filter_map(|m| m.category.clone())
                .collect();
            categories.sort();
            categories.dedup();
            let joined_disturbances = categories.join("|");

            
            flat_departures.push(Departure {
                trip_id: stop.id.clone(),
                train: train_name,
                destination: dest,
                path: full_route, 
                scheduled_time: formatted_time,
                platform: dp.platform.clone().unwrap_or_else(|| "--".to_string()),
                delay: delay_min,
                disturbances: joined_disturbances,
            });
        }
    }
    Ok(flat_departures)
}



fn write_parquet(departures: &Vec<Departure>) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Create the data arrays
    let trip_id_array = Arc::new(StringArray::from(departures.iter().map(|d| d.trip_id.as_str()).collect::<Vec<&str>>()));
    let train_array = Arc::new(StringArray::from(departures.iter().map(|d| d.train.as_str()).collect::<Vec<&str>>()));
    let dest_array = Arc::new(StringArray::from(departures.iter().map(|d| d.destination.as_str()).collect::<Vec<&str>>()));
    let path_array = Arc::new(StringArray::from(departures.iter().map(|d| d.path.as_str()).collect::<Vec<&str>>()));
    let time_array = Arc::new(StringArray::from(departures.iter().map(|d| d.scheduled_time.as_str()).collect::<Vec<&str>>()));
    let delay_array = Arc::new(Int32Array::from(departures.iter().map(|d| Some(d.delay)).collect::<Vec<Option<i32>>>()));
    let dist_array = Arc::new(StringArray::from(departures.iter().map(|d| d.disturbances.as_str()).collect::<Vec<&str>>()));
    let platform_array = Arc::new(StringArray::from(departures.iter().map(|d| d.platform.as_str()).collect::<Vec<&str>>()));

    // 2. Define the Schema 
    let schema = Arc::new(Schema::new(vec![
        Field::new("trip_id", DataType::Utf8, false),
        Field::new("train", DataType::Utf8, false),
        Field::new("destination", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("scheduled_time", DataType::Utf8, false),
        Field::new("platform", DataType::Utf8, true),
        Field::new("delay", DataType::Int32, true),
        Field::new("service_notices", DataType::Utf8, true),
    ]));

    // 3. Create the Batch
    let batch = RecordBatch::try_new(
        schema.clone(), 
        vec![
            trip_id_array, train_array, dest_array, path_array, 
            time_array, platform_array, delay_array, dist_array
        ]
    )?;

    // 4. Write to file
    // let path = format!("../data/bronze/bronze_{}.parquet", Utc::now().timestamp());
    let path = format!(
            "../data/bronze/bronze_{}.parquet",
            Utc::now().format("%Y%m%d_%H%M%S")
        );
    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

fn calculate_delay_minutes(planned: &str, actual: &str) -> i32 {
    let format = "%y%m%d%H%M";
    let p_time = NaiveDateTime::parse_from_str(planned, format);
    let a_time = NaiveDateTime::parse_from_str(actual, format);

    match (p_time, a_time) {
        (Ok(p), Ok(a)) => (a - p).num_minutes() as i32,
        _ => 0,
    }
}

fn format_db_time(raw: &str) -> String {
    if raw.len() != 10 { return raw.to_string(); }
    // Input: 2602101730 (YYMMDDHHMM)
    match NaiveDateTime::parse_from_str(raw, "%y%m%d%H%M") {
        Ok(dt) => dt.format("%Y-%m-%d %H:%M:%S").to_string(),
        Err(_) => raw.to_string(),
    }
}