use std::{collections::VecDeque, fs::File};


use polars::io::csv::CsvWriter;
use tracing::{info, trace, warn};
use tracing_subscriber;



mod redfin;
mod listing_structs;
mod helpers;


#[tracing::instrument]

/// Sends request to ScraperAPI for target_url
/// Returns html str


#[tokio::main]
async fn main() {
    // install global collector configured based on RUST_LOG env var.
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    

    
    // println!("{:?}", listings_container.data.shape());
    let mut listings_container = redfin::homes_by_zip(77532).await;
    listings_container.to_csv("out.csv");


    

    // println!("{:?}", elems);

    
}
