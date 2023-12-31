use listing_structs::ListingsContainer;
use tracing_subscriber;
use std::env;
use tracing::info;
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

    // Parse Args
    let args: Vec<String> = env::args().collect();
    println!("Args: {:?}", args);

    let mut force_refresh = false;
    let mut first_page_only = false;
    // let mut target_page = None;
    if args.contains(&String::from("force_refresh")) {
        force_refresh = true;
        info!("Data reset flag set");
    }
    if args.contains(&String::from("first_page")) {
        first_page_only = true;
        info!("First page only flag set");
    }

    let mut listings_container = ListingsContainer::new(force_refresh, first_page_only);
    listings_container.initialize_datasets();
    // println!("{:?}", listings_container.listing_history.get_columns());
    listings_container.homes_by_zip(77532).await;

    listings_container.print_data_head();
    listings_container.to_csv("out");


    

    // println!("{:?}", elems);

    
}
