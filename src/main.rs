use listing_structs::ListingsContainer;
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
    
    let mut listings_container = ListingsContainer::default();
    listings_container.initialize_dataset();

    listings_container.homes_by_zip(77532).await;

    // listings_container.to_csv("out.csv");


    

    // println!("{:?}", elems);

    
}
