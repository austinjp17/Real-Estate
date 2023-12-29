use scraper::{Html, Selector};
use anyhow::Result;
use tracing::{info, trace, warn};
use polars::prelude::*;
use crate::listing_structs::ListingsContainer;

pub(crate) async fn request(target_url: &str) -> Result<Html> {

    // Required ScraperAPI request params
    let params = [
        ("url", target_url),
        ("api_key", "0861bae719981ddf7ae64ddfcb5193ad")
    ];

    let scraper_url = "https://api.scraperapi.com/";
    let scraper_url = reqwest::Url::parse_with_params(scraper_url, params).unwrap();

    // Send Request
    let response = reqwest::get(scraper_url).await?;
    info!("Response Code: {}", response.status());

    // Convert resp to HTML str
    let response_str = response.text().await?;

    let parsed = Html::parse_document(&response_str);

    Ok(parsed)
}

impl ListingsContainer {
    pub(crate) fn initialize_dataset(&mut self) {
            
        // If local data exists, pull it in
        if let Ok(local_data) = CsvReader::from_path("out/listing_dataset.csv"){
            if let Ok(parsed) =local_data.has_header(true).finish() {
                self.data = parsed;
                info!("Local data initialized, shape: {:?}", self.data.shape())
            }
        } 
        // Else assign empty col dataframe
        else {
            info!("No local data found");
            let price_col = Series::new_empty("price", &DataType::UInt32);
            let beds_col = Series::new_empty("beds", &DataType::Int32);
            let baths_col = Series::new_empty("baths", &DataType::Int32);
            let sqft_col = Series::new_empty("sqft", &DataType::UInt32);
            let lot_size_col = Series::new_empty("lot_size", &DataType::Int32);
            let street_col = Series::new_empty("street", &DataType::Utf8);
            let apt_col = Series::new_empty("apt", &DataType::Int32);
            let city_col = Series::new_empty("city", &DataType::Utf8);
            let state_col = Series::new_empty("state", &DataType::Utf8);
            let zip_col = Series::new_empty("zip", &DataType::UInt32);
            let addr_str_col = Series::new_empty("addr_str", &DataType::Utf8);

            let cols = vec![price_col, beds_col, baths_col, sqft_col, lot_size_col, street_col, apt_col, city_col, state_col, zip_col, addr_str_col];
            self.data = DataFrame::new(cols).expect("Failed to create empty default");
        }

    }
}

// Extraction Helpers
