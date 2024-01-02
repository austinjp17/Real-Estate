Certainly! Here's the README file text formatted with Markdown:

---

# Redfin House Listing Scraper

## Overview

The **Redfin House Listing Scraper** is a program designed to fetch and manage house listing data from Redfin based on zipcode. It ensures that local data is initially read and subsequently updated on each run. If no local data is found, the program extracts and saves all available listings. The scraping process involves checking if each house is already in the features dataset. If present, only the price is taken, updating the price dataset. If not, all house features are gathered, and both datasets are updated.

## Data Structure

### Features Dataset

- Contains one entry per house.
- Includes information such as price, square footage, lot size, beds, baths, and location.

### Price Dataset

- Maintains a separate object for price due to its subject to change nature.
- Structured where each row represents an observation of a house's price on a given day.
- Allows for multiple entries per house, capturing changes in price over time.

## Usage

### Optional CLI Parameters

- **first_page**: Extracts only the first page of listings. Useful for debugging to reduce API calls.
  
- **force_refresh**: Disregards local data and updates both datasets for all listings.

### Example Command

```bash
$ ./redfin_scraper --zipcode 12345 --first_page --force_refresh
```

This command scrapes Redfin listings for the specified zipcode, extracts only the first page, and forces a refresh of all data.

## Dependencies

- Rust (version X.X.X)
- [Add any additional dependencies or libraries]

## Installation

1. Clone the repository:

```bash
$ git clone https://github.com/your_username/redfin-scraper.git
$ cd redfin-scraper
```

2. Build the project:

```bash
$ cargo build --release
```

3. Run the program:

```bash
$ ./target/release/redfin_scraper --zipcode 12345
```

## License

This project is licensed under the [MIT License](LICENSE).

---

Feel free to customize this template based on your actual implementation and any additional details specific to your project.