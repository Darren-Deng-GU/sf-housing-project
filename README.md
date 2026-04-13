# Visualizing Housing in the San Francisco Bay Area

**PPOL 5206: Massive Data Fundamentals — Georgetown University, Spring 2026**

An analysis of 200,000+ building permits, land use records, and census data in the SF Bay Area, built on a scalable cloud pipeline with AWS S3, Snowflake, PySpark, and interactive Plotly visualizations.

## Project Structure

```
sf-housing-project/
├── code/
│   ├── 01_data_ingestion.py          # Download datasets + upload to AWS S3
│   ├── 02_snowflake_etl.sql          # Snowflake: create tables, load, transform
│   ├── 03_spark_analysis.py          # PySpark analysis (Databricks)
│   ├── 04_generate_visualizations.py # Generate all Plotly/Folium charts
│   └── requirements.txt             # Python dependencies
├── data/
│   ├── raw/                          # Downloaded CSVs (not in git, too large)
│   └── processed/                    # Exported from Snowflake for visualization
├── docs/                             # GitHub Pages website root
│   ├── index.html                    # Home page
│   ├── methods.html                  # Methods & pipeline walkthrough
│   ├── findings.html                 # Findings & interactive charts
│   ├── css/style.css                 # Stylesheet
│   └── charts/                       # Generated chart HTML files
└── README.md
```

## Quick Start

### Prerequisites

- Python 3.9+
- AWS account with S3 access
- Snowflake account
- Census API key ([get one here](https://api.census.gov/data/key_signup.html))

### Setup

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/sf-housing-project.git
cd sf-housing-project

# Install dependencies
pip install -r code/requirements.txt

# Set environment variables
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>
export CENSUS_API_KEY=<your-census-key>
```

### Run the Pipeline

```bash
# Step 1: Download data and upload to S3
python code/01_data_ingestion.py

# Step 2: Run Snowflake ETL
# → Open code/02_snowflake_etl.sql in Snowflake Worksheet
# → Execute sections sequentially
# → Export results from Section 6 as CSVs to data/processed/

# Step 3 (optional): Run Spark analysis on Databricks
# → Upload code/03_spark_analysis.py as a Databricks notebook

# Step 4: Generate visualizations
python code/04_generate_visualizations.py
# → Charts saved to docs/charts/

# Step 5: Deploy website
# → Push to GitHub with Pages enabled on the /docs folder
```

## Data Sources

| Dataset | Source | URL |
|---------|--------|-----|
| Building Permits | SF Open Data | https://data.sfgov.org/Housing-and-Buildings/Building-Permits/i98e-djp9 |
| Land Use 2023 | SF Open Data | https://data.sfgov.org/Housing-and-Buildings/San-Francisco-Land-Use/us3s-fp9q |
| House Price Index | FRED | https://fred.stlouisfed.org/series/ATNHPIUS41884Q |
| Census ACS 5-Year | U.S. Census Bureau | https://data.census.gov/ |

## Tech Stack

- **Storage:** AWS S3
- **ETL:** Snowflake (SQL)
- **Distributed Computing:** PySpark on Databricks
- **Visualization:** Plotly, Folium
- **Website:** GitHub Pages (static HTML)
- **Languages:** Python, SQL

## Team

- Darren Deng (sd1511@georgetown.edu)
- Marilyn Rutecki (mr1970@georgetown.edu)

## License

This project is for academic purposes. Data sourced from public datasets.
