"""
01_data_ingestion.py
====================
Phase 1: Download raw datasets and upload to AWS S3.

Datasets:
  1. SF Building Permits        → CSV from SF Open Data API
  2. SF Land Use 2023           → CSV from SF Open Data API
  3. House Price Index (HPI)    → CSV from FRED
  4. Census ACS 5-Year          → JSON from Census Bureau API

Usage:
  pip install boto3 requests pandas
  export AWS_ACCESS_KEY_ID=<your-key>
  export AWS_SECRET_ACCESS_KEY=<your-secret>
  export CENSUS_API_KEY=<your-census-key>
  python 01_data_ingestion.py
"""

import os
import json
import requests
import pandas as pd
import boto3
from pathlib import Path

# ──────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

S3_BUCKET = "sf-housing-project"          # ← change to your bucket name
S3_PREFIX = "raw/"                         # folder inside bucket
AWS_REGION = "us-east-1"                   # ← change if needed

CENSUS_API_KEY = os.environ.get("CENSUS_API_KEY", "")

# SF Open Data endpoints (Socrata SODA API)
PERMITS_URL = (
    "https://data.sfgov.org/api/views/i98e-djp9/rows.csv"
    "?accessType=DOWNLOAD"
)
LANDUSE_URL = (
    "https://data.sfgov.org/api/views/us3s-fp9q/rows.csv"
    "?accessType=DOWNLOAD"
)
# FRED – All-Transactions HPI for SF-Oakland-Hayward MSA
HPI_URL = (
    "https://fred.stlouisfed.org/graph/fredgraph.csv"
    "?bgcolor=%23e1e9f0&chart_type=line&drp=0"
    "&fo=open%20sans&graph_bgcolor=%23ffffff"
    "&height=450&mode=fred&recession_bars=on"
    "&txtcolor=%23444444&ts=12&tts=12&width=1168"
    "&nt=0&thu=0&trc=0&show_legend=yes"
    "&show_axis_titles=yes&show_tooltip=yes"
    "&id=ATNHPIUS41884Q&scale=left"
    "&cosd=1975-01-01&coed=2024-10-01"
    "&line_color=%234572a7&link_values=false"
    "&line_style=solid&mark_type=none&mw=3"
    "&lw=2&ost=-99999&oet=99999&mma=0"
    "&fml=a&fq=Quarterly&fam=avg"
    "&fgst=lin&fgsnd=2020-02-01&line_index=1"
    "&transformation=lin&vintage_date=2025-04-01"
    "&revision_date=2025-04-01&nd=1975-01-01"
)

# Census ACS 5-year – Bay Area 9 counties
BAY_AREA_FIPS = {
    "001": "Alameda",
    "013": "Contra Costa",
    "041": "Marin",
    "055": "Napa",
    "075": "San Francisco",
    "081": "San Mateo",
    "085": "Santa Clara",
    "095": "Solano",
    "097": "Sonoma",
}


# ──────────────────────────────────────────────
# Helper functions
# ──────────────────────────────────────────────
def download_file(url: str, dest: Path, label: str) -> Path:
    """Download a file with progress reporting."""
    print(f"⬇  Downloading {label} ...")
    resp = requests.get(url, stream=True, timeout=300)
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    downloaded = 0
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=1024 * 256):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"\r   {pct:5.1f}%  ({downloaded:,} / {total:,} bytes)", end="")
    print(f"\n   ✓ Saved to {dest}  ({dest.stat().st_size:,} bytes)")
    return dest


def upload_to_s3(local_path: Path, s3_key: str) -> None:
    """Upload a local file to S3."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    print(f"☁  Uploading {local_path.name} → s3://{S3_BUCKET}/{s3_key} ...")
    s3.upload_file(str(local_path), S3_BUCKET, s3_key)
    print(f"   ✓ Upload complete.")


# ──────────────────────────────────────────────
# 1. Building Permits
# ──────────────────────────────────────────────
def ingest_permits():
    dest = RAW_DIR / "Building_Permits.csv"
    if dest.exists():
        print(f"⏭  Permits file already exists at {dest}, skipping download.")
    else:
        download_file(PERMITS_URL, dest, "SF Building Permits")
    upload_to_s3(dest, S3_PREFIX + "Building_Permits.csv")
    return dest


# ──────────────────────────────────────────────
# 2. Land Use
# ──────────────────────────────────────────────
def ingest_landuse():
    dest = RAW_DIR / "SF_Land_Use_2023.csv"
    if dest.exists():
        print(f"⏭  Land Use file already exists at {dest}, skipping download.")
    else:
        download_file(LANDUSE_URL, dest, "SF Land Use 2023")
    upload_to_s3(dest, S3_PREFIX + "SF_Land_Use_2023.csv")
    return dest


# ──────────────────────────────────────────────
# 3. House Price Index
# ──────────────────────────────────────────────
def ingest_hpi():
    dest = RAW_DIR / "ATNHPIUS41884Q.csv"
    if dest.exists():
        print(f"⏭  HPI file already exists at {dest}, skipping download.")
    else:
        download_file(HPI_URL, dest, "House Price Index (FRED)")
    upload_to_s3(dest, S3_PREFIX + "ATNHPIUS41884Q.csv")
    return dest


# ──────────────────────────────────────────────
# 4. Census ACS 5-Year
# ──────────────────────────────────────────────
def ingest_census():
    """
    Pull ACS 5-year estimates for Bay Area census tracts.
    Variables:
      B25064_001 = Median gross rent
      B19013_001 = Median household income
      B03002_001 = Total population
      B03002_003 = White alone (non-Hispanic)
    """
    dest = RAW_DIR / "census_acs_bayarea.csv"
    if dest.exists():
        print(f"⏭  Census file already exists at {dest}, skipping API call.")
        upload_to_s3(dest, S3_PREFIX + "census_acs_bayarea.csv")
        return dest

    if not CENSUS_API_KEY:
        print("⚠  CENSUS_API_KEY not set. Skipping Census data pull.")
        print("   Get a key at: https://api.census.gov/data/key_signup.html")
        return None

    print("⬇  Fetching Census ACS 5-Year data for Bay Area tracts ...")
    variables = "B25064_001E,B19013_001E,B03002_001E,B03002_003E"
    all_rows = []

    for fips, county_name in BAY_AREA_FIPS.items():
        url = (
            f"https://api.census.gov/data/2023/acs/acs5"
            f"?get=NAME,{variables}"
            f"&for=tract:*"
            f"&in=state:06&in=county:{fips}"
            f"&key={CENSUS_API_KEY}"
        )
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        header = data[0]
        for row in data[1:]:
            all_rows.append(row)
        print(f"   ✓ {county_name}: {len(data)-1} tracts")

    df = pd.DataFrame(all_rows, columns=header)
    df.rename(columns={
        "B25064_001E": "median_rent",
        "B19013_001E": "median_income",
        "B03002_001E": "pop_total",
        "B03002_003E": "pop_white",
    }, inplace=True)

    # Create GEOID for joining (state + county + tract)
    df["GEOID"] = df["state"] + df["county"] + df["tract"]

    df.to_csv(dest, index=False)
    print(f"   ✓ Census data saved to {dest}  ({len(df)} tracts)")
    upload_to_s3(dest, S3_PREFIX + "census_acs_bayarea.csv")
    return dest


# ──────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("  SF Bay Area Housing — Data Ingestion Pipeline")
    print("=" * 60)
    print()

    ingest_permits()
    print()
    ingest_landuse()
    print()
    ingest_hpi()
    print()
    ingest_census()

    print()
    print("=" * 60)
    print("  ✅  All datasets ingested and uploaded to S3.")
    print(f"  S3 location: s3://{S3_BUCKET}/{S3_PREFIX}")
    print("=" * 60)
