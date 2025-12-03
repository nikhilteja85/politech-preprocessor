#!/usr/bin/env python3
"""
Stage 0: Prepare geometry + ACS inputs for any US state.

This script downloads TIGER shapefiles and ACS data for a specified state.

Usage:
    python run_stage0.py <STATE_CODE> [options]
    
Examples:
    python run_stage0.py AZ
    python run_stage0.py CA --acs-year 2022
    python run_stage0.py TX --census-year 2020 --acs-year 2023

Outputs:
  inputs/tiger_<CENSUS_YEAR>/<state>_bg/...
  inputs/tiger_<CENSUS_YEAR>/<state>_tabblock20/...
  inputs/acs_<ACS_YEAR>/<state>_bg_race_<ACS_YEAR>.csv
  inputs/acs_<ACS_YEAR>/<state>_bg_income_<ACS_YEAR>.csv
"""

import os
import io
import zipfile
import requests
import pandas as pd
import geopandas as gpd
from dotenv import load_dotenv
from common import (
    setup_argument_parser, 
    validate_state_setup, 
    get_state_paths, 
    print_state_info
)

# Load environment variables
load_dotenv()


def download_and_unzip(url: str, out_dir: str) -> str:
    """Download a TIGER zip file and extract into out_dir."""
    print(f"Downloading {url}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(out_dir)

    print(f"Extracted to {out_dir}")
    return out_dir


def get_tabblock20_shapefile(state_abbr: str, state_fips: str, census_year: int, tiger_dir: str) -> gpd.GeoDataFrame:
    """Return GeoDataFrame for 2020 tabulation blocks (PL geometry base)."""
    target_dir = os.path.join(tiger_dir, f"{state_abbr}_tabblock20")
    os.makedirs(target_dir, exist_ok=True)

    tabblock20_base = "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20"
    zip_name = f"tl_{census_year}_{state_fips}_tabblock20.zip"
    url = f"{tabblock20_base}/{zip_name}"
    download_and_unzip(url, target_dir)

    shp = [f for f in os.listdir(target_dir) if f.endswith(".shp")][0]
    shp_path = os.path.join(target_dir, shp)

    print(f"Loading blocks from {shp_path}")
    gdf = gpd.read_file(shp_path)
    print("Blocks:", len(gdf))
    return gdf


def get_bg_shapefile(state_abbr: str, state_fips: str, census_year: int, tiger_dir: str) -> gpd.GeoDataFrame:
    """Return GeoDataFrame for 2020 block groups (BG geometry base)."""
    target_dir = os.path.join(tiger_dir, f"{state_abbr}_bg")
    os.makedirs(target_dir, exist_ok=True)

    bg_base = "https://www2.census.gov/geo/tiger/TIGER2020/BG"
    zip_name = f"tl_{census_year}_{state_fips}_bg.zip"
    url = f"{bg_base}/{zip_name}"
    download_and_unzip(url, target_dir)

    shp = [f for f in os.listdir(target_dir) if f.endswith(".shp")][0]
    shp_path = os.path.join(target_dir, shp)

    print(f"Loading block groups from {shp_path}")
    gdf = gpd.read_file(shp_path)
    print("Block groups:", len(gdf))
    return gdf


def fetch_acs_blockgroups_for_state(bg_gdf: gpd.GeoDataFrame,
                                    year: int,
                                    variables: list[str],
                                    census_api_key: str) -> pd.DataFrame:
    """
    Fetch ACS 5-year data at block-group level for this state.

    We must loop counties:
      for=block group:*&in=state:04 county:001
    """
    base_url = f"https://api.census.gov/data/{year}/acs/acs5"
    counties = sorted(bg_gdf["COUNTYFP"].unique())
    state_fips = bg_gdf["STATEFP"].iloc[0]

    rows = []
    var_str = ",".join(["GEO_ID"] + variables)

    for c in counties:
        params = {
            "get": var_str,
            "for": "block group:*",
            "in": f"state:{state_fips} county:{c}",
            "key": census_api_key,
        }
        print(f"  Fetching county {c} ...")
        resp = requests.get(base_url, params=params)
        resp.raise_for_status()
        data = resp.json()
        header = data[0]
        for row in data[1:]:
            rows.append(dict(zip(header, row)))

    df = pd.DataFrame(rows)
    # last 12 digits of GEO_ID is the block-group GEOID
    df["GEOID"] = df["GEO_ID"].str[-12:]

    for v in variables:
        df[v] = pd.to_numeric(df[v], errors="coerce")
    return df


def process_race_data(bg_gdf: gpd.GeoDataFrame, acs_year: int, census_api_key: str, acs_dir: str, state_abbr: str) -> str:
    """Process and save ACS race data."""
    race_vars = [
        "B03002_001E",  # total
        "B03002_003E",  # NH white
        "B03002_004E",  # NH black
        "B03002_005E",  # NH AIAN
        "B03002_006E",  # NH Asian
        "B03002_007E",  # NH NHPI
        "B03002_008E",  # NH other
        "B03002_009E",  # NH 2+ races
        "B03002_012E",  # Hispanic (any race)
    ]
    print("\n=== Fetching ACS race (B03002) ===")
    df_race = fetch_acs_blockgroups_for_state(bg_gdf, acs_year, race_vars, census_api_key)

    # Create year suffix for column names
    year_suffix = str(acs_year)[-2:]

    # Rename and derive POP-style columns
    df_race = df_race.rename(columns={
        "B03002_001E": f"TOT_POP{year_suffix}_RAW",
        "B03002_003E": f"WHT_NHSP{year_suffix}",
        "B03002_004E": f"BLK_NHSP{year_suffix}",
        "B03002_005E": f"AIA_NHSP{year_suffix}",
        "B03002_006E": f"ASN_NHSP{year_suffix}",
        "B03002_007E": f"HPI_NHSP{year_suffix}",
        "B03002_008E": f"OTH_NHSP{year_suffix}",
        "B03002_009E": f"2OM_NHSP{year_suffix}",
        "B03002_012E": f"HSP_POP{year_suffix}",
    })

    # Convert NH race to POP-style fields
    df_race[f"WHT_POP{year_suffix}"] = df_race[f"WHT_NHSP{year_suffix}"]
    df_race[f"BLK_POP{year_suffix}"] = df_race[f"BLK_NHSP{year_suffix}"]
    df_race[f"AIA_POP{year_suffix}"] = df_race[f"AIA_NHSP{year_suffix}"]
    df_race[f"ASN_POP{year_suffix}"] = df_race[f"ASN_NHSP{year_suffix}"]
    df_race[f"HPI_POP{year_suffix}"] = df_race[f"HPI_NHSP{year_suffix}"]
    df_race[f"OTH_POP{year_suffix}"] = df_race[f"OTH_NHSP{year_suffix}"]
    df_race[f"2OM_POP{year_suffix}"] = df_race[f"2OM_NHSP{year_suffix}"]

    df_race[f"NHSP_POP{year_suffix}"] = (
        df_race[f"WHT_POP{year_suffix}"] + df_race[f"BLK_POP{year_suffix}"] + df_race[f"AIA_POP{year_suffix}"] +
        df_race[f"ASN_POP{year_suffix}"] + df_race[f"HPI_POP{year_suffix}"] + df_race[f"OTH_POP{year_suffix}"] +
        df_race[f"2OM_POP{year_suffix}"]
    )
    df_race[f"TOT_POP{year_suffix}"] = df_race[f"HSP_POP{year_suffix}"] + df_race[f"NHSP_POP{year_suffix}"]

    race_keep = [
        "GEOID",
        f"TOT_POP{year_suffix}", f"HSP_POP{year_suffix}", f"NHSP_POP{year_suffix}",
        f"WHT_POP{year_suffix}", f"BLK_POP{year_suffix}", f"AIA_POP{year_suffix}", f"ASN_POP{year_suffix}",
        f"HPI_POP{year_suffix}", f"OTH_POP{year_suffix}", f"2OM_POP{year_suffix}",
    ]
    df_race_out = df_race[race_keep]

    race_csv_path = os.path.join(acs_dir, f"{state_abbr}_bg_race_{acs_year}.csv")
    df_race_out.to_csv(race_csv_path, index=False)
    print(f"Saved ACS race BG file: {race_csv_path}")
    return race_csv_path


def process_income_data(bg_gdf: gpd.GeoDataFrame, acs_year: int, census_api_key: str, acs_dir: str, state_abbr: str) -> str:
    """Process and save ACS income data."""
    income_vars = [
        "B19001_001E",
        "B19001_002E", "B19001_003E", "B19001_004E", "B19001_005E",
        "B19001_006E", "B19001_007E", "B19001_008E", "B19001_009E",
        "B19001_010E", "B19001_011E", "B19001_012E", "B19001_013E",
        "B19001_014E", "B19001_015E", "B19001_016E", "B19001_017E",
    ]
    print("\n=== Fetching ACS income (B19001) ===")
    df_inc = fetch_acs_blockgroups_for_state(bg_gdf, acs_year, income_vars, census_api_key)

    # Create year suffix for column names
    year_suffix = str(acs_year)[-2:]

    df_inc = df_inc.rename(columns={
        "B19001_001E": f"TOT_HOUS{year_suffix}",
        "B19001_002E": f"LESS_10K{year_suffix}",
        "B19001_003E": f"10K_15K{year_suffix}",
        "B19001_004E": f"15K_20K{year_suffix}",
        "B19001_005E": f"20K_25K{year_suffix}",
        "B19001_006E": f"25K_30K{year_suffix}",
        "B19001_007E": f"30K_35K{year_suffix}",
        "B19001_008E": f"35K_40K{year_suffix}",
        "B19001_009E": f"40K_45K{year_suffix}",
        "B19001_010E": f"45K_50K{year_suffix}",
        "B19001_011E": f"50K_60K{year_suffix}",
        "B19001_012E": f"60K_75K{year_suffix}",
        "B19001_013E": f"75K_100K{year_suffix}",
        "B19001_014E": f"100_125K{year_suffix}",
        "B19001_015E": f"125_150K{year_suffix}",
        "B19001_016E": f"150_200K{year_suffix}",
        "B19001_017E": f"200K_MOR{year_suffix}",
    })

    inc_keep = [
        "GEOID",
        f"TOT_HOUS{year_suffix}",
        f"LESS_10K{year_suffix}", f"10K_15K{year_suffix}", f"15K_20K{year_suffix}", f"20K_25K{year_suffix}",
        f"25K_30K{year_suffix}", f"30K_35K{year_suffix}", f"35K_40K{year_suffix}", f"40K_45K{year_suffix}",
        f"45K_50K{year_suffix}", f"50K_60K{year_suffix}", f"60K_75K{year_suffix}", f"75K_100K{year_suffix}",
        f"100_125K{year_suffix}", f"125_150K{year_suffix}", f"150_200K{year_suffix}", f"200K_MOR{year_suffix}",
    ]
    df_inc_out = df_inc[inc_keep]

    inc_csv_path = os.path.join(acs_dir, f"{state_abbr}_bg_income_{acs_year}.csv")
    df_inc_out.to_csv(inc_csv_path, index=False)
    print(f"Saved ACS income BG file: {inc_csv_path}")
    return inc_csv_path


def main():
    """Main function that processes command line arguments and runs the stage."""
    # Parse command line arguments
    parser = setup_argument_parser(
        description="Download TIGER shapefiles and ACS data for any US state.",
        stage_name="Stage 0"
    )
    args = parser.parse_args()

    # Validate state and get configuration
    state_info, state_paths = validate_state_setup(args.state, "stage0")
    print_state_info(state_info)

    # Get required variables
    state_abbr = state_info["abbr"]
    state_fips = state_info["fips"]
    census_api_key = os.environ.get("CENSUS_API_KEY")
    
    # Ensure directories exist
    os.makedirs(state_paths["tiger_dir"], exist_ok=True)
    os.makedirs(state_paths["acs_dir"], exist_ok=True)

    print(f"\n=== Preparing inputs for {state_info['name']} ===\n")

    # 1) Get BG & TABBLOCK shapefiles
    bg = get_bg_shapefile(state_abbr, state_fips, args.census_year, state_paths["tiger_dir"])
    blocks = get_tabblock20_shapefile(state_abbr, state_fips, args.census_year, state_paths["tiger_dir"])

    # Sanity: make sure they're this state
    print("BG unique STATEFP:", bg["STATEFP"].unique())

    # 2) Process ACS race data
    race_csv_path = process_race_data(bg, args.acs_year, census_api_key, state_paths["acs_dir"], state_abbr)

    # 3) Process ACS income data
    inc_csv_path = process_income_data(bg, args.acs_year, census_api_key, state_paths["acs_dir"], state_abbr)

    # Summary
    print("\nDone. You now have:")
    print(f"  - TIGER blocks   -> {state_paths['tabblock_dir']}")
    print(f"  - TIGER BGs      -> {os.path.dirname(state_paths['bg_shapefile'])}")
    print(f"  - ACS race BG    -> {race_csv_path}")
    print(f"  - ACS income BG  -> {inc_csv_path}")
    print(f"\nNext: Run stage 1 with -> python run_stage1.py {args.state}")


if __name__ == "__main__":
    main()