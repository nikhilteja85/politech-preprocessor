#!/usr/bin/env python3
"""
Stage 0: Prepare geometry + ACS inputs for any US state (+ optional TIGER "plans").

This script downloads:
  - TIGER block group shapefile
  - TIGER 2020 tabblock20 shapefile
  - ACS 5-year (race + income) at block-group level
  - OPTIONAL: official district boundaries from TIGER/Line ("plans"):
      * Congressional Districts (CD)
      * State Legislative Districts - Lower (SLDL)
      * State Legislative Districts - Upper (SLDU)

Usage:
    python run_stage0.py <STATE_CODE> [options]

Examples:
    python run_stage0.py AZ
    python run_stage0.py AZ --plan-year 2025
    python run_stage0.py CA --acs-year 2022 --plan-year 2024
    python run_stage0.py TX --skip-plans

Outputs:
  inputs/tiger_<CENSUS_YEAR>/<state>_bg/...
  inputs/tiger_<CENSUS_YEAR>/<state>_tabblock20/...
  inputs/acs_<ACS_YEAR>/<state>_bg_race_<ACS_YEAR>.csv
  inputs/acs_<ACS_YEAR>/<state>_bg_income_<ACS_YEAR>.csv
  inputs/plans/<state>/<state>_cong_tiger_<PLAN_YEAR>_<CDTAG>/...   (if available)
  inputs/plans/<state>/<state>_sldl_tiger_<PLAN_YEAR>/...           (if available)
  inputs/plans/<state>/<state>_sldu_tiger_<PLAN_YEAR>/...           (if available)
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
    print_state_info,
)

load_dotenv()

DEFAULT_ACS_YEAR = 2023
DEFAULT_CENSUS_YEAR = 2020
DEFAULT_PLAN_YEAR = 2025  # TIGER vintage for CD/SLDL/SLDU downloads

# ---------------------- Download helpers ----------------------

def download_and_unzip(url: str, out_dir: str) -> str:
    """Download a TIGER zip file and extract into out_dir."""
    print(f"Downloading {url}")
    resp = requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        zf.extractall(out_dir)

    print(f"Extracted to {out_dir}")
    return out_dir


def url_exists(url: str) -> bool:
    """
    Check existence without downloading entire file.
    HEAD sometimes fails on some hosts; fall back to a tiny GET.
    """
    try:
        r = requests.head(url, allow_redirects=True, timeout=30)
        if r.status_code == 200:
            return True
        if r.status_code in (403, 405):  # HEAD not allowed; fall back
            raise RuntimeError("HEAD not allowed")
        return False
    except Exception:
        try:
            r = requests.get(url, stream=True, timeout=30)
            ok = (r.status_code == 200)
            r.close()
            return ok
        except Exception:
            return False


# ---------------------- TIGER geometry (BG/blocks) ----------------------

def get_tabblock20_shapefile(state_abbr: str, state_fips: str, census_year: int, tiger_dir: str) -> gpd.GeoDataFrame:
    """Return GeoDataFrame for 2020 tabulation blocks (PL geometry base)."""
    target_dir = os.path.join(tiger_dir, f"{state_abbr}_tabblock20")
    os.makedirs(target_dir, exist_ok=True)

    tabblock20_base = "https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20"
    zip_name = f"tl_{census_year}_{state_fips}_tabblock20.zip"
    url = f"{tabblock20_base}/{zip_name}"
    download_and_unzip(url, target_dir)

    shp = [f for f in os.listdir(target_dir) if f.lower().endswith(".shp")][0]
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

    shp = [f for f in os.listdir(target_dir) if f.lower().endswith(".shp")][0]
    shp_path = os.path.join(target_dir, shp)

    print(f"Loading block groups from {shp_path}")
    gdf = gpd.read_file(shp_path)
    print("Block groups:", len(gdf))
    return gdf


# ---------------------- ACS pulling ----------------------

def fetch_acs_blockgroups_for_state(
    bg_gdf: gpd.GeoDataFrame,
    year: int,
    variables: list[str],
    census_api_key: str,
) -> pd.DataFrame:
    """
    Fetch ACS 5-year data at block-group level for this state.
    We loop counties:
      for=block group:*&in=state:04 county:001
    """
    if not census_api_key:
        raise RuntimeError("CENSUS_API_KEY is missing. Put it in your environment or .env file.")

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
        resp = requests.get(base_url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        header = data[0]
        for row in data[1:]:
            rows.append(dict(zip(header, row)))

    df = pd.DataFrame(rows)
    df["GEOID"] = df["GEO_ID"].str[-12:]  # BG GEOID (12 digits)

    for v in variables:
        df[v] = pd.to_numeric(df[v], errors="coerce")

    return df


def process_race_data(bg_gdf: gpd.GeoDataFrame, acs_year: int, census_api_key: str, acs_dir: str, state_abbr: str) -> str:
    race_vars = [
        "B03002_001E",  # total
        "B03002_003E",  # NH white
        "B03002_004E",  # NH black
        "B03002_005E",  # NH AIAN
        "B03002_006E",  # NH Asian
        "B03002_007E",  # NH NHPI
        "B03002_008E",  # NH other
        "B03002_009E",  # NH 2+ races
        "B03002_012E",  # Hispanic
    ]
    print("\n=== Fetching ACS race (B03002) ===")
    df_race = fetch_acs_blockgroups_for_state(bg_gdf, acs_year, race_vars, census_api_key)

    year_suffix = str(acs_year)[-2:]

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

    # POP-style fields
    for k in ["WHT", "BLK", "AIA", "ASN", "HPI", "OTH", "2OM"]:
        df_race[f"{k}_POP{year_suffix}"] = df_race[f"{k}_NHSP{year_suffix}"]

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
    income_vars = [
        "B19001_001E",
        "B19001_002E", "B19001_003E", "B19001_004E", "B19001_005E",
        "B19001_006E", "B19001_007E", "B19001_008E", "B19001_009E",
        "B19001_010E", "B19001_011E", "B19001_012E", "B19001_013E",
        "B19001_014E", "B19001_015E", "B19001_016E", "B19001_017E",
    ]
    print("\n=== Fetching ACS income (B19001) ===")
    df_inc = fetch_acs_blockgroups_for_state(bg_gdf, acs_year, income_vars, census_api_key)

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

    inc_keep = ["GEOID"] + [c for c in df_inc.columns if c.endswith(year_suffix)]
    df_inc_out = df_inc[inc_keep]

    inc_csv_path = os.path.join(acs_dir, f"{state_abbr}_bg_income_{acs_year}.csv")
    df_inc_out.to_csv(inc_csv_path, index=False)
    print(f"Saved ACS income BG file: {inc_csv_path}")
    return inc_csv_path


# ---------------------- Plans (CD / SLDL / SLDU) ----------------------

def download_tiger_plan_zip(tiger_year: int, subdir: str, zip_name: str, out_dir: str) -> str:
    base = f"https://www2.census.gov/geo/tiger/TIGER{tiger_year}/{subdir}"
    url = f"{base}/{zip_name}"
    download_and_unzip(url, out_dir)
    return out_dir


def download_plans_for_state(project_inputs_dir: str, state_abbr: str, state_fips: str, plan_year: int) -> dict:
    """
    Download official boundaries from TIGTR/Line. Not all will exist for all states/years.

    Returns dict with keys:
      cd_dir, cd_tag, sldl_dir, sldu_dir (some may be None)
    """
    plans_base = os.path.join(project_inputs_dir, "plans", state_abbr)
    os.makedirs(plans_base, exist_ok=True)

    results = {"cd_dir": None, "cd_tag": None, "sldl_dir": None, "sldu_dir": None}

    print(f"\n=== Downloading TIGER{plan_year} plans (official boundaries) ===")

    # ---- CD: needs a cd### tag; probe a few candidates ----
    cd_candidates = ["cd119", "cd118", "cd117", "cd116", "cd115", "cd114", "cd113"]
    cd_found = None
    for tag in cd_candidates:
        zip_name = f"tl_{plan_year}_{state_fips}_{tag}.zip"
        url = f"https://www2.census.gov/geo/tiger/TIGER{plan_year}/CD/{zip_name}"
        if url_exists(url):
            cd_found = tag
            cd_out_dir = os.path.join(plans_base, f"{state_abbr}_cong_adopted_{plan_year}_{tag}")
            os.makedirs(cd_out_dir, exist_ok=True)
            print(f"✓ Found CD zip: {zip_name}")
            download_tiger_plan_zip(plan_year, "CD", zip_name, cd_out_dir)
            results["cd_dir"] = cd_out_dir
            results["cd_tag"] = tag
            break

    if cd_found is None:
        print(f"⚠ Could not find a CD zip for {state_abbr} in TIGER{plan_year}. Skipping CD.")

    # ---- SLDL / SLDU: stable names but may not exist for some states/territories ----
    for chamber, subdir in [("sldl", "SLDL"), ("sldu", "SLDU")]:
        zip_name = f"tl_{plan_year}_{state_fips}_{chamber}.zip"
        url = f"https://www2.census.gov/geo/tiger/TIGER{plan_year}/{subdir}/{zip_name}"

        out_dir = os.path.join(plans_base, f"{state_abbr}_{chamber}_adopted_{plan_year}")
        os.makedirs(out_dir, exist_ok=True)

        if not url_exists(url):
            print(f"⚠ {subdir} not available for {state_abbr} in TIGER{plan_year}. Skipping {subdir}.")
            continue

        print(f"✓ Found {subdir} zip: {zip_name}")
        download_tiger_plan_zip(plan_year, subdir, zip_name, out_dir)
        if chamber == "sldl":
            results["sldl_dir"] = out_dir
        else:
            results["sldu_dir"] = out_dir

    return results


# ---------------------- main ----------------------

def main():
    parser = setup_argument_parser(
        description="Download TIGER shapefiles, ACS data, and optional TIGER plans for any US state.",
        stage_name="Stage 0"
    )
    # Extend your existing parser with plan knobs
    parser.add_argument("--plan-year", type=int, default=DEFAULT_PLAN_YEAR, help="TIGER vintage for plans (CD/SLDL/SLDU).")
    parser.add_argument("--skip-plans", action="store_true", help="Skip downloading TIGER plans.")
    args = parser.parse_args()

    acs_year = args.acs_year if args.acs_year else DEFAULT_ACS_YEAR
    census_year = args.census_year if args.census_year else DEFAULT_CENSUS_YEAR
    plan_year = args.plan_year

    state_info, state_paths = validate_state_setup(
        args.state,
        "stage0",
        acs_year=acs_year,
        census_year=census_year
    )
    print_state_info(state_info)

    state_abbr = state_info["abbr"]
    state_fips = state_info["fips"]
    census_api_key = os.environ.get("CENSUS_API_KEY")

    # stage0 already makes these
    os.makedirs(state_paths["tiger_dir"], exist_ok=True)
    os.makedirs(state_paths["acs_dir"], exist_ok=True)

    # Your repo "inputs" root (used for plans)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(base_dir)
    project_inputs_dir = os.path.join(project_root, "inputs")
    os.makedirs(project_inputs_dir, exist_ok=True)

    print(f"\n=== Preparing inputs for {state_info['name']} ===")
    print(f"Using ACS year: {acs_year}, Census year: {census_year}, Plan (TIGER) year: {plan_year}\n")

    # 1) Get BG & TABBLOCK shapefiles
    bg = get_bg_shapefile(state_abbr, state_fips, census_year, state_paths["tiger_dir"])
    _ = get_tabblock20_shapefile(state_abbr, state_fips, census_year, state_paths["tiger_dir"])

    print("BG unique STATEFP:", bg["STATEFP"].unique())

    # 2) Process ACS race + income
    race_csv_path = process_race_data(bg, acs_year, census_api_key, state_paths["acs_dir"], state_abbr)
    inc_csv_path = process_income_data(bg, acs_year, census_api_key, state_paths["acs_dir"], state_abbr)

    # 3) Optional: plans
    plans_result = None
    if not args.skip_plans:
        plans_result = download_plans_for_state(project_inputs_dir, state_abbr, state_fips, plan_year)

    # Summary
    print("\nDone. You now have:")
    print(f"  - TIGER blocks   -> {state_paths['tabblock_dir']}")
    print(f"  - TIGER BGs      -> {os.path.dirname(state_paths['bg_shapefile'])}")
    print(f"  - ACS race BG    -> {race_csv_path}")
    print(f"  - ACS income BG  -> {inc_csv_path}")

    if plans_result:
        print("  - Plans:")
        print(f"      CD   -> {plans_result.get('cd_dir') or 'SKIPPED/NOT FOUND'}")
        print(f"      SLDL -> {plans_result.get('sldl_dir') or 'SKIPPED/NOT FOUND'}")
        print(f"      SLDU -> {plans_result.get('sldu_dir') or 'SKIPPED/NOT FOUND'}")

    print(f"\nNext: Run stage 1 with -> python run_stage1.py {args.state}")


if __name__ == "__main__":
    main()
