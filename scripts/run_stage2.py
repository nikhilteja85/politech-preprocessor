#!/usr/bin/env python3
"""
Stage 2: Build redistricting plans and assignments for any US state.

This script processes congressional and legislative district plans and creates
precinct-to-district assignments using spatial intersection.

Usage:
    python run_stage2.py <STATE_CODE> [options]
    
Examples:
    python run_stage2.py AZ
    python run_stage2.py CA --plan-year 2021
    python run_stage2.py TX --plan-year 2022

Inputs:
  - outputs/<state>/<state>_precinct_all_pop_<year>.geojson  (from Stage 1)
  - inputs/plans/<state>/<state>_cong_adopted_<year>/
  - inputs/plans/<state>/<state>_sl_adopted_<year>/

Outputs:
  - outputs/<state>/<state>_plans_<year>.json
  - outputs/<state>/<state>_assignments_<year>.json
"""

import os
import json
import geopandas as gpd
import maup
import pandas as pd
from common import (
    setup_argument_parser,
    validate_state_setup,
    get_state_paths,
    print_state_info,
    find_plan_shapefiles
)

# ============ CONSTANTS ============

WORK_CRS = "EPSG:5070"
OUTPUT_CRS = "EPSG:4326"


def load_precincts(precinct_geojson: str):
    """Load precincts from Stage 1 output."""
    if not os.path.exists(precinct_geojson):
        raise FileNotFoundError(
            f"Precinct GeoJSON not found: {precinct_geojson}\n"
            "Run Stage 1 first."
        )
    print(f"Loading precincts: {precinct_geojson}")
    gdf = gpd.read_file(precinct_geojson).to_crs(WORK_CRS)

    if "UNIQUE_ID" not in gdf.columns:
        raise RuntimeError("Expected 'UNIQUE_ID' column in precincts layer.")

    print(f"Loaded {len(gdf)} precincts")
    return gdf


def load_plan(shp_path: str, chamber_code: str, state_info: dict, plan_year: int):
    """
    Load district shapefile for a plan and normalize columns.

    Returns (districts_gdf, plan_meta_dict).
    """
    if not os.path.exists(shp_path):
        raise FileNotFoundError(
            f"Plan shapefile not found: {shp_path}\n"
            "Make sure you have downloaded the redistricting plan shapefiles."
        )

    print(f"\nLoading {chamber_code} plan shapefile: {shp_path}")
    gdf = gpd.read_file(shp_path).to_crs(WORK_CRS)

    # Map TIGER/RDH column names to standardized DISTRICT column
    # TIGER congressional: CD119FP (for 119th Congress)
    # TIGER state legislative lower: SLDLST
    # TIGER state legislative upper: SLDUST
    # RDH format: DISTRICT
    
    district_col_map = {
        'DISTRICT': 'DISTRICT',  # Already standardized (RDH format)
        'CD119FP': 'DISTRICT',   # TIGER congressional
        'CD118FP': 'DISTRICT',   # TIGER congressional (118th Congress)
        'CD117FP': 'DISTRICT',   # TIGER congressional (117th Congress)
        'CD116FP': 'DISTRICT',   # TIGER congressional (116th Congress)
        'SLDLST': 'DISTRICT',    # TIGER state legislative lower
        'SLDUST': 'DISTRICT',    # TIGER state legislative upper
    }
    
    # Find and map the district column
    for tiger_col, std_col in district_col_map.items():
        if tiger_col in gdf.columns:
            if tiger_col != std_col:
                print(f"Mapping '{tiger_col}' → 'DISTRICT'")
                gdf['DISTRICT'] = gdf[tiger_col]
            break
    
    # If still no DISTRICT column, try generic pattern matching
    if "DISTRICT" not in gdf.columns:
        district_cols = [c for c in gdf.columns if 'district' in c.lower() or 'dist' in c.lower() or c.endswith('FP') or c.endswith('ST')]
        if district_cols:
            print(f"Using '{district_cols[0]}' as DISTRICT column")
            gdf["DISTRICT"] = gdf[district_cols[0]]

    if "DISTRICT" not in gdf.columns:
        raise RuntimeError(
            f"Could not find a district identifier column in {shp_path}. "
            f"Available columns: {list(gdf.columns)}"
        )

    n_districts = int(gdf["DISTRICT"].nunique())
    print(f"{chamber_code} plan has {n_districts} districts")

    # Build simple plan metadata (no geometry here)
    plan_id = f"{state_info['abbr'].upper()}_{chamber_code}_ENACTED_{plan_year}"

    plan_meta = {
        "state": state_info["abbr"].upper(),
        "state_name": state_info["name"],
        "plan_id": plan_id,
        "chamber": chamber_code,   # "CONG" or "SL"
        "year": plan_year,
        "cycle": 2020,             # uses 2020 census; adjust if needed
        "source": "RDH official adopted plan shapefile",
        "name": f"{state_info['name']} {plan_year} Enacted {chamber_code} Plan",
        "num_districts": n_districts,
    }

    return gdf, plan_meta


def build_assignments_for_plan(precincts: gpd.GeoDataFrame,
                               districts: gpd.GeoDataFrame,
                               plan_meta: dict):
    """
    Use maup.assign to assign each precinct to exactly one district in this plan.
    Returns (assignments_list, n_unassigned).
    """
    chamber = plan_meta["chamber"]
    plan_id = plan_meta["plan_id"]

    print(f"\nAssigning precincts to {chamber} plan {plan_id} ...")

    # maup.assign returns a Series: index = precinct index, value = district index (in districts gdf)
    assignment = maup.assign(precincts, districts)

    # Map to DISTRICT code
    district_codes = assignment.map(districts["DISTRICT"])

    # Count unassigned
    unassigned_mask = district_codes.isna()
    n_unassigned = int(unassigned_mask.sum())
    if n_unassigned > 0:
        print(f"⚠ {n_unassigned} precincts could not be assigned to a {chamber} district")

    assignments = []
    for idx, precinct_row in precincts.iterrows():
        dist_code = district_codes.loc[idx]
        if dist_code is None or (isinstance(dist_code, float) and pd.isna(dist_code)):
            continue  # skip unassigned

        # Handle non-numeric district codes (e.g., 'ZZZ' for areas without defined districts)
        try:
            district_id = int(dist_code)
        except (ValueError, TypeError):
            # Use -1 for special codes like 'ZZZ' (districts not defined)
            # This preserves the precinct in the data while marking it as special
            district_id = -1

        assignments.append({
            "state": plan_meta["state"],
            "plan_id": plan_id,
            "precinct_id": precinct_row["UNIQUE_ID"],
            "district_id": district_id,
        })

    print(f"Built {len(assignments)} assignments for plan {plan_id}")
    return assignments, n_unassigned


def main():
    """Main function that processes command line arguments and runs the stage."""
    # Parse command line arguments
    parser = setup_argument_parser(
        description="Process redistricting plans and create precinct assignments for any US state.",
        stage_name="Stage 2"
    )
    
    # Optional plan-year override
    parser.add_argument(
        "--plan-year",
        type=int,
        default=None,
        help="Plan year for redistricting plans (default: auto-detect from available plans)"
    )
    
    args = parser.parse_args()

    # Validate state and get configuration
    state_info, state_paths = validate_state_setup(args.state, acs_year=args.acs_year, census_year=args.census_year)
    print_state_info(state_info)

    # 1. Load precincts from Stage 1 output
    precincts = load_precincts(state_paths["precinct_geojson"])

    # 2. Find and load plans (auto-detects year if not specified)
    try:
        plan_files = find_plan_shapefiles(state_paths["plans_dir"], state_info["abbr"], args.plan_year)
        plan_year = plan_files['year']  # Get the detected/specified year
    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        print(f"\n📁 Expected plan structure:")
        print(f"   inputs/plans/{state_info['abbr']}/")
        print(f"   ├── {state_info['abbr']}_cong_adopted_YYYY/       # Congressional districts")
        print(f"   │   └── [shapefile].shp")
        print(f"   ├── {state_info['abbr']}_sldl_adopted_YYYY/       # State Legislative Lower")
        print(f"   │   └── [shapefile].shp")
        print(f"   └── {state_info['abbr']}_sldu_adopted_YYYY/       # State Legislative Upper")
        print(f"       └── [shapefile].shp")
        print(f"\n   Note: Some states may use 'sl' instead of 'sldl'/'sldu' for unicameral legislatures")
        print(f"\nDownload plans from: https://redistrictingdatahub.org/")
        return

    all_plans = []
    all_assignments = []
    
    # Chamber code mapping for display/metadata
    chamber_codes = {
        'cong': 'CONG',
        'sl': 'SL',
        'sldl': 'SLDL',
        'sldu': 'SLDU'
    }
    
    chamber_names = {
        'cong': 'Congressional',
        'sl': 'State Legislative',
        'sldl': 'State Legislative Lower',
        'sldu': 'State Legislative Upper'
    }
    
    # Process all detected chambers dynamically
    for chamber_key in plan_files:
        if chamber_key == 'year':  # Skip the year metadata
            continue
            
        chamber_code = chamber_codes.get(chamber_key, chamber_key.upper())
        chamber_name = chamber_names.get(chamber_key, chamber_key.title())
        
        try:
            plan_gdf, plan_meta = load_plan(plan_files[chamber_key], chamber_code, state_info, plan_year)
            assignments, unassigned = build_assignments_for_plan(precincts, plan_gdf, plan_meta)
            all_plans.append(plan_meta)
            all_assignments.extend(assignments)
            print(f"{chamber_name} plan: {len(assignments)} assignments, {unassigned} unassigned")
        except Exception as e:
            print(f"⚠ Warning: Failed to process {chamber_name} plan: {e}")

    if not all_plans:
        print("❌ No plans were successfully processed.")
        return

    print(f"\nTotal assignments created: {len(all_assignments)}")

    # 3. Save JSON outputs to centralized files (support appending)
    plans_json = state_paths["plans_json"]
    assignments_json = state_paths["assignments_json"]
    
    # Load existing data or start with empty lists
    existing_plans = []
    existing_assignments = []
    
    if os.path.exists(plans_json):
        with open(plans_json, "r") as f:
            existing_plans = json.load(f)
    
    if os.path.exists(assignments_json):
        with open(assignments_json, "r") as f:
            existing_assignments = json.load(f)
    
    # Remove old entries for this state/year combination
    existing_plans = [p for p in existing_plans 
                      if not (p.get("state") == state_info["abbr"].upper() and p.get("year") == plan_year)]
    existing_assignments = [a for a in existing_assignments 
                           if not (a.get("state") == state_info["abbr"].upper() and a.get("year") == plan_year)]
    
    # Append new data
    existing_plans.extend(all_plans)
    existing_assignments.extend(all_assignments)
    
    # Save updated data
    with open(plans_json, "w") as f:
        json.dump(existing_plans, f, indent=2)
    print(f"\n✅ Saved plans to centralized JSON: {plans_json}")
    print(f"   Total plans in file: {len(existing_plans)}")

    with open(assignments_json, "w") as f:
        json.dump(existing_assignments, f, indent=2)
    print(f"✅ Saved assignments to centralized JSON: {assignments_json}")
    print(f"   Total assignments in file: {len(existing_assignments)}")
    print(f"\nNext: Run stage 3 with -> python run_stage3_dots.py {args.state}")


if __name__ == "__main__":
    main()