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

    # Expect DISTRICT, LONGNAME, SHORTNAME based on typical RDH format
    for col in ["DISTRICT", "LONGNAME", "SHORTNAME"]:
        if col not in gdf.columns:
            print(f"Warning: Expected column '{col}' not found in plan shapefile {shp_path}")
            print(f"Available columns: {list(gdf.columns)}")
            # Try to find a district column with different name
            district_cols = [c for c in gdf.columns if 'district' in c.lower() or 'dist' in c.lower()]
            if district_cols and "DISTRICT" not in gdf.columns:
                print(f"Using '{district_cols[0]}' as DISTRICT column")
                gdf["DISTRICT"] = gdf[district_cols[0]]
            break

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

        assignments.append({
            "state": plan_meta["state"],
            "plan_id": plan_id,
            "precinct_id": precinct_row["UNIQUE_ID"],
            "district_id": int(dist_code),
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
    
    parser.add_argument(
        "--plan-year",
        type=int,
        default=2022,
        help="Plan year for redistricting plans (default: 2022)"
    )
    
    args = parser.parse_args()

    # Validate state and get configuration
    state_info, state_paths = validate_state_setup(args.state)
    print_state_info(state_info)

    # 1. Load precincts from Stage 1 output
    precincts = load_precincts(state_paths["precinct_geojson"])

    # 2. Find and load plans
    try:
        plan_files = find_plan_shapefiles(state_paths["plans_dir"], state_info["abbr"], args.plan_year)
    except FileNotFoundError as e:
        print(f"\n❌ {e}")
        print(f"\n📁 Expected plan structure:")
        print(f"   inputs/plans/{state_info['abbr']}/")
        print(f"   ├── {state_info['abbr']}_cong_adopted_{args.plan_year}/")
        print(f"   │   └── [congressional_district_shapefile].shp")
        print(f"   └── {state_info['abbr']}_sl_adopted_{args.plan_year}/")
        print(f"       └── [legislative_district_shapefile].shp")
        print(f"\nDownload plans from: https://redistrictingdatahub.org/")
        return

    all_plans = []
    all_assignments = []
    
    # Process congressional plan
    if "cong" in plan_files:
        try:
            cong_gdf, cong_meta = load_plan(plan_files["cong"], "CONG", state_info, args.plan_year)
            cong_assignments, cong_unassigned = build_assignments_for_plan(precincts, cong_gdf, cong_meta)
            all_plans.append(cong_meta)
            all_assignments.extend(cong_assignments)
            print(f"Congressional plan: {len(cong_assignments)} assignments, {cong_unassigned} unassigned")
        except Exception as e:
            print(f"⚠ Warning: Failed to process congressional plan: {e}")
    
    # Process legislative plan
    if "leg" in plan_files:
        try:
            leg_gdf, leg_meta = load_plan(plan_files["leg"], "SL", state_info, args.plan_year)
            leg_assignments, leg_unassigned = build_assignments_for_plan(precincts, leg_gdf, leg_meta)
            all_plans.append(leg_meta)
            all_assignments.extend(leg_assignments)
            print(f"Legislative plan: {len(leg_assignments)} assignments, {leg_unassigned} unassigned")
        except Exception as e:
            print(f"⚠ Warning: Failed to process legislative plan: {e}")

    if not all_plans:
        print("❌ No plans were successfully processed.")
        return

    print(f"\nTotal assignments created: {len(all_assignments)}")

    # 3. Save JSON outputs
    plans_json = state_paths["plans_json"].format(year=args.plan_year)
    assignments_json = state_paths["assignments_json"].format(year=args.plan_year)
    
    with open(plans_json, "w") as f:
        json.dump(all_plans, f, indent=2)
    print(f"\n✅ Saved plans JSON: {plans_json}")

    with open(assignments_json, "w") as f:
        json.dump(all_assignments, f, indent=2)
    print(f"✅ Saved assignments JSON: {assignments_json}")
    print(f"\nNext: Run stage 3 with -> python run_stage3_dots.py {args.state}")


if __name__ == "__main__":
    main()