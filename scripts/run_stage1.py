#!/usr/bin/env python3
"""
Stage 1: Build precinct-level demographics from block-group ACS + CVAP using maup.

This script aggregates demographic data from Census block groups to election precincts
for any US state using area-weighted interpolation.

Usage:
    python run_stage1.py <STATE_CODE> [options]
    
Examples:
    python run_stage1.py AZ
    python run_stage1.py CA --acs-year 2022
    python run_stage1.py TX --acs-year 2023

Outputs:
  outputs/<state>/<state>_bg_all_data_<year>.geojson
  outputs/<state>/<state>_precinct_all_pop_<year>.geojson
  outputs/<state>/<state>_population_comparison_<year>.csv
  outputs/<state>/<state>_cvap_comparison_<year>.csv
  outputs/<state>/<state>_income_comparison_<year>.csv
"""

import os
import pandas as pd
import geopandas as gpd
import maup
from common import (
    setup_argument_parser,
    validate_state_setup,
    get_state_paths,
    print_state_info,
    find_precinct_shapefile,
    find_acs_file
)

# ===================== CONSTANTS =====================

OUTPUT_CRS = "EPSG:4326"
WORK_CRS = "EPSG:5070"   # NAD83 / Conus Albers (good for area/weights)


def get_column_name(base_name: str, year: int) -> str:
    """Generate column name with year suffix (e.g., 'HSP_POP' -> 'HSP_POP23')"""
    year_suffix = str(year)[-2:]  # Get last 2 digits
    return f"{base_name}{year_suffix}"


def make_bg_geoid(df: gpd.GeoDataFrame) -> pd.Series:
    """
    Construct a 12-digit block-group GEOID from the TIGER BG shapefile.

    Handles:
      - GEOID or GEOID20 already present, OR
      - STATEFP/COUNTYFP/TRACTCE/BLKGRPCE, OR
      - STATEFP20/COUNTYFP20/TRACTCE20/BLKGRPCE20
    """
    for col in ("GEOID", "GEOID20"):
        if col in df.columns:
            return df[col].astype(str)

    # (state, county, tract, block group)
    candidates = [
        ("STATEFP20", "COUNTYFP20", "TRACTCE20", "BLKGRPCE20"),
        ("STATEFP", "COUNTYFP", "TRACTCE", "BLKGRPCE"),
    ]
    for cols in candidates:
        if all(c in df.columns for c in cols):
            state = df[cols[0]].astype(str).str.zfill(2)
            county = df[cols[1]].astype(str).str.zfill(3)
            tract = df[cols[2]].astype(str).str.zfill(6)
            bg = df[cols[3]].astype(str).str.zfill(1)
            return state + county + tract + bg

    raise RuntimeError(
        "Could not construct BG GEOID – check column names in the TIGER BG shapefile."
    )


def load_bg_geometry(bg_shapefile: str) -> gpd.GeoDataFrame:
    """Load and prepare block group geometry."""
    print(f"Loading TIGER BG shapefile: {bg_shapefile}")
    bg = gpd.read_file(bg_shapefile)
    bg["GEOID"] = make_bg_geoid(bg)
    bg = bg.to_crs(WORK_CRS)
    print(f"Loaded {len(bg)} block groups")
    return bg


def load_acs_race(acs_race_csv: str) -> pd.DataFrame:
    """Load ACS race data."""
    print(f"Loading ACS race BG CSV: {acs_race_csv}")
    df = pd.read_csv(acs_race_csv, dtype={"GEOID": str})
    return df


def load_acs_income(acs_income_csv: str) -> pd.DataFrame:
    """Load ACS income data."""
    print(f"Loading ACS income BG CSV: {acs_income_csv}")
    df = pd.read_csv(acs_income_csv, dtype={"GEOID": str})
    return df


def load_cvap_blockgroups(cvap_csv: str, acs_year: int) -> pd.DataFrame:
    """
    Read national BlockGr.csv and build CVAP columns at BG level for this state.

    Using LNNUMBER codes from CVAP docs:
      1  = Total CVAP
      3  = AIAN alone (NH)
      4  = Asian alone (NH)
      5  = Black alone (NH)
      6  = NHPI alone (NH)
      7  = White alone (NH)
      8–12 = multiracial categories (NH) → combined into 2OM_CVAP
      13 = Hispanic or Latino
    """
    print(f"Loading CVAP BlockGr CSV: {cvap_csv}")
    cvap = pd.read_csv(cvap_csv, dtype={"geoid": str}, encoding="latin1")
    cvap["STATEFP"] = cvap["geoid"].str[9:11]
    cvap["GEOID"] = cvap["geoid"].str[-12:]

    # Pivot so we have one row per GEOID with columns = LNNUMBER
    pivot = cvap.pivot_table(
        index="GEOID",
        columns="lnnumber",
        values="cvap_est",
        aggfunc="first",
    ).fillna(0)

    out = pd.DataFrame(index=pivot.index)
    out[get_column_name("TOT_CVAP", acs_year)] = pivot.get(1, 0)
    out[get_column_name("HSP_CVAP", acs_year)] = pivot.get(13, 0)
    out[get_column_name("WHT_CVAP", acs_year)] = pivot.get(7, 0)
    out[get_column_name("BLK_CVAP", acs_year)] = pivot.get(5, 0)
    out[get_column_name("AIA_CVAP", acs_year)] = pivot.get(3, 0)
    out[get_column_name("ASN_CVAP", acs_year)] = pivot.get(4, 0)
    out[get_column_name("HPI_CVAP", acs_year)] = pivot.get(6, 0)

    multi_cols = [c for c in [8, 9, 10, 11, 12] if c in pivot.columns]
    if multi_cols:
        out[get_column_name("2OM_CVAP", acs_year)] = pivot[multi_cols].sum(axis=1)
    else:
        out[get_column_name("2OM_CVAP", acs_year)] = 0

    out = out.reset_index()  # bring GEOID back as column
    return out


def aggregate_bg_to_precincts(bg: gpd.GeoDataFrame,
                              precincts: gpd.GeoDataFrame,
                              columns: list[str],
                              label: str):
    """
    Use maup.assign to aggregate BG columns -> precincts.
    Returns (precincts_with_values, comparison_df).
    """
    print(f"\n=== Aggregating {label} from BG to precincts ===")

    bg = bg.to_crs(WORK_CRS)
    precincts = precincts.to_crs(WORK_CRS)

    assignment = maup.assign(bg, precincts)

    # Sum BG columns by assigned precinct index
    agg = bg[columns].groupby(assignment).sum()

    for col in columns:
        precincts[col] = agg.get(col, 0)

    # Comparison: BG totals vs precinct totals
    source_totals = bg[columns].sum()
    target_totals = precincts[columns].sum()
    differences = target_totals - source_totals
    pct_diff = (differences / source_totals.replace(0, pd.NA)) * 100

    comparison = pd.DataFrame({
        "Source_BG": source_totals,
        "Target_Precinct": target_totals,
        "Difference": differences,
        "Pct_Difference": pct_diff.round(8),
    })

    print(comparison)
    print(f"Total difference across all {label} columns: {differences.sum()}")

    return precincts, comparison


def main():
    """Main function that processes command line arguments and runs the stage."""
    # Parse command line arguments
    parser = setup_argument_parser(
        description="Aggregate demographic data from block groups to precincts for any US state.",
        stage_name="Stage 1"
    )
    args = parser.parse_args()

    # Validate state and get configuration
    state_info, state_paths = validate_state_setup(args.state, acs_year=args.acs_year, census_year=args.census_year)
    print_state_info(state_info)

    # Get the detected/specified years
    acs_year = state_paths["acs_year"]
    census_year = state_paths["census_year"]
    
    print(f"\n=== Processing {state_info['name']} demographics ===")
    print(f"Using ACS year: {acs_year}, Census year: {census_year}\n")

    # Get required variables
    state_abbr = state_info["abbr"]
    
    # 1. Load BG geometry
    bg_geo = load_bg_geometry(state_paths["bg_shapefile"])

    # 2. Load data tables
    acs_race_csv = find_acs_file(state_abbr, acs_year, "race")
    acs_income_csv = find_acs_file(state_abbr, acs_year, "income")
    
    acs_race = load_acs_race(acs_race_csv)
    acs_income = load_acs_income(acs_income_csv)
    cvap_bg = load_cvap_blockgroups(state_paths["cvap_blockgr_csv"], acs_year)

    # 3. Merge ACS + CVAP onto BG geometry
    print("Merging ACS race onto BG geometry...")
    bg = bg_geo.merge(acs_race, on="GEOID", how="left", validate="1:1")

    print("Merging ACS income onto BG geometry...")
    bg = bg.merge(acs_income, on="GEOID", how="left", validate="1:1")

    print("Merging CVAP onto BG geometry...")
    bg = bg.merge(cvap_bg, on="GEOID", how="left", validate="1:1")

    # 4. Derive totals on BG side
    race_cols = [
        get_column_name("HSP_POP", acs_year), get_column_name("WHT_POP", acs_year), 
        get_column_name("BLK_POP", acs_year), get_column_name("AIA_POP", acs_year), 
        get_column_name("ASN_POP", acs_year), get_column_name("HPI_POP", acs_year),
        get_column_name("OTH_POP", acs_year), get_column_name("2OM_POP", acs_year),
    ]
    
    bg[get_column_name("NHSP_POP", acs_year)] = (
        bg[get_column_name("WHT_POP", acs_year)] + bg[get_column_name("BLK_POP", acs_year)] + 
        bg[get_column_name("AIA_POP", acs_year)] + bg[get_column_name("ASN_POP", acs_year)] + 
        bg[get_column_name("HPI_POP", acs_year)] + bg[get_column_name("OTH_POP", acs_year)] +
        bg[get_column_name("2OM_POP", acs_year)]
    )
    bg[get_column_name("TOT_POP", acs_year)] = bg[get_column_name("HSP_POP", acs_year)] + bg[get_column_name("NHSP_POP", acs_year)]
    race_cols_all = race_cols + [get_column_name("NHSP_POP", acs_year), get_column_name("TOT_POP", acs_year)]

    cvap_cols = [
        get_column_name("HSP_CVAP", acs_year), get_column_name("WHT_CVAP", acs_year), 
        get_column_name("BLK_CVAP", acs_year), get_column_name("AIA_CVAP", acs_year), 
        get_column_name("ASN_CVAP", acs_year), get_column_name("HPI_CVAP", acs_year),
        get_column_name("2OM_CVAP", acs_year),
    ]
    
    bg[get_column_name("NHSP_CVAP", acs_year)] = (
        bg[get_column_name("WHT_CVAP", acs_year)] + bg[get_column_name("BLK_CVAP", acs_year)] + 
        bg[get_column_name("AIA_CVAP", acs_year)] + bg[get_column_name("ASN_CVAP", acs_year)] + 
        bg[get_column_name("HPI_CVAP", acs_year)] + bg[get_column_name("2OM_CVAP", acs_year)]
    )
    bg[get_column_name("TOT_CVAP", acs_year)] = bg[get_column_name("HSP_CVAP", acs_year)] + bg[get_column_name("NHSP_CVAP", acs_year)]
    cvap_cols_all = cvap_cols + [get_column_name("NHSP_CVAP", acs_year), get_column_name("TOT_CVAP", acs_year)]

    income_cols = [
        get_column_name("LESS_10K", acs_year), get_column_name("10K_15K", acs_year), 
        get_column_name("15K_20K", acs_year), get_column_name("20K_25K", acs_year),
        get_column_name("25K_30K", acs_year), get_column_name("30K_35K", acs_year), 
        get_column_name("35K_40K", acs_year), get_column_name("40K_45K", acs_year),
        get_column_name("45K_50K", acs_year), get_column_name("50K_60K", acs_year), 
        get_column_name("60K_75K", acs_year), get_column_name("75K_100K", acs_year),
        get_column_name("100_125K", acs_year), get_column_name("125_150K", acs_year), 
        get_column_name("150_200K", acs_year), get_column_name("200K_MOR", acs_year),
    ]
    
    bg[get_column_name("TOT_HOUS", acs_year)] = bg[income_cols].sum(axis=1)
    income_cols_all = income_cols + [get_column_name("TOT_HOUS", acs_year)]

    numeric_cols = race_cols_all + cvap_cols_all + income_cols_all
    for col in numeric_cols:
        if col in bg.columns:
            bg[col] = bg[col].fillna(0).astype("Int64")

    # 5. Save BG all-data GeoJSON for reuse (dotmaps etc.)
    print(f"\nSaving BG all-data GeoJSON: {state_paths['bg_geojson']}")
    bg_out = bg.to_crs(OUTPUT_CRS)
    bg_out.to_file(state_paths["bg_geojson"], driver="GeoJSON")

    # 6. Load precincts
    precinct_shp = find_precinct_shapefile(state_paths["precincts_dir"])
    print(f"\nLoading precinct shapefile: {precinct_shp}")
    precincts = gpd.read_file(precinct_shp).to_crs(WORK_CRS)
    print(f"Loaded {len(precincts)} precincts")

    # 7. Aggregate race / CVAP / income from BG -> precincts
    precincts, pop_comp = aggregate_bg_to_precincts(
        bg, precincts, race_cols_all, label="population by race"
    )
    precincts, cvap_comp = aggregate_bg_to_precincts(
        bg, precincts, cvap_cols_all, label="CVAP by race"
    )
    precincts, income_comp = aggregate_bg_to_precincts(
        bg, precincts, income_cols_all, label="household income"
    )

    # Placeholder median income (can be computed properly later)
    precincts[get_column_name("MEDN_INC", acs_year)] = 0

    # Make sure numeric columns are integer-like on precincts
    for col in numeric_cols + [get_column_name("MEDN_INC", acs_year)]:
        if col in precincts.columns:
            precincts[col] = precincts[col].fillna(0).round().astype("Int64")

    # 8. Save comparison CSVs (commented out - just printing to terminal is enough)
    # pop_comp.to_csv(state_paths["pop_comparison_csv"], index_label="variable")
    # cvap_comp.to_csv(state_paths["cvap_comparison_csv"], index_label="variable")
    # income_comp.to_csv(state_paths["income_comparison_csv"], index_label="variable")
    # print(f"\nSaved diagnostics to {state_paths['state_output_dir']}")

    # 9. Prepare final precinct output with key columns
    # Core ID / meta columns we want to keep (only if they exist)
    id_cols = [
        "UNIQUE_ID",
        "COUNTYFP",
        "COUNTY_NAM",
        "PRECINCTNA",
        "CONG_DIST",
        "SLDL_DIST",
        "SLDU_DIST",
    ]

    # Election fields you care about
    election_cols = [
        "G24PREDHAR",
        "G24PRERTRU",
        "GCON01DSHA",
        "GCON01RSCH",
    ]

    # Demographic fields: any column ending with year suffix
    year_suffix = str(acs_year)[-2:]  # Get last 2 digits
    demo_cols = [
        c
        for c in precincts.columns
        if c.endswith(f"POP{year_suffix}") or c.endswith(f"CVAP{year_suffix}")
    ]

    other_cols = [get_column_name("MEDN_INC", acs_year), "geometry"]

    # Now build keep list, but only include columns that actually exist
    raw_keep_cols = (
        [c for c in id_cols if c in precincts.columns]
        + [c for c in election_cols if c in precincts.columns]
        + [c for c in demo_cols if c in precincts.columns]
        + [c for c in income_cols_all if c in precincts.columns]
        + [c for c in other_cols if c in precincts.columns]
    )

    # Deduplicate while preserving order
    keep_cols = list(dict.fromkeys(raw_keep_cols))
    precincts_slim = precincts[keep_cols]

    # 10. Save slim precinct GeoJSON
    precincts_out = precincts_slim.to_crs(OUTPUT_CRS)
    precincts_out.to_file(state_paths["precinct_geojson"], driver="GeoJSON")
    print(f"✅ Saved precinct GeoJSON: {state_paths['precinct_geojson']}")
    print(f"Columns in final precinct layer: {list(precincts_out.columns)}")
    print(f"\nNext: Run stage 2 with -> python run_stage2.py {args.state}")


if __name__ == "__main__":
    main()