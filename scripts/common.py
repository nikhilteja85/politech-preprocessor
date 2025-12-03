#!/usr/bin/env python3
"""
Common configuration and utilities for all politech-processor scripts.
This module handles state configuration, argument parsing, and shared functionality.
"""

import json
import os
import argparse
from typing import Dict, Any, Tuple

# Base directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_DIR = os.path.join(BASE_DIR, "config")
INPUTS_DIR = os.path.join(BASE_DIR, "inputs")
OUTPUTS_DIR = os.path.join(BASE_DIR, "outputs")

# Ensure directories exist
for dir_path in [CONFIG_DIR, INPUTS_DIR, OUTPUTS_DIR]:
    os.makedirs(dir_path, exist_ok=True)

# Load state configuration
STATES_CONFIG_PATH = os.path.join(CONFIG_DIR, "states.json")

def load_states_config() -> Dict[str, Any]:
    """Load the states configuration from JSON file."""
    with open(STATES_CONFIG_PATH, 'r') as f:
        return json.load(f)

def get_state_info(state_code: str) -> Dict[str, str]:
    """
    Get state information by state code (2-letter abbreviation).
    
    Args:
        state_code: 2-letter state code (e.g., 'AZ', 'CA', 'TX')
        
    Returns:
        Dictionary with state name, FIPS code, and lowercase abbreviation
        
    Raises:
        KeyError: If state code is not found
    """
    states_config = load_states_config()
    state_code = state_code.upper()
    
    if state_code not in states_config["states"]:
        available = ", ".join(sorted(states_config["states"].keys()))
        raise KeyError(f"State code '{state_code}' not found. Available: {available}")
    
    return states_config["states"][state_code]

def setup_argument_parser(description: str, stage_name: str = None) -> argparse.ArgumentParser:
    """
    Create a standardized argument parser for all scripts.
    
    Args:
        description: Description of what the script does
        stage_name: Optional stage name for better error messages
        
    Returns:
        Configured ArgumentParser instance
    """
    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        "state",
        help="Two-letter state code (e.g., AZ, CA, TX, SC)"
    )
    
    parser.add_argument(
        "--acs-year",
        type=int,
        default=2023,
        help="ACS data year (default: 2023)"
    )
    
    parser.add_argument(
        "--census-year",
        type=int,
        default=2020,
        help="Census year for TIGER shapefiles (default: 2020)"
    )
    
    if stage_name:
        parser.add_argument(
            "--output-dir",
            help=f"Custom output directory for {stage_name} results"
        )
    
    return parser

def get_state_paths(state_abbr: str, acs_year: int = 2023, census_year: int = 2020) -> Dict[str, str]:
    """
    Generate all standard file paths for a given state.
    
    Args:
        state_abbr: Lowercase state abbreviation (e.g., 'az', 'ca')
        acs_year: ACS data year
        census_year: Census year for TIGER data
        
    Returns:
        Dictionary of standard file paths for the state
    """
    state_fips = get_state_info(state_abbr.upper())["fips"]
    
    # Input directories
    tiger_dir = os.path.join(INPUTS_DIR, f"tiger_{census_year}")
    acs_dir = os.path.join(INPUTS_DIR, f"acs_{acs_year}", state_abbr)
    cvap_dir = os.path.join(INPUTS_DIR, "cvap", "CVAP_2019-2023_ACS_csv_files")
    precincts_dir = os.path.join(INPUTS_DIR, "precincts", state_abbr)
    plans_dir = os.path.join(INPUTS_DIR, "plans", state_abbr)
    
    # Output directory
    state_output_dir = os.path.join(OUTPUTS_DIR, state_abbr)
    os.makedirs(state_output_dir, exist_ok=True)
    
    return {
        # Directories
        "tiger_dir": tiger_dir,
        "acs_dir": acs_dir,
        "cvap_dir": cvap_dir,
        "precincts_dir": precincts_dir,
        "plans_dir": plans_dir,
        "state_output_dir": state_output_dir,
        
        # State identifiers (for compatibility)
        "state_abbr": state_abbr,
        "state_fips": state_fips,
        
        # TIGER shapefiles
        "bg_shapefile": os.path.join(tiger_dir, f"{state_abbr}_bg", f"tl_{census_year}_{state_fips}_bg.shp"),
        "tiger_bg_shp": os.path.join(tiger_dir, f"{state_abbr}_bg", f"tl_{census_year}_{state_fips}_bg.shp"),
        "tabblock_dir": os.path.join(tiger_dir, f"{state_abbr}_tabblock20"),
        
        # ACS data files
        "acs_race_csv": os.path.join(acs_dir, f"{state_abbr}_bg_race_{acs_year}.csv"),
        "acs_income_csv": os.path.join(acs_dir, f"{state_abbr}_bg_income_{acs_year}.csv"),
        
        # CVAP file (national)
        "cvap_blockgr_csv": os.path.join(cvap_dir, "BlockGr.csv"),
        
        # Output files
        "bg_geojson": os.path.join(state_output_dir, f"{state_abbr}_bg_all_data_{acs_year}.geojson"),
        "precinct_geojson": os.path.join(state_output_dir, f"{state_abbr}_precinct_all_pop_{acs_year}.geojson"),
        "dots_geojson": os.path.join(state_output_dir, f"{state_abbr}_dots_pop{str(acs_year)[-2:]}_unit{{dot_unit}}.geojson"),
        "plans_json": os.path.join(state_output_dir, f"{state_abbr}_plans_{{year}}.json"),
        "assignments_json": os.path.join(state_output_dir, f"{state_abbr}_assignments_{{year}}.json"),
        
        # Comparison CSV files
        "pop_comparison_csv": os.path.join(state_output_dir, f"{state_abbr}_population_comparison_{acs_year}.csv"),
        "cvap_comparison_csv": os.path.join(state_output_dir, f"{state_abbr}_cvap_comparison_{acs_year}.csv"),
        "income_comparison_csv": os.path.join(state_output_dir, f"{state_abbr}_income_comparison_{acs_year}.csv"),
    }

def find_acs_file(state_abbr: str, acs_year: int, file_type: str) -> str:
    """
    Find ACS data file with backward compatibility.
    
    Checks for files in new structure first: inputs/acs_{year}/{state_abbr}/{state_abbr}_bg_{type}_{year}.csv
    Falls back to old structure: inputs/acs_{year}/{state_abbr}_bg_{type}_{year}.csv
    
    Args:
        state_abbr: State abbreviation (e.g., 'az')
        acs_year: ACS year
        file_type: 'race' or 'income'
        
    Returns:
        Path to the ACS file
        
    Raises:
        FileNotFoundError: If file not found in either location
    """
    # New structure: inputs/acs_{year}/{state_abbr}/{state_abbr}_bg_{type}_{year}.csv
    new_path = os.path.join(INPUTS_DIR, f"acs_{acs_year}", state_abbr, f"{state_abbr}_bg_{file_type}_{acs_year}.csv")
    
    # Old structure: inputs/acs_{year}/{state_abbr}_bg_{type}_{year}.csv  
    old_path = os.path.join(INPUTS_DIR, f"acs_{acs_year}", f"{state_abbr}_bg_{file_type}_{acs_year}.csv")
    
    if os.path.exists(new_path):
        return new_path
    elif os.path.exists(old_path):
        print(f"⚠ Using ACS {file_type} file from old location: {old_path}")
        print(f"   Consider moving to new structure: {new_path}")
        return old_path
    else:
        raise FileNotFoundError(
            f"ACS {file_type} file not found for {state_abbr.upper()} {acs_year}.\n"
            f"Searched locations:\n"
            f"  - {new_path}\n" 
            f"  - {old_path}\n"
            f"Run Stage 0 first to download the data."
        )

def find_precinct_shapefile(precincts_dir: str) -> str:
    """
    Find the precinct shapefile in the given directory.
    
    Args:
        precincts_dir: Directory containing precinct shapefiles
        
    Returns:
        Path to the first .shp file found
        
    Raises:
        FileNotFoundError: If no shapefile is found or directory doesn't exist
    """
    if not os.path.isdir(precincts_dir):
        raise FileNotFoundError(
            f"Precincts directory not found: {precincts_dir}\n"
            "Create it and place your precinct shapefile there."
        )

    shp_files = [f for f in os.listdir(precincts_dir) if f.lower().endswith(".shp")]
    if not shp_files:
        raise FileNotFoundError(
            f"No .shp files found in {precincts_dir}. "
            "Place your precinct shapefile (with election results) there."
        )
    
    if len(shp_files) > 1:
        print("Warning: multiple .shp files found, using the first one:")
        for s in shp_files:
            print(f" - {s}")

    shp_path = os.path.join(precincts_dir, shp_files[0])
    print(f"Using precinct shapefile: {shp_path}")
    return shp_path

def find_plan_shapefiles(plans_dir: str, state_abbr: str, plan_year: int = 2022) -> Dict[str, str]:
    """
    Find congressional and legislative plan shapefiles.
    
    Args:
        plans_dir: Directory containing plan shapefiles
        state_abbr: State abbreviation
        plan_year: Year of the plans
        
    Returns:
        Dictionary with 'cong' and 'leg' keys pointing to shapefile paths
        
    Raises:
        FileNotFoundError: If required plan directories or shapefiles are not found
    """
    plans = {}
    
    # Congressional plan
    cong_dir = os.path.join(plans_dir, f"{state_abbr}_cong_adopted_{plan_year}")
    if os.path.isdir(cong_dir):
        cong_shps = [f for f in os.listdir(cong_dir) if f.endswith(".shp")]
        if cong_shps:
            plans["cong"] = os.path.join(cong_dir, cong_shps[0])
    
    # Legislative plan
    leg_dir = os.path.join(plans_dir, f"{state_abbr}_sl_adopted_{plan_year}")
    if os.path.isdir(leg_dir):
        leg_shps = [f for f in os.listdir(leg_dir) if f.endswith(".shp")]
        if leg_shps:
            plans["leg"] = os.path.join(leg_dir, leg_shps[0])
    
    if not plans:
        raise FileNotFoundError(
            f"No plan shapefiles found in {plans_dir}. "
            f"Expected directories: {state_abbr}_cong_adopted_{plan_year}, {state_abbr}_sl_adopted_{plan_year}"
        )
    
    return plans

def validate_state_setup(state_code: str, stage: str = None) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Validate that a state is properly configured and return state info and paths.
    
    Args:
        state_code: Two-letter state code
        stage: Optional stage name for better error messages
        
    Returns:
        Tuple of (state_info, state_paths)
        
    Raises:
        KeyError: If state code is invalid
        RuntimeError: If required environment variables are missing
    """
    # Validate state code
    state_info = get_state_info(state_code)
    state_paths = get_state_paths(state_info["abbr"])
    
    # Check for required environment variables based on stage
    if stage in ["stage0", "stage_0", "get_inputs"]:
        import os
        census_api_key = os.environ.get("CENSUS_API_KEY")
        if not census_api_key:
            raise RuntimeError(
                "CENSUS_API_KEY environment variable is required for data collection.\n"
                "Set it in your .env file or environment."
            )
    
    return state_info, state_paths

def print_state_info(state_info: Dict[str, str]):
    """Print formatted state information."""
    print(f"Processing state: {state_info['name']} ({state_info['abbr'].upper()})")
    print(f"FIPS code: {state_info['fips']}")