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
        default=None,
        help="ACS data year (default: auto-detect most recent available)"
    )
    
    parser.add_argument(
        "--census-year",
        type=int,
        default=None,
        help="Census year for TIGER shapefiles (default: auto-detect most recent available)"
    )
    
    if stage_name:
        parser.add_argument(
            "--output-dir",
            help=f"Custom output directory for {stage_name} results"
        )
    
    return parser

def get_state_paths(state_abbr: str, acs_year: int = None, census_year: int = None) -> Dict[str, str]:
    """
    Generate all standard file paths for a given state.
    
    Args:
        state_abbr: Lowercase state abbreviation (e.g., 'az', 'ca')
        acs_year: ACS data year (auto-detects if None)
        census_year: Census year for TIGER data (auto-detects if None)
        
    Returns:
        Dictionary of standard file paths for the state
    """
    state_fips = get_state_info(state_abbr.upper())["fips"]
    
    # Auto-detect years if not provided
    if acs_year is None:
        available_acs_years = detect_available_acs_years(state_abbr)
        if available_acs_years:
            acs_year = available_acs_years[0]  # Use most recent
            print(f"📅 Auto-detected ACS year: {acs_year}")
        else:
            # Fallback to 2023 if no data found (e.g., for Stage 0 download)
            acs_year = 2023
    
    if census_year is None:
        available_tiger_years = detect_available_tiger_years()
        if available_tiger_years:
            census_year = available_tiger_years[0]  # Use most recent
            print(f"📅 Auto-detected TIGER year: {census_year}")
        else:
            # Fallback to 2020 if no data found (e.g., for Stage 0 download)
            census_year = 2020
    
    # Input directories
    tiger_dir = os.path.join(INPUTS_DIR, f"tiger_{census_year}")
    acs_dir = os.path.join(INPUTS_DIR, f"acs_{acs_year}", state_abbr)
    cvap_dir = os.path.join(INPUTS_DIR, "cvap", "CVAP_2019-2023_ACS_csv_files")
    precincts_dir = os.path.join(INPUTS_DIR, "precincts", state_abbr)
    plans_dir = os.path.join(INPUTS_DIR, "plans", state_abbr)
    
    # Output directory
    state_output_dir = os.path.join(OUTPUTS_DIR, state_abbr)
    os.makedirs(state_output_dir, exist_ok=True)
    
    # Ensure outputs directory exists
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    
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
        
        # Years used
        "acs_year": acs_year,
        "census_year": census_year,
        
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
        
        # Centralized JSON files (all states in one file)
        "plans_json": os.path.join(OUTPUTS_DIR, "plans.json"),
        "assignments_json": os.path.join(OUTPUTS_DIR, "assignments.json"),
        
        # Comparison CSV files
        "pop_comparison_csv": os.path.join(state_output_dir, f"{state_abbr}_population_comparison_{acs_year}.csv"),
        "cvap_comparison_csv": os.path.join(state_output_dir, f"{state_abbr}_cvap_comparison_{acs_year}.csv"),
        "income_comparison_csv": os.path.join(state_output_dir, f"{state_abbr}_income_comparison_{acs_year}.csv"),
    }

def detect_available_acs_years(state_abbr: str = None) -> list:
    """
    Detect available ACS years in the inputs directory.
    
    Args:
        state_abbr: Optional state abbreviation to check for state-specific data
        
    Returns:
        List of available years (sorted, most recent first)
    """
    import re
    years = []
    
    if not os.path.exists(INPUTS_DIR):
        return years
    
    # Find all acs_* directories
    for item in os.listdir(INPUTS_DIR):
        match = re.match(r'acs_(\d{4})$', item)
        if match:
            year = int(match.group(1))
            acs_dir = os.path.join(INPUTS_DIR, item)
            
            # If state specified, check if data exists for that state
            if state_abbr:
                # Check new structure
                state_dir = os.path.join(acs_dir, state_abbr)
                if os.path.isdir(state_dir) and any(f.endswith('.csv') for f in os.listdir(state_dir)):
                    years.append(year)
                    continue
                # Check old structure
                if any(f.startswith(f"{state_abbr}_bg_") and f.endswith('.csv') for f in os.listdir(acs_dir)):
                    years.append(year)
            else:
                years.append(year)
    
    return sorted(years, reverse=True)


def detect_available_tiger_years() -> list:
    """
    Detect available TIGER years in the inputs directory.
    
    Returns:
        List of available years (sorted, most recent first)
    """
    import re
    years = []
    
    if not os.path.exists(INPUTS_DIR):
        return years
    
    # Find all tiger_* directories
    for item in os.listdir(INPUTS_DIR):
        match = re.match(r'tiger_(\d{4})$', item)
        if match:
            years.append(int(match.group(1)))
    
    return sorted(years, reverse=True)


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

def detect_available_plan_years(plans_dir: str, state_abbr: str) -> Dict[str, list]:
    """
    Detect available plan years for a state by scanning plan directories.
    
    Args:
        plans_dir: Directory containing plan shapefiles
        state_abbr: State abbreviation
        
    Returns:
        Dictionary with chamber keys (e.g., 'cong', 'sldl', 'sldu', 'sl') mapping to lists of available years
    """
    import re
    years = {}
    
    if not os.path.isdir(plans_dir):
        return years
    
    for item in os.listdir(plans_dir):
        item_path = os.path.join(plans_dir, item)
        if not os.path.isdir(item_path):
            continue
        
        # Match any chamber pattern: {state}_{chamber}_adopted_{year}
        # Common chambers: cong, sl, sldl, sldu
        match = re.match(rf'{state_abbr}_(cong|sl|sldl|sldu)_adopted_(\d{{4}})(?:_.*)?$', item)
        if match:
            chamber = match.group(1)
            year = int(match.group(2))
            # Check if it has shapefiles
            if any(f.endswith('.shp') for f in os.listdir(item_path)):
                if chamber not in years:
                    years[chamber] = []
                years[chamber].append(year)
    
    # Sort years (most recent first) for each chamber
    for chamber in years:
        years[chamber].sort(reverse=True)
    
    return years


def find_plan_shapefiles(plans_dir: str, state_abbr: str, plan_year: int = None) -> Dict[str, str]:
    """
    Find all available plan shapefiles for all chambers (congressional, legislative upper/lower, etc.).
    
    Args:
        plans_dir: Directory containing plan shapefiles
        state_abbr: State abbreviation
        plan_year: Year of the plans (optional - will auto-detect if not provided)
        
    Returns:
        Dictionary with chamber keys (e.g., 'cong', 'sldl', 'sldu') pointing to shapefile paths, and 'year' key
        
    Raises:
        FileNotFoundError: If no plan directories or shapefiles are found
    """
    import re
    plans = {}
    
    # Auto-detect year if not provided
    if plan_year is None:
        available_years = detect_available_plan_years(plans_dir, state_abbr)
        
        if not available_years:
            raise FileNotFoundError(
                f"No plan shapefiles found in {plans_dir}.\n"
                f"Expected directories like: {state_abbr}_cong_adopted_YYYY, {state_abbr}_sldl_adopted_YYYY, etc."
            )
        
        # Get all unique years across all chambers
        all_years = sorted(set(year for years_list in available_years.values() for year in years_list), reverse=True)
        plan_year = all_years[0]  # Use most recent year
        
        print(f"📅 Auto-detected plan year: {plan_year}")
        chambers_info = ", ".join([f"{chamber}: {years}" for chamber, years in available_years.items()])
        print(f"   Available years by chamber: {chambers_info}")
    
    # Scan for all chamber directories matching the pattern
    if os.path.isdir(plans_dir):
        for item in os.listdir(plans_dir):
            # Match pattern: {state}_{chamber}_adopted_{year} (with optional suffix like _cd119)
            match = re.match(rf'{state_abbr}_(cong|sl|sldl|sldu)_adopted_{plan_year}(?:_.*)?$', item)
            if match:
                chamber = match.group(1)
                chamber_dir = os.path.join(plans_dir, item)
                if os.path.isdir(chamber_dir):
                    shp_files = [f for f in os.listdir(chamber_dir) if f.endswith(".shp")]
                    if shp_files:
                        plans[chamber] = os.path.join(chamber_dir, shp_files[0])
    
    if not plans:
        raise FileNotFoundError(
            f"No plan shapefiles found in {plans_dir} for year {plan_year}.\n"
            f"Expected directories: {state_abbr}_{{chamber}}_adopted_{plan_year}"
        )
    
    plans['year'] = plan_year  # Include detected/provided year
    return plans

def validate_state_setup(state_code: str, stage: str = None, acs_year: int = None, census_year: int = None) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Validate that a state is properly configured and return state info and paths.
    
    Args:
        state_code: Two-letter state code
        stage: Optional stage name for better error messages
        acs_year: ACS year (None for auto-detect)
        census_year: Census year (None for auto-detect)
        
    Returns:
        Tuple of (state_info, state_paths)
        
    Raises:
        KeyError: If state code is invalid
        RuntimeError: If required environment variables are missing
    """
    # Validate state code
    state_info = get_state_info(state_code)
    state_paths = get_state_paths(state_info["abbr"], acs_year=acs_year, census_year=census_year)
    
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