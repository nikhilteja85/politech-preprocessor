#!/usr/bin/env python3
"""
Stage 4: Compare plans and dots visualization with district statistics.

This script creates side-by-side visualizations showing:
  • Block groups (light dashed outlines)
  • Precinct boundaries (darker solid outlines)
  • Race dot map (from Stage 3)
  • Congressional plan outlines
  • State legislative plan outlines
  • District-level demographic statistics (population, income, CVAP)

Usage:
    python run_stage4_comp.py <STATE_CODE> [options]
    
Examples:
    python run_stage4_comp.py AZ
    python run_stage4_comp.py CA --dot-unit 25 --cong-plan "ca_cong_2022"
    python run_stage4_comp.py TX --acs-year 2022 --leg-plan "tx_leg_2021"
    python run_stage4_comp.py LA --show-stats

Inputs:
  - outputs/<state>/<state>_precinct_all_pop_<year>.geojson (from Stage 2)
  - outputs/<state>/<state>_dots_pop<yy>_unit<X>.geojson (from Stage 3)
  - inputs/tiger_2020/<state>_bg/tl_2020_<fips>_bg.shp (TIGER block groups)
  - inputs/plans/<state>/<cong_plan>/<plan_file>.shp
  - inputs/plans/<state>/<leg_plan>/<plan_file>.shp
  - outputs/assignments.json (from Stage 2)

Outputs:
  - Interactive matplotlib visualization
  - District statistics tables (if --show-stats enabled)
"""

import sys
sys.setrecursionlimit(10000)  # allow deep geometries

import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from typing import Optional, Dict, List

# Force Fiona backend for all reads to avoid geometry issues
gpd.options.io_engine = "fiona"

from common import (
    setup_argument_parser,
    validate_state_setup,
    get_state_paths,
    print_state_info
)

# ===================== CONSTANTS =====================

WORK_CRS = "EPSG:5070"   # for alignment
PLOT_CRS = "EPSG:3857"   # web-mercator for nicer plots

# High-contrast dot colors (must match Stage 3)
DOT_COLORS = {
    "white":       "#4daf4a",  # green
    "black":       "#984ea3",  # purple
    "asian":       "#377eb8",  # blue
    "hispanic":    "#ff7f00",  # orange
    "native":      "#a65628",  # brown
    "nhpi":        "#a65628",  # brown
    "other":       "#f781bf",  # pink
    "two_or_more": "#999999",  # light grey
}

# ===================== HELPERS =====================

def load_layer_simple(path: str, name: str) -> gpd.GeoDataFrame:
    """Load a geographic layer with error handling."""
    if not os.path.exists(path):
        raise FileNotFoundError(f"{name} not found at {path}")
    print(f"Loading {name}: {path}")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        print(f"⚠ {name} has no CRS; assuming EPSG:4326")
        gdf.crs = "EPSG:4326"
    
    # Map TIGER column names to standard DISTRICT column for plan shapefiles
    district_col_map = {
        'DISTRICT': 'DISTRICT',  # Already standardized
        'CD119FP': 'DISTRICT',   # TIGER congressional
        'CD118FP': 'DISTRICT',
        'CD117FP': 'DISTRICT',
        'CD116FP': 'DISTRICT',
        'SLDLST': 'DISTRICT',    # TIGER state legislative lower
        'SLDUST': 'DISTRICT',    # TIGER state legislative upper
    }
    
    for tiger_col in district_col_map:
        if tiger_col in gdf.columns and tiger_col != 'DISTRICT':
            gdf['DISTRICT'] = gdf[tiger_col]
            break
    
    return gdf


def prep_for_plot(gdf: Optional[gpd.GeoDataFrame]) -> Optional[gpd.GeoDataFrame]:
    """Reproject GeoDataFrame to plotting CRS."""
    if gdf is None:
        return None
    return gdf.to_crs(WORK_CRS).to_crs(PLOT_CRS)


def add_district_labels(ax, districts_gdf, label_col="DISTRICT"):
    """Add district number labels to the center of each district."""
    if label_col not in districts_gdf.columns:
        print(f"⚠ District label column '{label_col}' not found")
        return
    for _, row in districts_gdf.iterrows():
        if row.geometry is None or row.geometry.is_empty:
            continue
        centroid = row.geometry.representative_point()
        txt = ax.text(
            centroid.x,
            centroid.y,
            str(row[label_col]),
            fontsize=8,
            ha="center",
            va="center",
            color="black",
            alpha=0.7,
            weight="bold"
        )
        txt.set_clip_on(True)


def load_dots(state_paths: dict, dot_unit: int, acs_year: int) -> Optional[gpd.GeoDataFrame]:
    """Load dot density data from Stage 3 output."""
    # Try combined file first
    dots_combined = state_paths["dots_geojson"].format(dot_unit=dot_unit)
    
    if os.path.exists(dots_combined):
        print(f"Loading dot layer (combined): {dots_combined}")
        dots = load_layer_simple(dots_combined, "combined dots")
        if "group" not in dots.columns:
            print("⚠ Combined dots file has no 'group' column; treating all as 'unknown'")
            dots["group"] = "unknown"
        return dots

    # Fall back to per-group files
    year_suffix = str(acs_year)[-2:]
    state_abbr = state_paths["state_abbr"].lower()
    state_out_dir = state_paths["state_output_dir"]
    
    dot_group_files = {
        "white":       os.path.join(state_out_dir, f"{state_abbr}_dots_pop{year_suffix}_unit{dot_unit}_white.geojson"),
        "black":       os.path.join(state_out_dir, f"{state_abbr}_dots_pop{year_suffix}_unit{dot_unit}_black.geojson"),
        "asian":       os.path.join(state_out_dir, f"{state_abbr}_dots_pop{year_suffix}_unit{dot_unit}_asian.geojson"),
        "hispanic":    os.path.join(state_out_dir, f"{state_abbr}_dots_pop{year_suffix}_unit{dot_unit}_hispanic.geojson"),
        "native":      os.path.join(state_out_dir, f"{state_abbr}_dots_pop{year_suffix}_unit{dot_unit}_native.geojson"),
        "nhpi":        os.path.join(state_out_dir, f"{state_abbr}_dots_pop{year_suffix}_unit{dot_unit}_nhpi.geojson"),
        "other":       os.path.join(state_out_dir, f"{state_abbr}_dots_pop{year_suffix}_unit{dot_unit}_other.geojson"),
        "two_or_more": os.path.join(state_out_dir, f"{state_abbr}_dots_pop{year_suffix}_unit{dot_unit}_two_or_more.geojson"),
    }

    pieces = []
    crs = None
    for group, path in dot_group_files.items():
        if not os.path.exists(path):
            print(f"⚠ Per-group dots file missing for {group}: {path}")
            continue
        g = load_layer_simple(path, f"dots for {group}")
        g["group"] = group
        pieces.append(g)
        if crs is None:
            crs = g.crs

    if not pieces:
        print("⚠ No dots files found.")
        return None

    dots = gpd.GeoDataFrame(pd.concat(pieces, ignore_index=True), crs=crs)
    return dots


def plot_dots(ax, dots_plot: Optional[gpd.GeoDataFrame]):
    """Plot dot density points with group-specific colors."""
    if dots_plot is None or dots_plot.empty:
        return

    if "group" not in dots_plot.columns:
        dots_plot.plot(
            ax=ax,
            markersize=2,
            color="#444444",
            alpha=0.6,
            linewidth=0,
            zorder=5,
        )
        return

    for group, color in DOT_COLORS.items():
        subset = dots_plot[dots_plot["group"] == group]
        if subset.empty:
            continue
        subset.plot(
            ax=ax,
            markersize=2,
            color=color,
            alpha=0.8,
            linewidth=0,
            zorder=5,
        )


def find_plan_file(plans_dir: str, plan_name: str, plan_type: str) -> Optional[str]:
    """Find the shapefile for a redistricting plan."""
    plan_dir = os.path.join(plans_dir, plan_name)
    if not os.path.exists(plan_dir):
        print(f"⚠ {plan_type} plan directory not found: {plan_dir}")
        return None
        
    # Look for .shp files in the plan directory
    shp_files = [f for f in os.listdir(plan_dir) if f.endswith('.shp')]
    
    if not shp_files:
        print(f"⚠ No .shp files found in {plan_type} plan directory: {plan_dir}")
        return None
        
    if len(shp_files) == 1:
        return os.path.join(plan_dir, shp_files[0])
    
    # Multiple shapefiles - try to pick the most likely one
    preferred_names = ['map', 'district', 'plan', 'approved', 'official', 'adopted']
    for pref in preferred_names:
        for shp in shp_files:
            if pref.lower() in shp.lower():
                return os.path.join(plan_dir, shp)
    
    # Fallback to first file
    print(f"⚠ Multiple .shp files in {plan_type} plan directory, using: {shp_files[0]}")
    return os.path.join(plan_dir, shp_files[0])


def compute_district_stats(
    precincts: gpd.GeoDataFrame,
    assignments_file: str,
    plan_id: str,
    state_abbr: str,
    acs_year: int
) -> Optional[pd.DataFrame]:
    """
    Compute district-level statistics from precinct data and assignments.
    
    Returns DataFrame with columns: district_id, total_pop, median_income, 
    cvap_total, white_pop, black_pop, hispanic_pop, asian_pop, etc.
    """
    # Load assignments
    if not os.path.exists(assignments_file):
        print(f"⚠ Assignments file not found: {assignments_file}")
        return None
    
    with open(assignments_file, 'r') as f:
        all_assignments = json.load(f)
    
    # Filter for this plan
    plan_assignments = [
        a for a in all_assignments 
        if a.get('plan_id') == plan_id and a.get('state') == state_abbr.upper()
    ]
    
    if not plan_assignments:
        print(f"⚠ No assignments found for plan {plan_id}")
        return None
    
    # Create assignments DataFrame
    assignments_df = pd.DataFrame(plan_assignments)
    
    # Merge precincts with assignments
    precincts_with_district = precincts.merge(
        assignments_df[['precinct_id', 'district_id']],
        left_on='UNIQUE_ID',
        right_on='precinct_id',
        how='inner'
    )
    
    if precincts_with_district.empty:
        print(f"⚠ No matching precincts found for plan {plan_id}")
        return None
    
    # Get year suffix for column names
    year_suffix = str(acs_year)[-2:]
    
    # Define demographic columns to aggregate
    demo_cols = {
        f'TOT_POP{year_suffix}': 'total_pop',
        f'WHT_POP{year_suffix}': 'white_pop',
        f'BLK_POP{year_suffix}': 'black_pop',
        f'HSP_POP{year_suffix}': 'hispanic_pop',
        f'ASN_POP{year_suffix}': 'asian_pop',
        f'AIA_POP{year_suffix}': 'native_pop',
        f'HPI_POP{year_suffix}': 'nhpi_pop',
        f'OTH_POP{year_suffix}': 'other_pop',
        f'2OM_POP{year_suffix}': 'two_or_more_pop',
        f'WHT_CVAP{year_suffix}': 'white_cvap',
        f'BLK_CVAP{year_suffix}': 'black_cvap',
        f'HSP_CVAP{year_suffix}': 'hispanic_cvap',
        f'ASN_CVAP{year_suffix}': 'asian_cvap',
    }
    
    # Filter to only columns that exist
    available_cols = {k: v for k, v in demo_cols.items() if k in precincts_with_district.columns}
    
    # Group by district and sum
    agg_dict = {col: 'sum' for col in available_cols.keys()}
    
    # Add total households if available
    households_col = f'TOT_HOUS{year_suffix}'
    if households_col in precincts_with_district.columns:
        agg_dict[households_col] = 'sum'
    
    district_stats = precincts_with_district.groupby('district_id').agg(agg_dict).reset_index()
    
    # Rename columns
    for old_col, new_col in available_cols.items():
        if old_col in district_stats.columns:
            district_stats.rename(columns={old_col: new_col}, inplace=True)
    
    # Rename households column
    if households_col in district_stats.columns:
        district_stats.rename(columns={households_col: 'total_households'}, inplace=True)
    
    # Add election results if available
    election_cols = [col for col in precincts_with_district.columns if col.startswith('G') and any(yr in col for yr in ['20', '22', '24'])]
    if election_cols:
        election_agg = precincts_with_district.groupby('district_id')[election_cols].sum().reset_index()
        district_stats = district_stats.merge(election_agg, on='district_id', how='left')
    
    # Calculate total CVAP
    cvap_cols = [col for col in district_stats.columns if col.endswith('_cvap')]
    if cvap_cols:
        district_stats['cvap_total'] = district_stats[cvap_cols].sum(axis=1)
    
    # Sort by district_id
    district_stats = district_stats.sort_values('district_id')
    
    return district_stats


def print_district_stats(stats: pd.DataFrame, plan_name: str):
    """Print district statistics in a formatted table."""
    if stats is None or stats.empty:
        return
    
    # Check for election results
    election_cols = [col for col in stats.columns if col.startswith('G') and any(yr in col for yr in ['20', '22', '24'])]
    has_elections = len(election_cols) > 0
    
    print(f"\n{'='*140 if has_elections else '='*120}")
    print(f"District Statistics for {plan_name}")
    print(f"{'='*140 if has_elections else '='*120}")
    
    # Print header
    if has_elections:
        # Find party columns (match actual patterns like G24PREDHAR, G24PRERTRU)
        dem_cols = [col for col in election_cols if 'DHAR' in col or 'DEM' in col.upper()]
        rep_cols = [col for col in election_cols if 'RTRU' in col or 'REP' in col.upper() or 'TRU' in col]
        print(f"{'District':<10} {'Total Pop':>12} {'Households':>12} {'CVAP':>12} {'White %':>9} {'Black %':>9} {'Hisp %':>9} {'Asian %':>9} {'Dem Votes':>12} {'Rep Votes':>12}")
    else:
        print(f"{'District':<10} {'Total Pop':>12} {'Households':>12} {'CVAP':>12} {'White %':>9} {'Black %':>9} {'Hispanic %':>10} {'Asian %':>10}")
    print(f"{'-'*140 if has_elections else '-'*120}")
    
    # Print each district
    for _, row in stats.iterrows():
        district = row['district_id']
        if district == -1:
            district = "ZZZ"
        
        total_pop = int(row.get('total_pop', 0))
        total_households = int(row.get('total_households', 0))
        cvap = int(row.get('cvap_total', 0))
        
        # Calculate percentages
        white_pct = (row.get('white_pop', 0) / total_pop * 100) if total_pop > 0 else 0
        black_pct = (row.get('black_pop', 0) / total_pop * 100) if total_pop > 0 else 0
        hispanic_pct = (row.get('hispanic_pop', 0) / total_pop * 100) if total_pop > 0 else 0
        asian_pct = (row.get('asian_pop', 0) / total_pop * 100) if total_pop > 0 else 0
        
        if has_elections:
            dem_votes = int(sum(row.get(col, 0) for col in dem_cols))
            rep_votes = int(sum(row.get(col, 0) for col in rep_cols))
            print(f"{str(district):<10} {total_pop:>12,} {total_households:>12,} {cvap:>12,} {white_pct:>8.1f}% {black_pct:>8.1f}% {hispanic_pct:>8.1f}% {asian_pct:>8.1f}% {dem_votes:>12,} {rep_votes:>12,}")
        else:
            print(f"{str(district):<10} {total_pop:>12,} {total_households:>12,} {cvap:>12,} {white_pct:>8.1f}% {black_pct:>8.1f}% {hispanic_pct:>9.1f}% {asian_pct:>9.1f}%")
    
    # Print totals
    print(f"{'-'*140 if has_elections else '-'*120}")
    total_pop_all = int(stats['total_pop'].sum())
    total_cvap_all = int(stats.get('cvap_total', pd.Series([0])).sum())
    total_households_all = int(stats.get('total_households', pd.Series([0])).sum())
    
    if has_elections:
        total_dem = int(sum(stats[col].sum() for col in dem_cols))
        total_rep = int(sum(stats[col].sum() for col in rep_cols))
        print(f"{'TOTAL':<10} {total_pop_all:>12,} {total_households_all:>12,} {total_cvap_all:>12,} {'':>9} {'':>9} {'':>9} {'':>9} {total_dem:>12,} {total_rep:>12,}")
    else:
        print(f"{'TOTAL':<10} {total_pop_all:>12,} {total_households_all:>12,} {total_cvap_all:>12,}")
    print(f"{'='*140 if has_elections else '='*120}\n")


def main():
    """Main function that processes command line arguments and runs the stage."""
    # Parse command line arguments
    parser = setup_argument_parser(
        description="Create comparison visualization of demographic dots and redistricting plans.",
        stage_name="Stage 4"
    )
    
    parser.add_argument(
        "--dot-unit",
        type=int,
        default=50,
        help="People per dot (must match Stage 3, default: 50)"
    )
    
    parser.add_argument(
        "--cong-plan",
        type=str,
        help="Congressional plan directory name (auto-detect if not specified)"
    )
    
    parser.add_argument(
        "--sldl-plan", 
        type=str,
        help="State Legislative Lower plan directory name (auto-detect if not specified)"
    )
    
    parser.add_argument(
        "--sldu-plan", 
        type=str,
        help="State Legislative Upper plan directory name (auto-detect if not specified)"
    )
    
    parser.add_argument(
        "--show-stats",
        action="store_true",
        help="Display district-level statistics (population, income, CVAP)"
    )

    args = parser.parse_args()

    # Validate state and get configuration
    state_info, state_paths = validate_state_setup(args.state)
    print_state_info(state_info)

    # 1. Load block groups (TIGER shapefile)
    bg_tiger_path = state_paths["tiger_bg_shp"]
    if not os.path.exists(bg_tiger_path):
        raise FileNotFoundError(f"TIGER block groups not found: {bg_tiger_path}")
    bg = load_layer_simple(bg_tiger_path, "BG TIGER layer")

    # 2. Load precincts (from Stage 2)
    precinct_path = state_paths["precinct_geojson"]
    if not os.path.exists(precinct_path):
        raise FileNotFoundError(
            f"Precinct data not found: {precinct_path}\n"
            "Run Stage 2 to generate it first."
        )
    precincts = load_layer_simple(precinct_path, "Precinct layer")

    # 3. Load dots (from Stage 3)
    dots = load_dots(state_paths, args.dot_unit, state_paths["acs_year"])
    if dots is not None and "group" in dots.columns:
        print("\nDot counts by group:")
        print(dots["group"].value_counts())

    # 4. Load redistricting plans
    plans_dir = state_paths["plans_dir"]
    
    # Auto-detect or use specified congressional plan
    cong = None
    if args.cong_plan:
        cong_path = find_plan_file(plans_dir, args.cong_plan, "Congressional")
        if cong_path:
            cong = load_layer_simple(cong_path, "Congressional plan")
    else:
        # Auto-detect congressional plans
        if os.path.exists(plans_dir):
            for item in os.listdir(plans_dir):
                if 'cong' in item.lower() and 'adopted' in item.lower():
                    cong_path = find_plan_file(plans_dir, item, "Congressional")
                    if cong_path:
                        cong = load_layer_simple(cong_path, "Congressional plan")
                        break

    # Auto-detect or use specified legislative plans (can have multiple chambers)
    leg_plans = {}
    
    # Check for SLDL (State Legislative Lower)
    if hasattr(args, 'sldl_plan') and args.sldl_plan:
        sldl_path = find_plan_file(plans_dir, args.sldl_plan, "State Legislative Lower")
        if sldl_path:
            leg_plans['sldl'] = load_layer_simple(sldl_path, "State Legislative Lower plan")
    else:
        # Auto-detect SLDL
        if os.path.exists(plans_dir):
            for item in os.listdir(plans_dir):
                if 'sldl' in item.lower() and 'adopted' in item.lower():
                    sldl_path = find_plan_file(plans_dir, item, "State Legislative Lower")
                    if sldl_path:
                        leg_plans['sldl'] = load_layer_simple(sldl_path, "State Legislative Lower plan")
                        break
    
    # Check for SLDU (State Legislative Upper)
    if hasattr(args, 'sldu_plan') and args.sldu_plan:
        sldu_path = find_plan_file(plans_dir, args.sldu_plan, "State Legislative Upper")
        if sldu_path:
            leg_plans['sldu'] = load_layer_simple(sldu_path, "State Legislative Upper plan")
    else:
        # Auto-detect SLDU
        if os.path.exists(plans_dir):
            for item in os.listdir(plans_dir):
                if 'sldu' in item.lower() and 'adopted' in item.lower():
                    sldu_path = find_plan_file(plans_dir, item, "State Legislative Upper")
                    if sldu_path:
                        leg_plans['sldu'] = load_layer_simple(sldu_path, "State Legislative Upper plan")
                        break
    
    # Fallback: check for generic 'sl' (unicameral legislature)
    if not leg_plans and os.path.exists(plans_dir):
        for item in os.listdir(plans_dir):
            if item.lower().startswith(state_info['abbr'] + '_sl_') and 'sldl' not in item.lower() and 'sldu' not in item.lower():
                sl_path = find_plan_file(plans_dir, item, "State Legislative")
                if sl_path:
                    leg_plans['sl'] = load_layer_simple(sl_path, "State Legislative plan")
                    break

    if cong is None and not leg_plans:
        print("⚠ No redistricting plans found. Visualization will only show demographics.")

    # 4a. Compute and display district statistics if requested
    if args.show_stats:
        assignments_file = os.path.join(
            os.path.dirname(state_paths["state_output_dir"]),
            "assignments.json"
        )
        plans_file = os.path.join(
            os.path.dirname(state_paths["state_output_dir"]),
            "plans.json"
        )
        
        # Load plans metadata to get plan IDs
        plan_metadata = {}
        if os.path.exists(plans_file):
            with open(plans_file, 'r') as f:
                all_plans = json.load(f)
                for plan in all_plans:
                    if plan.get('state') == state_info['abbr'].upper():
                        plan_metadata[plan.get('chamber', '').lower()] = {
                            'plan_id': plan['plan_id'],
                            'name': plan['name']
                        }
        
        # Compute stats for congressional plan
        if cong is not None and 'cong' in plan_metadata:
            stats = compute_district_stats(
                precincts,
                assignments_file,
                plan_metadata['cong']['plan_id'],
                state_info['abbr'],
                state_paths['acs_year']
            )
            if stats is not None:
                print_district_stats(stats, plan_metadata['cong']['name'])
        
        # Compute stats for legislative plans
        for chamber in leg_plans.keys():
            if chamber in plan_metadata:
                stats = compute_district_stats(
                    precincts,
                    assignments_file,
                    plan_metadata[chamber]['plan_id'],
                    state_info['abbr'],
                    state_paths['acs_year']
                )
                if stats is not None:
                    print_district_stats(stats, plan_metadata[chamber]['name'])

    # 5. Reproject for plotting
    bg_plot = prep_for_plot(bg)
    precincts_plot = prep_for_plot(precincts)
    cong_plot = prep_for_plot(cong)
    leg_plots = {chamber: prep_for_plot(plan) for chamber, plan in leg_plans.items()}
    dots_plot = prep_for_plot(dots)

    # 6. Create separate figure windows for each map type
    # This provides the best zoom/pan experience
    
    def draw_base(ax):
        """Draw the base layers (block groups, precincts, dots)."""
        # Block group boundaries – hidden
        # (no plot)
        # Precinct boundaries – thinner, dark brown, solid
        precincts_plot.boundary.plot(
            ax=ax,
            linewidth=1.25,
            edgecolor="#5C4033",  # dark brown
            alpha=0.85,
            zorder=2,
        )
        # Dots
        plot_dots(ax, dots_plot)

    # Draw congressional plan in separate window
    if cong_plot is not None:
        fig_cong = plt.figure(figsize=(14, 12))
        ax_cong = fig_cong.add_subplot(111)
        ax_cong.set_title(
            f"{state_info['name']}: Congressional Districts",
            fontsize=14,
        )
        draw_base(ax_cong)
        cong_plot.boundary.plot(
            ax=ax_cong,
            linewidth=2.0,
            edgecolor="red",
            alpha=0.95,
            zorder=4,
        )
        add_district_labels(ax_cong, cong_plot, "DISTRICT")
        ax_cong.set_axis_off()
        
        # Add legend
        from matplotlib.lines import Line2D
        from matplotlib.patches import Patch
        legend_elements = [
            Line2D([0], [0], color='red', linewidth=2, label='Districts'),
            Line2D([0], [0], color='#5C4033', linewidth=1.25, label='Precincts'),
            Patch(facecolor='#984ea3', label='Black'),
            Patch(facecolor='#ff7f00', label='Hispanic'),
            Patch(facecolor='#4daf4a', label='White'),
            Patch(facecolor='#377eb8', label='Asian'),
        ]
        ax_cong.legend(handles=legend_elements, loc='lower right', fontsize=8, framealpha=0.95, bbox_to_anchor=(1.0, -0.15))
        
        # Set extent
        minx, miny, maxx, maxy = bg_plot.total_bounds
        ax_cong.set_xlim(minx, maxx)
        ax_cong.set_ylim(miny, maxy)
        fig_cong.tight_layout()

    # Draw legislative plans in separate windows
    chamber_names = {
        'sldl': 'State House (SLDL)',
        'sldu': 'State Senate (SLDU)',
        'sl': 'Legislative Districts'
    }
    
    for chamber, leg_plot in leg_plots.items():
        if leg_plot is not None:
            fig_leg = plt.figure(figsize=(14, 12))
            ax_leg = fig_leg.add_subplot(111)
            chamber_name = chamber_names.get(chamber, chamber.upper())
            ax_leg.set_title(
                f"{state_info['name']}: {chamber_name}",
                fontsize=14,
            )
            draw_base(ax_leg)
            leg_plot.boundary.plot(
                ax=ax_leg,
                linewidth=2.0,
                edgecolor="blue",
                alpha=0.95,
                zorder=4,
            )
            add_district_labels(ax_leg, leg_plot, "DISTRICT")
            ax_leg.set_axis_off()
            
            # Add legend
            from matplotlib.lines import Line2D
            from matplotlib.patches import Patch
            legend_elements = [
                Line2D([0], [0], color='blue', linewidth=2, label='Districts'),
                Line2D([0], [0], color='#5C4033', linewidth=1.25, label='Precincts'),
                Patch(facecolor='#984ea3', label='Black'),
                Patch(facecolor='#ff7f00', label='Hispanic'),
                Patch(facecolor='#4daf4a', label='White'),
                Patch(facecolor='#377eb8', label='Asian'),
            ]
            ax_leg.legend(handles=legend_elements, loc='lower right', fontsize=8, framealpha=0.95, bbox_to_anchor=(1.0, -0.15))
            
            # Set extent
            minx, miny, maxx, maxy = bg_plot.total_bounds
            ax_leg.set_xlim(minx, maxx)
            ax_leg.set_ylim(miny, maxy)
            fig_leg.tight_layout()

    # If no plans, just show demographics in one window
    if cong is None and not leg_plans:
        fig_demo = plt.figure(figsize=(14, 12))
        ax_demo = fig_demo.add_subplot(111)
        ax_demo.set_title(
            f"{state_info['name']}: Demographics",
            fontsize=14,
        )
        draw_base(ax_demo)
        ax_demo.set_axis_off()
        
        # Add legend
        from matplotlib.lines import Line2D
        from matplotlib.patches import Patch
        legend_elements = [
            Line2D([0], [0], color='#5C4033', linewidth=1.25, label='Precincts'),
            Patch(facecolor='#984ea3', label='Black'),
            Patch(facecolor='#ff7f00', label='Hispanic'),
            Patch(facecolor='#4daf4a', label='White'),
            Patch(facecolor='#377eb8', label='Asian'),
        ]
        ax_demo.legend(handles=legend_elements, loc='lower right', fontsize=8, framealpha=0.95, bbox_to_anchor=(1.0, -0.15))
        
        # Set extent
        minx, miny, maxx, maxy = bg_plot.total_bounds
        ax_demo.set_xlim(minx, maxx)
        ax_demo.set_ylim(miny, maxy)
        fig_demo.tight_layout()

    # Show all figure windows
    plt.show()

    print(f"\n✅ Stage 4 completed! Interactive visualization displayed.")
    print("\nPipeline Summary:")
    print("  Stage 0: Downloaded TIGER shapefiles and ACS data ✓")
    print("  Stage 1: Aggregated demographics to precincts ✓") 
    print("  Stage 2: Processed redistricting plans ✓")
    print("  Stage 3: Generated race dot maps ✓")
    print("  Stage 4: Created comparison visualization ✓")


if __name__ == "__main__":
    main()