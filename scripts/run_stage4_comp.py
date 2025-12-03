#!/usr/bin/env python3
"""
Stage 4: Compare plans and dots visualization.

This script creates side-by-side visualizations showing:
  • Block groups (light dashed outlines)
  • Precinct boundaries (darker solid outlines)
  • Race dot map (from Stage 3)
  • Congressional plan outlines
  • State legislative plan outlines

Usage:
    python run_stage4_comp.py <STATE_CODE> [options]
    
Examples:
    python run_stage4_comp.py AZ
    python run_stage4_comp.py CA --dot-unit 25 --cong-plan "ca_cong_2022"
    python run_stage4_comp.py TX --acs-year 2022 --leg-plan "tx_leg_2021"

Inputs:
  - outputs/<state>/<state>_precinct_all_pop_<year>.geojson (from Stage 2)
  - outputs/<state>/<state>_dots_pop<yy>_unit<X>.geojson (from Stage 3)
  - inputs/tiger_2020/<state>_bg/tl_2020_<fips>_bg.shp (TIGER block groups)
  - inputs/plans/<state>/<cong_plan>/<plan_file>.shp
  - inputs/plans/<state>/<leg_plan>/<plan_file>.shp

Outputs:
  - Interactive matplotlib visualization
"""

import sys
sys.setrecursionlimit(10000)  # allow deep geometries

import os
import pandas as pd
import matplotlib.pyplot as plt
import geopandas as gpd
from typing import Optional

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
    "white":       "#d9d9d9",
    "black":       "#000000", 
    "asian":       "#377eb8",
    "hispanic":    "#e41a1c",
    "native":      "#4daf4a",
    "nhpi":        "#ff7f00",
    "other":       "#984ea3",
    "two_or_more": "#a65628",
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
        ax.text(
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

    # 5. Reproject for plotting
    bg_plot = prep_for_plot(bg)
    precincts_plot = prep_for_plot(precincts)
    cong_plot = prep_for_plot(cong)
    leg_plots = {chamber: prep_for_plot(plan) for chamber, plan in leg_plans.items()}
    dots_plot = prep_for_plot(dots)

    # 6. Create figure - determine number of subplots
    num_plots = 0
    if cong is not None:
        num_plots += 1
    num_plots += len(leg_plans)
    
    if num_plots == 0:
        # No plans, just show base
        fig, axes = plt.subplots(1, 1, figsize=(10, 8), constrained_layout=True)
        axes = [axes]
    elif num_plots == 1:
        fig, axes = plt.subplots(1, 1, figsize=(10, 8), constrained_layout=True)
        axes = [axes]
    elif num_plots == 2:
        fig, axes = plt.subplots(1, 2, figsize=(16, 8), constrained_layout=True, sharex=True, sharey=True)
    elif num_plots == 3:
        fig, axes = plt.subplots(1, 3, figsize=(24, 8), constrained_layout=True, sharex=True, sharey=True)
    else:
        # More than 3, use grid
        ncols = min(3, num_plots)
        nrows = (num_plots + ncols - 1) // ncols
        fig, axes = plt.subplots(nrows, ncols, figsize=(8*ncols, 8*nrows), constrained_layout=True, sharex=True, sharey=True)
        axes = axes.flatten() if num_plots > 1 else [axes]

    def draw_base(ax):
        """Draw the base layers (block groups, precincts, dots)."""
        # Block group boundaries – light, dashed
        bg_plot.boundary.plot(
            ax=ax,
            linewidth=0.2,
            edgecolor="#cccccc",
            linestyle="--",
            alpha=0.7,
            zorder=1,
        )
        # Precinct boundaries – darker, solid
        precincts_plot.boundary.plot(
            ax=ax,
            linewidth=0.4,
            edgecolor="#666666",
            alpha=0.8,
            zorder=2,
        )
        # Dots
        plot_dots(ax, dots_plot)

    # Plot based on available plans
    plot_idx = 0
    
    # Draw congressional plan
    if cong_plot is not None:
        ax = axes[plot_idx]
        ax.set_title(
            f"{state_info['name']}: BG (dashed) + Precincts (solid) + Dots + Congressional Districts",
            fontsize=11,
        )
        draw_base(ax)
        cong_plot.boundary.plot(
            ax=ax,
            linewidth=1.5,
            edgecolor="red",
            alpha=0.9,
            zorder=4,
        )
        add_district_labels(ax, cong_plot, "DISTRICT")
        ax.set_axis_off()
        plot_idx += 1

    # Draw legislative plans
    chamber_names = {
        'sldl': 'State Legislative Lower',
        'sldu': 'State Legislative Upper',
        'sl': 'State Legislative'
    }
    
    for chamber, leg_plot in leg_plots.items():
        if leg_plot is not None:
            ax = axes[plot_idx]
            chamber_name = chamber_names.get(chamber, chamber.upper())
            ax.set_title(
                f"{state_info['name']}: BG (dashed) + Precincts (solid) + Dots + {chamber_name} Districts",
                fontsize=11,
            )
            draw_base(ax)
            leg_plot.boundary.plot(
                ax=ax,
                linewidth=1.3,
                edgecolor="blue",
                alpha=0.9,
                zorder=4,
            )
            add_district_labels(ax, leg_plot, "DISTRICT")
            ax.set_axis_off()
            plot_idx += 1

    # If no plans, just show demographics
    if num_plots == 0:
        ax = axes[0]
        ax.set_title(
            f"{state_info['name']}: BG (dashed) + Precincts (solid) + Race Dots",
            fontsize=12,
        )
        draw_base(ax)
        ax.set_axis_off()

    if num_plots > 1:
        plt.suptitle(
            f"{state_info['name']} Comparison: BG / Precincts / Dots / Plans",
            fontsize=14,
        )

    # Set common extent from block groups
    minx, miny, maxx, maxy = bg_plot.total_bounds
    for ax in axes:
        ax.set_xlim(minx, maxx)
        ax.set_ylim(miny, maxy)

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