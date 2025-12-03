#!/usr/bin/env python3
"""
Stage 3: Generate race dot maps from block-group-level demographic data.

This script creates dot density maps showing racial and ethnic population distribution
for any US state using data from Stage 1.

Usage:
    python run_stage3_dots.py <STATE_CODE> [options]
    
Examples:
    python run_stage3_dots.py AZ
    python run_stage3_dots.py CA --dot-unit 25 --acs-year 2022
    python run_stage3_dots.py TX --dot-unit 100 --seed 42

Inputs (from Stage 1):
  - outputs/<state>/<state>_bg_all_data_<year>.geojson

Outputs:
  - outputs/<state>/<state>_dots_pop<yy>_unit<X>.geojson
  - Per-group files: <state>_dots_pop<yy>_unit<X>_<group>.geojson
"""

import os
import random
import json
from typing import Dict, List

import geopandas as gpd
import numpy as np
from shapely.geometry import Point
from common import (
    setup_argument_parser,
    validate_state_setup,
    get_state_paths,
    print_state_info
)

# ===================== CONSTANTS =====================

# Which demographic columns to use for which dot "group"
# These match the columns Stage 1 produced on the BG layer.
GROUP_COLS_TEMPLATE = {
    "white":      "WHT_POP{year_suffix}",
    "black":      "BLK_POP{year_suffix}",
    "asian":      "ASN_POP{year_suffix}",
    "hispanic":   "HSP_POP{year_suffix}",
    "native":     "AIA_POP{year_suffix}",   # American Indian / Alaska Native
    "nhpi":       "HPI_POP{year_suffix}",   # Native Hawaiian / Pacific Islander
    "other":      "OTH_POP{year_suffix}",
    "two_or_more":"2OM_POP{year_suffix}",
}

TOTAL_POP_COL_TEMPLATE = "TOT_POP{year_suffix}"   # used for presence check

# High-contrast dot colors
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


def random_point_in_polygon(poly, rng: random.Random) -> Point:
    """Sample a random point inside a polygon using rejection sampling."""
    minx, miny, maxx, maxy = poly.bounds
    if maxx <= minx or maxy <= miny:
        return poly.representative_point()
    for _ in range(2000):
        x = rng.uniform(minx, maxx)
        y = rng.uniform(miny, maxy)
        p = Point(x, y)
        if poly.contains(p):
            return p
    # Fallback if geometry is weird
    return poly.representative_point()


def area_weighted_sample_point(geom, rng: random.Random) -> Point:
    """
    Sample a point from a (Multi)Polygon with probability proportional
    to area of each component.
    """
    if geom.is_empty:
        return Point()

    geom_type = geom.geom_type
    if geom_type == "Polygon":
        return random_point_in_polygon(geom, rng)

    if geom_type == "MultiPolygon":
        geoms = list(geom.geoms)
        areas = np.array([g.area for g in geoms], dtype="float64")
        if areas.sum() <= 0:
            return random_point_in_polygon(geoms[0], rng)
        probs = areas / areas.sum()
        choice = np.random.default_rng().choice(len(geoms), p=probs)
        return random_point_in_polygon(geoms[choice], rng)

    # Fallback: representative point
    return geom.representative_point()


def compute_dots_for_groups(bg, dot_unit: int, groups: Dict[str, str], seed: int):
    """
    For each group, compute how many dots per block group.

    Returns:
      dots_by_group: dict[group] -> np.ndarray[int]
      counts_by_group: dict[group] -> np.ndarray[float]
    """
    dots_by_group = {}
    counts_by_group = {}

    rng = np.random.default_rng(seed)

    for group, col in groups.items():
        if col not in bg.columns:
            print(f"Warning: column {col} not in BG; skipping group '{group}'")
            continue

        counts = bg[col].fillna(0).astype(float).values
        counts_by_group[group] = counts

        # expected dots = people / dot_unit
        expected = counts / float(dot_unit)

        # Random rounding: floor + Bernoulli(frac)
        base = np.floor(expected).astype(int)
        frac = expected - base
        add_one = (rng.random(len(frac)) < frac).astype(int)
        dots = base + add_one

        dots_by_group[group] = dots

    return dots_by_group, counts_by_group


def ensure_presence(dots_by_group: Dict[str, np.ndarray],
                    counts_by_group: Dict[str, np.ndarray],
                    tot_pop: np.ndarray,
                    seed: int):
    """
    Ensure that any block group with total population > 0 has at least 1 dot total.
    If a BG has zero dots but tot_pop > 0, give 1 dot to the majority group.
    """
    groups = list(dots_by_group.keys())
    if not groups:
        return dots_by_group

    # total dots per BG
    total_dots = np.zeros_like(tot_pop, dtype=int)
    for g in groups:
        total_dots += dots_by_group[g]

    # BGs with people but no dots
    zero_idxs = np.where((tot_pop > 0) & (total_dots == 0))[0]
    if len(zero_idxs) == 0:
        return dots_by_group

    print(f"Presence: {len(zero_idxs)} block groups had population but 0 dots; assigning 1 dot each to majority group.")

    rng = np.random.default_rng(seed)

    for i in zero_idxs:
        # pick group with max count in this BG (ties broken randomly)
        cvals = np.array([counts_by_group[g][i] for g in groups], dtype="float64")
        mx = cvals.max()
        if mx <= 0:
            # no group actually has people → skip, no dot
            continue
        top_idxs = np.where(cvals == mx)[0]
        choice = rng.choice(top_idxs)
        g_sel = groups[choice]
        dots_by_group[g_sel][i] += 1

    return dots_by_group


def main():
    """Main function that processes command line arguments and runs the stage."""
    # Parse command line arguments
    parser = setup_argument_parser(
        description="Generate race dot maps from demographic data for any US state.",
        stage_name="Stage 3"
    )
    
    parser.add_argument(
        "--dot-unit",
        type=int,
        default=50,
        help="People per dot (default: 50)"
    )
    
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible dot placement (default: 42)"
    )
    
    args = parser.parse_args()

    # Validate state and get configuration
    state_info, state_paths = validate_state_setup(args.state)
    print_state_info(state_info)

    # Get year suffix for column names (use the year from state_paths, not args)
    year_suffix = str(state_paths["acs_year"])[-2:]
    
    # Build group columns with year suffix
    group_cols = {
        group: template.format(year_suffix=year_suffix)
        for group, template in GROUP_COLS_TEMPLATE.items()
    }
    
    total_pop_col = TOTAL_POP_COL_TEMPLATE.format(year_suffix=year_suffix)

    # Check input file
    bg_input = state_paths["bg_geojson"]
    if not os.path.exists(bg_input):
        raise FileNotFoundError(
            f"BG input GeoJSON not found: {bg_input}\n"
            "Run Stage 1 to generate it first."
        )

    print(f"[1] Loading BG data: {bg_input}")
    bg = gpd.read_file(bg_input)

    # Work in projected CRS for better area-based sampling
    bg = bg.to_crs("EPSG:5070")

    if total_pop_col not in bg.columns:
        raise RuntimeError(f"Expected '{total_pop_col}' in BG layer.")

    tot_pop = bg[total_pop_col].fillna(0).astype(float).values

    # 2. Compute dots per group
    print(f"[2] Computing dots per group (DOT_UNIT = {args.dot_unit})...")
    dots_by_group, counts_by_group = compute_dots_for_groups(bg, args.dot_unit, group_cols, args.seed)

    # 3. Presence: ensure at least 1 dot for non-empty BGs
    dots_by_group = ensure_presence(dots_by_group, counts_by_group, tot_pop, args.seed)

    # 4. Emit point features
    print("[3] Sampling dot locations...")
    rng = random.Random(args.seed)
    feats = []
    geoid_col = "GEOID" if "GEOID" in bg.columns else None

    for i, row in bg.iterrows():
        geom = row.geometry
        if geom is None or geom.is_empty:
            continue

        for group, col in group_cols.items():
            if group not in dots_by_group:
                continue
            n = int(dots_by_group[group][i])
            if n <= 0:
                continue

            for _ in range(n):
                p = area_weighted_sample_point(geom, rng)
                feat = {
                    "group": group,
                    "geometry": p,
                }
                if geoid_col:
                    feat["bg_geoid"] = row[geoid_col]
                feats.append(feat)

    print(f"  -> total dots: {len(feats):,}")

    if not feats:
        print("No dots generated; check demographic fields and DOT_UNIT.")
        return

    dots = gpd.GeoDataFrame(feats, geometry="geometry", crs=bg.crs)

    # 5. Save combined + per-group GeoJSONs
    print("[4] Writing GeoJSONs...")
    dots_web = dots.to_crs(4326)

    # Generate output paths
    dots_combined = state_paths["dots_geojson"].format(dot_unit=args.dot_unit)
    dots_web.to_file(dots_combined, driver="GeoJSON")
    print(f"  -> combined dots: {dots_combined}")

    # per-group
    # prefix = os.path.splitext(os.path.basename(dots_combined))[0]
    # out_dir = os.path.dirname(dots_combined)

    # for g in group_cols.keys():
    #     subset = dots_web.query("group == @g")
    #     if subset.empty:
    #         continue
    #     path = os.path.join(out_dir, f"{prefix}_{g}.geojson")
    #     subset.to_file(path, driver="GeoJSON")
    #     print(f"  -> {g}: {len(subset):,} dots → {path}")

    print("[4] Done.")
    print(f"\nNext: Run stage 4 with -> python run_stage4_comp.py {args.state}")


if __name__ == "__main__":
    main()