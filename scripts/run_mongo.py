#!/usr/bin/env python3
"""
MongoDB Upload Script: Upload political data to MongoDB.

This script uploads precincts, dots, plans, and assignments data to MongoDB.
- {state}_precincts: One collection per state with precinct geometries and demographics
- {state}_dots: One collection per state with dot density data (overwrites on re-run)
- plans: Single collection for all plans across all states
- assignments: Single collection for all precinct-district assignments

Usage:
    python run_mongo.py <STATE_CODE> [options]
    
Examples:
    python run_mongo.py AZ
    python run_mongo.py LA --mongo-uri "mongodb://localhost:27017"
    python run_mongo.py CA --database ars-mongo --dot-unit 40

Environment Variables:
    ARS_MONGO_URI: MongoDB connection string (default: mongodb://localhost:27017)
    ARS_MONGO_DB: MongoDB database name (optional if specified in URI)

Requires:
  - pymongo package: pip install pymongo
  - Stage 1, 2, 3 outputs for the specified state
"""

import os
import sys
import json
import argparse
from typing import Dict, List, Any

import geopandas as gpd
from pymongo import MongoClient, GEOSPHERE
from pymongo.errors import ConnectionFailure, DuplicateKeyError

from common import (
    validate_state_setup,
    get_state_paths,
    print_state_info
)


def get_mongo_connection(mongo_uri: str, database: str = None):
    """Connect to MongoDB and return database instance."""
    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command('ping')
        
        # If no database specified, try to get it from URI
        if database is None:
            default_db = client.get_default_database()
            if default_db is not None:
                database = default_db.name
            else:
                database = "political_data"  # fallback default
        
        print(f"✓ Connected to MongoDB at {mongo_uri}")
        print(f"✓ Using database: {database}")
        return client[database]
    except ConnectionFailure as e:
        raise ConnectionFailure(f"Failed to connect to MongoDB: {e}")


def geojson_to_mongodb_format(gdf: gpd.GeoDataFrame) -> List[Dict[str, Any]]:
    """
    Convert GeoDataFrame to MongoDB-ready format with GeoJSON geometries.
    
    MongoDB requires geometries in GeoJSON format for geospatial indexing.
    """
    # Convert to WGS84 if not already
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    
    records = []
    for _, row in gdf.iterrows():
        record = row.to_dict()
        
        # Convert geometry to GeoJSON format
        if row.geometry is not None and not row.geometry.is_empty:
            record['geometry'] = json.loads(gpd.GeoSeries([row.geometry]).to_json())['features'][0]['geometry']
        else:
            record['geometry'] = None
        
        records.append(record)
    
    return records


def upload_precincts(db, state_abbr: str, precinct_file: str):
    """
    Upload precinct data to {state}_precincts collection.
    Replaces existing data for the state.
    """
    collection_name = f"{state_abbr.lower()}_precincts"
    collection = db[collection_name]
    
    print(f"\n[1] Loading precincts from: {precinct_file}")
    precincts = gpd.read_file(precinct_file)
    
    print(f"[2] Converting {len(precincts)} precincts to MongoDB format...")
    records = geojson_to_mongodb_format(precincts)
    
    print(f"[3] Uploading to collection '{collection_name}'...")
    # Drop existing data
    collection.delete_many({})
    
    # Insert new data
    if records:
        collection.insert_many(records)
        
        # Create geospatial index on geometry field
        collection.create_index([("geometry", GEOSPHERE)])
        print(f"✓ Uploaded {len(records)} precincts to '{collection_name}'")
    else:
        print(f"⚠ No precinct records to upload")


def upload_dots(db, state_abbr: str, dots_file: str, dot_unit: int):
    """
    Upload dot density data to {state}_dots collection.
    Overwrites existing data for the state (replaces all dots).
    """
    collection_name = f"{state_abbr.lower()}_dots"
    collection = db[collection_name]
    
    print(f"\n[1] Loading dots from: {dots_file}")
    dots = gpd.read_file(dots_file)
    
    print(f"[2] Converting {len(dots)} dots to MongoDB format...")
    records = geojson_to_mongodb_format(dots)
    
    # Add dot_unit metadata to each record
    for record in records:
        record['dot_unit'] = dot_unit
    
    print(f"[3] Uploading to collection '{collection_name}' (overwriting)...")
    # Drop all existing dots for this state
    collection.delete_many({})
    
    # Insert new data
    if records:
        collection.insert_many(records)
        
        # Create geospatial index
        collection.create_index([("geometry", GEOSPHERE)])
        
        # Create index on group field for filtering
        collection.create_index("group")
        
        print(f"✓ Uploaded {len(records)} dots (unit={dot_unit}) to '{collection_name}'")
    else:
        print(f"⚠ No dot records to upload")


def upload_plans(db, plans_file: str, state_abbr: str = None):
    """
    Upload plans to 'plans' collection.
    If state_abbr is provided, only uploads/updates plans for that state.
    Otherwise, uploads all plans from the file.
    """
    collection = db['plans']
    
    print(f"\n[1] Loading plans from: {plans_file}")
    with open(plans_file, 'r') as f:
        all_plans = json.load(f)
    
    # Filter by state if specified
    if state_abbr:
        plans = [p for p in all_plans if p.get('state') == state_abbr.upper()]
        print(f"[2] Found {len(plans)} plans for state {state_abbr.upper()}")
    else:
        plans = all_plans
        print(f"[2] Found {len(plans)} total plans")
    
    if not plans:
        print(f"⚠ No plans to upload")
        return
    
    print(f"[3] Uploading to collection 'plans'...")
    
    # Upsert plans (update if exists, insert if new)
    uploaded_count = 0
    updated_count = 0
    
    for plan in plans:
        result = collection.replace_one(
            {'plan_id': plan['plan_id']},
            plan,
            upsert=True
        )
        if result.upserted_id:
            uploaded_count += 1
        elif result.modified_count > 0:
            updated_count += 1
    
    # Create index on plan_id and state
    collection.create_index("plan_id", unique=True)
    collection.create_index("state")
    collection.create_index("chamber")
    
    print(f"✓ Plans: {uploaded_count} inserted, {updated_count} updated")


def upload_assignments(db, assignments_file: str, state_abbr: str = None):
    """
    Upload assignments to 'assignments' collection.
    Uses upsert based on {plan_id, precinct_id} to update existing assignments
    and insert new ones. Preserves assignments for other states.
    """
    collection = db['assignments']
    
    print(f"\n[1] Loading assignments from: {assignments_file}")
    with open(assignments_file, 'r') as f:
        all_assignments = json.load(f)
    
    # Filter by state if specified
    if state_abbr:
        assignments = [a for a in all_assignments if a.get('state') == state_abbr.upper()]
        print(f"[2] Found {len(assignments)} assignments for state {state_abbr.upper()}")
    else:
        assignments = all_assignments
        print(f"[2] Found {len(assignments)} total assignments")
    
    if not assignments:
        print(f"⚠ No assignments to upload")
        return
    
    print(f"[3] Upserting to collection 'assignments'...")
    
    # Upsert assignments (update if exists, insert if new)
    # Use compound key {plan_id, precinct_id} for uniqueness
    upserted_count = 0
    modified_count = 0
    
    for assignment in assignments:
        result = collection.replace_one(
            {
                'plan_id': assignment['plan_id'],
                'precinct_id': assignment['precinct_id']
            },
            assignment,
            upsert=True
        )
        if result.upserted_id:
            upserted_count += 1
        elif result.modified_count > 0:
            modified_count += 1
    
    # Create compound indexes for efficient queries
    collection.create_index([("state", 1), ("plan_id", 1)])
    collection.create_index([("plan_id", 1), ("precinct_id", 1)], unique=True)
    collection.create_index("district_id")
    
    print(f"✓ Assignments: {upserted_count} inserted, {modified_count} updated")


def main():
    """Main function that processes command line arguments and uploads data."""
    parser = argparse.ArgumentParser(
        description="Upload political data to MongoDB for any US state.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_mongo.py AZ
  python run_mongo.py LA --mongo-uri "mongodb://user:pass@localhost:27017/my_db"
  python run_mongo.py CA --database my_db --dot-unit 40
  
Environment Variables:
  ARS_MONGO_URI: MongoDB connection string (default: mongodb://localhost:27017)
  ARS_MONGO_DB: MongoDB database name (optional if specified in URI)
        """
    )
    
    parser.add_argument(
        "state",
        type=str,
        help="Two-letter state code (e.g., AZ, CA, TX)"
    )
    
    parser.add_argument(
        "--mongo-uri",
        type=str,
        default=os.getenv("ARS_MONGO_URI", "mongodb://localhost:27017"),
        help="MongoDB connection URI (default: mongodb://localhost:27017 or ARS_MONGO_URI env var)"
    )
    
    parser.add_argument(
        "--database",
        type=str,
        default=None,
        help="MongoDB database name (optional if specified in URI, or ARS_MONGO_DB env var)"
    )
    
    parser.add_argument(
        "--dot-unit",
        type=int,
        default=50,
        help="Dot unit used in Stage 3 (default: 50)"
    )
    
    parser.add_argument(
        "--skip-precincts",
        action="store_true",
        help="Skip uploading precincts"
    )
    
    parser.add_argument(
        "--skip-dots",
        action="store_true",
        help="Skip uploading dots"
    )
    
    parser.add_argument(
        "--skip-plans",
        action="store_true",
        help="Skip uploading plans"
    )
    
    parser.add_argument(
        "--skip-assignments",
        action="store_true",
        help="Skip uploading assignments"
    )
    
    args = parser.parse_args()
    
    # Validate state and get configuration
    state_info, state_paths = validate_state_setup(args.state)
    print_state_info(state_info)
    state_abbr = state_info["abbr"]
    
    # Get database name (from arg, env var, or None to extract from URI)
    database = args.database or os.getenv("ARS_MONGO_DB")
    
    # Connect to MongoDB
    print(f"\nConnecting to MongoDB...")
    try:
        db = get_mongo_connection(args.mongo_uri, database)
    except Exception as e:
        print(f"✗ Error: {e}")
        print("\nMake sure MongoDB is running and accessible.")
        print("Install pymongo if needed: pip install pymongo")
        sys.exit(1)
    
    # Check required files
    precinct_file = state_paths["precinct_geojson"]
    dots_file = state_paths["dots_geojson"].format(dot_unit=args.dot_unit)
    plans_file = "/Users/nikhiltejamalyala/Repositories/advproj/politech-processor/outputs/plans.json"
    assignments_file = "/Users/nikhiltejamalyala/Repositories/advproj/politech-processor/outputs/assignments.json"
    
    missing_files = []
    if not args.skip_precincts and not os.path.exists(precinct_file):
        missing_files.append(f"Precincts: {precinct_file}")
    if not args.skip_dots and not os.path.exists(dots_file):
        missing_files.append(f"Dots: {dots_file}")
    if not args.skip_plans and not os.path.exists(plans_file):
        missing_files.append(f"Plans: {plans_file}")
    if not args.skip_assignments and not os.path.exists(assignments_file):
        missing_files.append(f"Assignments: {assignments_file}")
    
    if missing_files:
        print("\n✗ Missing required files:")
        for mf in missing_files:
            print(f"  - {mf}")
        print("\nRun the appropriate stage scripts first:")
        print(f"  python run_stage1.py {state_abbr}")
        print(f"  python run_stage2.py {state_abbr}")
        print(f"  python run_stage3_dots.py {state_abbr} --dot-unit {args.dot_unit}")
        sys.exit(1)
    
    # Upload data
    print(f"\n{'='*60}")
    print(f"Uploading data for {state_info['name']} ({state_abbr})")
    print(f"URI: {args.mongo_uri}")
    if database:
        print(f"Database: {database}")
    print(f"{'='*60}")
    
    try:
        if not args.skip_precincts:
            upload_precincts(db, state_abbr, precinct_file)
        
        if not args.skip_dots:
            upload_dots(db, state_abbr, dots_file, args.dot_unit)
        
        if not args.skip_plans:
            upload_plans(db, plans_file, state_abbr)
        
        if not args.skip_assignments:
            upload_assignments(db, assignments_file, state_abbr)
        
        print(f"\n{'='*60}")
        print(f"✓ Upload complete for {state_abbr}!")
        print(f"{'='*60}")
        
        print(f"\nMongoDB Collections Created/Updated:")
        print(f"  - {state_abbr.lower()}_precincts: Precinct geometries and demographics")
        print(f"  - {state_abbr.lower()}_dots: Dot density visualization (unit={args.dot_unit})")
        print(f"  - plans: Redistricting plans (shared across all states)")
        print(f"  - assignments: Precinct-district assignments (shared across all states)")
        
    except Exception as e:
        print(f"\n✗ Error during upload: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
