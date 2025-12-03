#!/usr/bin/env python3
"""
Master script to run all politech-processor stages for any US state.

This script provides a simple interface to run all stages of the politech processing
pipeline for any state. It handles the coordination between stages and provides
helpful progress information.

Usage:
    python run_all_stages.py <STATE_CODE> [options]
    
Examples:
    python run_all_stages.py AZ
    python run_all_stages.py CA --acs-year 2022
    python run_all_stages.py TX --skip-stage0  # if data already exists
    python run_all_stages.py FL --stages 0,1,2  # run specific stages

Available Stages:
    0: Download TIGER shapefiles and ACS data
    1: Build precinct-level demographics using maup
    2: Build district plans and assignments
    3: Generate race dot maps
    4: Create comparison visualizations
"""

import subprocess
import sys
import os
from common import setup_argument_parser, validate_state_setup, print_state_info


def run_stage(stage_num: int, state_code: str, extra_args: list = None) -> bool:
    """
    Run a specific stage script.
    
    Args:
        stage_num: Stage number (0-4)
        state_code: Two-letter state code
        extra_args: Additional arguments to pass to the stage script
        
    Returns:
        True if successful, False if failed
    """
    stage_scripts = {
        0: "run_stage0.py",
        1: "run_stage1.py", 
        2: "run_stage2.py",
        3: "run_stage3_dots.py",
        4: "run_stage4_comp.py"
    }
    
    if stage_num not in stage_scripts:
        print(f"❌ Invalid stage number: {stage_num}")
        return False
    
    script_name = stage_scripts[stage_num]
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    
    if not os.path.exists(script_path):
        print(f"❌ Stage script not found: {script_path}")
        return False
    
    # Build command
    cmd = [sys.executable, script_name, state_code]
    if extra_args:
        cmd.extend(extra_args)
    
    print(f"\n🚀 Running Stage {stage_num}: {script_name}")
    print(f"Command: {' '.join(cmd)}")
    print("=" * 60)
    
    try:
        result = subprocess.run(cmd, check=True, cwd=os.path.dirname(__file__))
        print(f"✅ Stage {stage_num} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Stage {stage_num} failed with exit code {e.returncode}")
        return False
    except Exception as e:
        print(f"❌ Stage {stage_num} failed with error: {e}")
        return False


def main():
    """Main function that coordinates all stages."""
    # Set up argument parser
    parser = setup_argument_parser(
        description="Run all politech-processor stages for any US state.",
        stage_name="All Stages"
    )
    
    parser.add_argument(
        "--stages",
        help="Comma-separated list of stages to run (e.g., '0,1,2' or '1-3'). Default: all stages"
    )
    
    parser.add_argument(
        "--skip-stage0", 
        action="store_true",
        help="Skip stage 0 (data download) - useful if data already exists"
    )
    
    parser.add_argument(
        "--dot-unit",
        type=int,
        default=50,
        help="People per dot for stage 3 (default: 50)"
    )
    
    parser.add_argument(
        "--plan-year",
        type=int,
        default=2022,
        help="Plan year for stages 2 and 4 (default: 2022)"
    )
    
    args = parser.parse_args()
    
    # Validate state
    try:
        state_info, state_paths = validate_state_setup(args.state)
        print_state_info(state_info)
    except Exception as e:
        print(f"❌ State validation failed: {e}")
        sys.exit(1)
    
    # Determine which stages to run
    if args.stages:
        if '-' in args.stages:
            # Range notation (e.g., "1-3")
            start, end = map(int, args.stages.split('-'))
            stages_to_run = list(range(start, end + 1))
        else:
            # Comma-separated (e.g., "0,1,2")
            stages_to_run = [int(s.strip()) for s in args.stages.split(',')]
    else:
        # Default: all stages (0-4)
        stages_to_run = list(range(5))
    
    # Skip stage 0 if requested
    if args.skip_stage0 and 0 in stages_to_run:
        stages_to_run.remove(0)
        print("⚠️  Skipping Stage 0 (data download) as requested")
    
    print(f"\n📋 Will run stages: {stages_to_run}")
    
    # Prepare extra arguments for each stage
    extra_args = []
    if args.acs_year != 2023:
        extra_args.extend(["--acs-year", str(args.acs_year)])
    if args.census_year != 2020:
        extra_args.extend(["--census-year", str(args.census_year)])
    
    # Run each stage
    total_stages = len(stages_to_run)
    successful_stages = 0
    
    for i, stage_num in enumerate(stages_to_run, 1):
        print(f"\n📊 Progress: Stage {stage_num} ({i}/{total_stages})")
        
        # Stage-specific arguments
        stage_args = extra_args.copy()
        
        if stage_num == 3:  # Dots stage
            stage_args.extend(["--dot-unit", str(args.dot_unit)])
        
        if stage_num in [2, 4]:  # Plan stages
            stage_args.extend(["--plan-year", str(args.plan_year)])
        
        success = run_stage(stage_num, args.state, stage_args)
        
        if success:
            successful_stages += 1
        else:
            print(f"\n❌ Pipeline failed at Stage {stage_num}")
            print(f"✅ Completed {successful_stages}/{total_stages} stages successfully")
            
            # Ask if user wants to continue
            try:
                response = input("\nContinue with remaining stages? (y/N): ").lower()
                if response != 'y':
                    break
            except KeyboardInterrupt:
                print("\n\n🛑 Pipeline interrupted by user")
                break
    
    # Final summary
    print("\n" + "=" * 60)
    print("🏁 PIPELINE SUMMARY")
    print("=" * 60)
    print(f"State: {state_info['name']} ({args.state})")
    print(f"Stages completed: {successful_stages}/{total_stages}")
    
    if successful_stages == total_stages:
        print("🎉 All stages completed successfully!")
        print(f"\nOutput directory: {state_paths['output_dir']}")
        print("\nGenerated files:")
        
        # List key output files that should exist
        key_files = [
            f"{args.state.lower()}_bg_all_data_{args.acs_year}.geojson",
            f"{args.state.lower()}_precinct_all_pop_{args.acs_year}.geojson",
        ]
        
        if 3 in stages_to_run:
            key_files.append(f"{args.state.lower()}_dots_pop{str(args.acs_year)[-2:]}_unit{args.dot_unit}.geojson")
        
        for filename in key_files:
            filepath = os.path.join(state_paths['output_dir'], filename)
            if os.path.exists(filepath):
                print(f"  ✅ {filename}")
            else:
                print(f"  ❌ {filename} (missing)")
    else:
        print("⚠️  Some stages failed. Check the output above for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()