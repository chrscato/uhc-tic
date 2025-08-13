#!/usr/bin/env python3
"""
Resume extraction script for interrupted rate extraction processes.

This script helps recover from interrupted extractions by:
1. Checking for existing output files
2. Identifying any backup files
3. Consolidating partial results
4. Providing resume instructions
"""

import os
import sys
from pathlib import Path
import pandas as pd
import glob

def find_existing_files(output_dir: str = "output"):
    """Find existing rate extraction files and backup files."""
    output_path = Path(output_dir)
    
    if not output_path.exists():
        print(f"❌ Output directory {output_dir} not found")
        return
    
    # Find main rate files
    rate_files = list(output_path.glob("rates_*.parquet"))
    backup_files = list(output_path.glob("rates_*_backup_*.parquet"))
    
    print(f"🔍 Found {len(rate_files)} main rate files:")
    for f in rate_files:
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  📁 {f.name} ({size_mb:.1f} MB)")
    
    if backup_files:
        print(f"\n🔍 Found {len(backup_files)} backup files:")
        for f in backup_files:
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"  📋 {f.name} ({size_mb:.1f} MB)")
    
    return rate_files, backup_files

def consolidate_backup_files(output_dir: str = "output"):
    """Consolidate backup files into main files."""
    output_path = Path(output_dir)
    
    # Find all backup files
    backup_files = list(output_path.glob("rates_*_backup_*.parquet"))
    
    if not backup_files:
        print("✅ No backup files to consolidate")
        return
    
    print(f"🔄 Consolidating {len(backup_files)} backup files...")
    
    # Group backup files by base name
    backup_groups = {}
    for backup_file in backup_files:
        # Extract base name (e.g., "rates_20250810_153112" from "rates_20250810_153112_backup_1234567890.parquet")
        base_name = backup_file.name.split("_backup_")[0]
        if base_name not in backup_groups:
            backup_groups[base_name] = []
        backup_groups[base_name].append(backup_file)
    
    for base_name, group_files in backup_groups.items():
        print(f"\n📁 Processing group: {base_name}")
        
        # Find main file
        main_file = output_path / f"{base_name}.parquet"
        
        if main_file.exists():
            print(f"  📖 Reading main file: {main_file.name}")
            try:
                main_df = pd.read_parquet(main_file)
                print(f"    📊 Main file has {len(main_df):,} records")
            except Exception as e:
                print(f"    ❌ Error reading main file: {e}")
                continue
        else:
            print(f"  ⚠️  Main file not found, will create from backups")
            main_df = None
        
        # Read backup files
        backup_dfs = []
        for backup_file in group_files:
            try:
                backup_df = pd.read_parquet(backup_file)
                backup_dfs.append(backup_df)
                print(f"    📖 Read backup: {backup_file.name} ({len(backup_df):,} records)")
            except Exception as e:
                print(f"    ❌ Error reading backup {backup_file.name}: {e}")
        
        if not backup_dfs:
            print(f"    ⚠️  No valid backup files for this group")
            continue
        
        # Consolidate
        if main_df is not None:
            all_dfs = [main_df] + backup_dfs
        else:
            all_dfs = backup_dfs
        
        consolidated_df = pd.concat(all_dfs, ignore_index=True)
        
        # Write consolidated file
        consolidated_path = output_path / f"{base_name}_consolidated.parquet"
        consolidated_df.to_parquet(consolidated_path, index=False)
        
        print(f"    ✅ Consolidated into: {consolidated_path.name}")
        print(f"    📊 Total records: {len(consolidated_df):,}")
        
        # Replace original if it existed
        if main_file.exists():
            import shutil
            backup_main = output_path / f"{base_name}_original.parquet"
            shutil.move(str(main_file), str(backup_main))
            print(f"    💾 Backed up original to: {backup_main.name}")
        
        shutil.move(str(consolidated_path), str(main_file))
        print(f"    🔄 Replaced with consolidated version")
        
        # Clean up backup files
        for backup_file in group_files:
            try:
                backup_file.unlink()
                print(f"    🗑️  Cleaned up: {backup_file.name}")
            except Exception as e:
                print(f"    ⚠️  Could not delete {backup_file.name}: {e}")

def main():
    """Main function to analyze and consolidate existing files."""
    print("🚀 Rate Extraction Recovery Tool")
    print("=" * 50)
    
    # Check for existing files
    rate_files, backup_files = find_existing_files()
    
    if not rate_files and not backup_files:
        print("\n💡 No existing files found. You can start fresh with:")
        print("   python src/extract_rates.py <your_url> --provider-groups-parquet <your_file>")
        return
    
    # Offer to consolidate
    if backup_files:
        print(f"\n💡 Found {len(backup_files)} backup files that can be consolidated.")
        response = input("Would you like to consolidate them now? (y/n): ").lower().strip()
        
        if response in ['y', 'yes']:
            consolidate_backup_files()
        else:
            print("💡 You can consolidate later by running: python resume_extraction.py")
    
    # Show resume instructions
    print(f"\n📋 Resume Instructions:")
    print(f"1. If you have backup files, consolidate them first")
    print(f"2. Check the size of your main output file(s)")
    print(f"3. Consider using the --time or --items flags to limit processing")
    print(f"4. The improved extractor will handle file permission issues automatically")
    
    if rate_files:
        print(f"\n💡 Your existing output file(s) can be used as a starting point.")
        print(f"   The extractor will automatically append new data to existing files.")

if __name__ == "__main__":
    main() 