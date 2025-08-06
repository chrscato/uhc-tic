#!/usr/bin/env python3
"""
Efficient TIN discovery script - only looks at provider_references, not in_network data.
Much faster since it doesn't process the huge in_network array.
"""

import json
import gzip
import requests
from io import BytesIO
from collections import Counter
from tic_mrf_scraper.fetch.blobs import get_cloudfront_headers

def discover_tins_efficiently(target_tins=None):
    """
    Efficiently discover TIN values by only parsing provider_references.
    
    Args:
        target_tins: Set of TIN values to specifically search for
    """
    
    url = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-08-01/2025-08-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_PPO---NDC_PPO-NDC_in-network-rates.json.gz"
    
    print(f"🔍 EFFICIENT TIN DISCOVERY")
    print(f"⚡ Only parsing provider_references (ignoring in_network for speed)")
    
    if target_tins:
        print(f"🎯 Searching for specific TINs: {target_tins}")
    
    # Fetch and parse
    print(f"📥 Fetching MRF file...")
    headers = get_cloudfront_headers()
    response = requests.get(url, headers=headers, timeout=300)
    response.raise_for_status()
    
    print(f"📦 Downloaded {len(response.content) / 1024**2:.1f} MB")
    print(f"🔓 Parsing JSON (this may take a moment)...")
    
    with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
        data = json.load(gz)
    
    # Only process provider_references - much faster!
    print(f"👥 Analyzing provider_references only...")
    
    tin_counter = Counter()
    provider_refs = data.get("provider_references", [])
    target_found = {}
    all_tins = set()
    
    print(f"📊 Found {len(provider_refs)} provider reference groups")
    
    for ref_idx, provider_ref in enumerate(provider_refs):
        provider_group_id = provider_ref.get("provider_group_id")
        
        for group_idx, group in enumerate(provider_ref.get("provider_groups", [])):
            tin_info = group.get("tin", {})
            tin_value = str(tin_info.get("value", "")).strip()
            tin_type = tin_info.get("type", "")
            npi_count = len(group.get("npi", []))
            
            if tin_value and tin_value != "":
                all_tins.add(tin_value)
                tin_counter[tin_value] += npi_count
                
                # Check if this is a target TIN
                if target_tins and tin_value in target_tins:
                    if tin_value not in target_found:
                        target_found[tin_value] = []
                    
                    target_found[tin_value].append({
                        "provider_group_id": provider_group_id,
                        "tin_type": tin_type,
                        "npi_count": npi_count,
                        "npis": group.get("npi", [])[:5]  # Show first 5 NPIs
                    })
        
        # Progress update for large files
        if ref_idx % 1000 == 0 and ref_idx > 0:
            print(f"  Processed {ref_idx:,} provider reference groups...")
    
    # Results
    print(f"\n📊 DISCOVERY RESULTS:")
    print(f"✅ Total unique TINs found: {len(all_tins):,}")
    print(f"👥 Total provider entries: {sum(tin_counter.values()):,}")
    
    # Target TIN results
    if target_tins:
        print(f"\n🎯 TARGET TIN SEARCH RESULTS:")
        found_count = 0
        for tin in target_tins:
            if tin in target_found:
                found_count += 1
                occurrences = target_found[tin]
                total_npis = sum(occ["npi_count"] for occ in occurrences)
                print(f"✅ FOUND: {tin}")
                print(f"   🏢 Appears in {len(occurrences)} provider groups")
                print(f"   👥 Total NPIs: {total_npis}")
                print(f"   📝 Provider Group IDs: {[occ['provider_group_id'] for occ in occurrences[:3]]}...")
                if occurrences[0]["npis"]:
                    print(f"   🔢 Sample NPIs: {occurrences[0]['npis'][:3]}...")
            else:
                print(f"❌ NOT FOUND: {tin}")
        
        print(f"\n📈 Target TIN Summary: {found_count}/{len(target_tins)} found")
        
        if found_count > 0:
            found_tins = set(target_found.keys())
            print(f"✅ Use these TINs in your whitelist: {found_tins}")
        else:
            print(f"⚠️  None of your target TINs were found in this file")
    
    # Show top TINs regardless
    print(f"\n🔝 TOP 20 TINs BY PROVIDER COUNT:")
    for i, (tin, count) in enumerate(tin_counter.most_common(20), 1):
        marker = "🎯" if target_tins and tin in target_tins else "  "
        print(f"{marker} {i:2d}. {tin} ({count:,} providers)")
    
    # Generate suggestions
    top_tins = [tin for tin, _ in tin_counter.most_common(10)]
    
    print(f"\n💡 SUGGESTED TIN WHITELIST (top 10 most common):")
    print(f"tin_whitelist = {{")
    for tin in top_tins[:5]:
        print(f'    "{tin}",  # {tin_counter[tin]:,} providers')
    print(f"    # Add more as needed...")
    print(f"}}")
    
    return {
        "all_tins": all_tins,
        "tin_counter": tin_counter,
        "target_found": target_found,
        "top_tins": top_tins
    }

def quick_tin_check(target_tins):
    """
    Quick check if specific TINs exist in the file.
    
    Args:
        target_tins: List or set of TIN values to check
    """
    if isinstance(target_tins, (list, tuple)):
        target_tins = set(str(tin) for tin in target_tins)
    
    print(f"🎯 QUICK TIN CHECK")
    print(f"Checking if these TINs exist: {target_tins}")
    
    results = discover_tins_efficiently(target_tins)
    
    found_tins = set(results["target_found"].keys())
    missing_tins = target_tins - found_tins
    
    print(f"\n📋 QUICK RESULTS:")
    print(f"✅ Found: {found_tins}")
    print(f"❌ Missing: {missing_tins}")
    print(f"📊 Success rate: {len(found_tins)}/{len(target_tins)} ({len(found_tins)/len(target_tins)*100:.1f}%)")
    
    return len(found_tins) > 0

if __name__ == "__main__":
    print("🔍 TIN Discovery Options:")
    print("1. Discover all TINs in the file")
    print("2. Check specific target TINs")
    print()
    
    # Option 1: Discover all TINs
    print("=" * 60)
    print("OPTION 1: DISCOVER ALL TINS")
    print("=" * 60)
    all_results = discover_tins_efficiently()
    
    # Option 2: Check your specific TINs
    print("\n" + "=" * 60)
    print("OPTION 2: CHECK YOUR TARGET TINS")
    print("=" * 60)
    
    # Replace these with your actual target TINs
    your_target_tins = {
        "300466706",
        "581646537"
    }
    
    target_results = discover_tins_efficiently(your_target_tins)
    
    print(f"\n🚀 NEXT STEPS:")
    if target_results["target_found"]:
        found_tins = set(target_results["target_found"].keys())
        print(f"✅ Use these TINs that were found: {found_tins}")
        print(f"💡 Try processing with: tin_value_whitelist={found_tins}")
    else:
        top_5 = target_results["top_tins"][:5]
        print(f"⚠️  Your target TINs weren't found")
        print(f"💡 Try these common TINs instead: {set(top_5)}")
        print(f"💡 Or use tin_value_whitelist=None to process all TINs")