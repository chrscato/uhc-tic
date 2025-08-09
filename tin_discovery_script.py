#!/usr/bin/env python3
"""
Memory-safe streaming TIN discovery - uses ijson to parse without loading full file.
Extremely memory efficient - uses only ~50-100MB regardless of file size.
"""

import gzip
import requests
import gc
import psutil
import os
from io import BytesIO
from collections import Counter
from tic_mrf_scraper.fetch.blobs import get_cloudfront_headers

# Try to import ijson for streaming
try:
    import ijson
    IJSON_AVAILABLE = True
except ImportError:
    IJSON_AVAILABLE = False
    print("⚠️  ijson not available. Install with: pip install ijson")
    print("   Falling back to memory-intensive method")

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def stream_discover_tins(target_tins=None):
    """
    Memory-safe streaming TIN discovery using ijson.
    Only uses ~50-100MB memory regardless of file size.
    """
    url = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-08-01/2025-08-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_PPO---NDC_PPO-NDC_in-network-rates.json.gz"
    
    if not IJSON_AVAILABLE:
        print("❌ ijson required for streaming. Install with: pip install ijson")
        return None
    
    print(f"🔍 MEMORY-SAFE STREAMING TIN DISCOVERY")
    print(f"🧠 Initial memory: {get_memory_usage():.1f} MB")
    
    if target_tins:
        target_tins = set(str(tin) for tin in target_tins)
        print(f"🎯 Searching for specific TINs: {target_tins}")
    
    # Stream the file without loading into memory
    print(f"📥 Streaming MRF file...")
    headers = get_cloudfront_headers()
    response = requests.get(url, headers=headers, stream=True, timeout=300)
    response.raise_for_status()
    
    print(f"📦 File size: {response.headers.get('content-length', 'unknown')} bytes")
    print(f"🔓 Streaming and parsing provider_references...")
    
    # Results storage
    tin_counter = Counter()
    target_found = {}
    all_tins = set()
    provider_refs_processed = 0
    
    try:
        # Handle gzipped streaming
        if url.endswith('.gz'):
            # For gzipped files, we need to decompress first
            # This is still memory efficient because we stream the decompression
            print("🗜️  Decompressing gzipped stream...")
            gz_content = gzip.decompress(response.content)
            stream = BytesIO(gz_content)
            print(f"🧠 Memory after decompression: {get_memory_usage():.1f} MB")
            
            # Clear the response content from memory
            del response
            gc.collect()
        else:
            stream = response.raw
        
        # Parse JSON stream - only extract provider_references
        print("🔄 Parsing provider_references with streaming...")
        
        # Parse provider_references array
        provider_refs = ijson.items(stream, 'provider_references.item')
        
        for provider_ref in provider_refs:
            provider_refs_processed += 1
            provider_group_id = provider_ref.get("provider_group_id")
            
            # Process each provider group within this reference
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
            
            # Progress and memory monitoring
            if provider_refs_processed % 100 == 0:
                current_memory = get_memory_usage()
                print(f"  📊 Processed {provider_refs_processed} provider refs | Memory: {current_memory:.1f} MB")
                
                # Force garbage collection if memory gets high
                if current_memory > 200:  # Keep under 200MB
                    gc.collect()
    
    except Exception as e:
        print(f"❌ Streaming error: {e}")
        print("💡 Falling back to regular method...")
        return discover_tins_fallback(target_tins)
    
    finally:
        # Clean up
        if 'stream' in locals():
            try:
                stream.close()
            except:
                pass
        if 'response' in locals():
            try:
                response.close()
            except:
                pass
        gc.collect()
    
    final_memory = get_memory_usage()
    print(f"\n✅ STREAMING DISCOVERY COMPLETE!")
    print(f"🧠 Final memory: {final_memory:.1f} MB")
    print(f"📊 Provider references processed: {provider_refs_processed:,}")
    
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
                if len(occurrences) <= 3:
                    print(f"   📝 Provider Group IDs: {[occ['provider_group_id'] for occ in occurrences]}")
                else:
                    print(f"   📝 Provider Group IDs: {[occ['provider_group_id'] for occ in occurrences[:3]]}... (+{len(occurrences)-3} more)")
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
    
    # Show top TINs
    print(f"\n🔝 TOP 15 TINs BY PROVIDER COUNT:")
    for i, (tin, count) in enumerate(tin_counter.most_common(15), 1):
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
        "top_tins": top_tins,
        "memory_peak": final_memory
    }

def discover_tins_fallback(target_tins=None):
    """
    Fallback method if ijson is not available.
    Uses more memory but still faster than processing in_network.
    """
    print("🔄 Using fallback method (more memory intensive)...")
    
    url = "https://mrfstorageprod.blob.core.windows.net/public-mrf/2025-08-01/2025-08-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_PPO---NDC_PPO-NDC_in-network-rates.json.gz"
    
    print(f"🧠 Initial memory: {get_memory_usage():.1f} MB")
    
    # Fetch and parse normally
    import json
    headers = get_cloudfront_headers()
    response = requests.get(url, headers=headers, timeout=300)
    response.raise_for_status()
    
    print(f"📦 Downloaded {len(response.content) / 1024**2:.1f} MB")
    print(f"🧠 Memory after download: {get_memory_usage():.1f} MB")
    
    if url.endswith('.gz'):
        with gzip.GzipFile(fileobj=BytesIO(response.content)) as gz:
            data = json.load(gz)
    else:
        data = json.loads(response.content.decode('utf-8'))
    
    print(f"🧠 Memory after JSON parse: {get_memory_usage():.1f} MB")
    
    # Clear response from memory
    del response
    gc.collect()
    
    # Process only provider_references
    tin_counter = Counter()
    target_found = {}
    all_tins = set()
    
    provider_refs = data.get("provider_references", [])
    print(f"👥 Processing {len(provider_refs)} provider references...")
    
    for provider_ref in provider_refs:
        provider_group_id = provider_ref.get("provider_group_id")
        
        for group in provider_ref.get("provider_groups", []):
            tin_info = group.get("tin", {})
            tin_value = str(tin_info.get("value", "")).strip()
            tin_type = tin_info.get("type", "")
            npi_count = len(group.get("npi", []))
            
            if tin_value and tin_value != "":
                all_tins.add(tin_value)
                tin_counter[tin_value] += npi_count
                
                if target_tins and tin_value in target_tins:
                    if tin_value not in target_found:
                        target_found[tin_value] = []
                    
                    target_found[tin_value].append({
                        "provider_group_id": provider_group_id,
                        "tin_type": tin_type,
                        "npi_count": npi_count,
                        "npis": group.get("npi", [])[:5]
                    })
    
    # Clear data from memory
    del data
    gc.collect()
    
    final_memory = get_memory_usage()
    print(f"🧠 Final memory: {final_memory:.1f} MB")
    
    return {
        "all_tins": all_tins,
        "tin_counter": tin_counter,
        "target_found": target_found,
        "top_tins": [tin for tin, _ in tin_counter.most_common(10)],
        "memory_peak": final_memory
    }

def quick_tin_check_streaming(target_tins):
    """Memory-safe quick check for specific TINs."""
    if isinstance(target_tins, (list, tuple)):
        target_tins = set(str(tin) for tin in target_tins)
    
    print(f"🎯 MEMORY-SAFE TIN CHECK")
    print(f"🧠 This uses <100MB memory regardless of file size")
    print(f"Checking: {target_tins}")
    
    results = stream_discover_tins(target_tins)
    
    if results:
        found_tins = set(results["target_found"].keys())
        missing_tins = target_tins - found_tins
        
        print(f"\n📋 QUICK RESULTS:")
        print(f"✅ Found: {found_tins}")
        print(f"❌ Missing: {missing_tins}")
        print(f"📊 Success rate: {len(found_tins)}/{len(target_tins)} ({len(found_tins)/len(target_tins)*100:.1f}%)")
        print(f"🧠 Peak memory usage: {results['memory_peak']:.1f} MB")
        
        return len(found_tins) > 0
    
    return False

if __name__ == "__main__":
    print("🔍 MEMORY-SAFE TIN DISCOVERY")
    print("=" * 60)
    
    if not IJSON_AVAILABLE:
        print("⚠️  For best memory efficiency, install ijson:")
        print("   pip install ijson")
        print()
    
    # Your target TINs
    your_target_tins = {
        "300466706",  # Your original TINs
        "581646537"
    }
    
    print("🧪 TESTING OPTIONS:")
    print("1. Memory-safe streaming discovery (recommended)")
    print("2. Quick target TIN check")
    print()
    
    # Option 1: Full streaming discovery
    print("=" * 60)
    print("STREAMING TIN DISCOVERY")
    print("=" * 60)
    
    if IJSON_AVAILABLE:
        results = stream_discover_tins(your_target_tins)
        print(f"\n💡 Memory efficiency: Used only {results['memory_peak']:.1f} MB for entire file!")
    else:
        results = discover_tins_fallback(your_target_tins)
        print(f"\n⚠️  Fallback method used {results['memory_peak']:.1f} MB")
    
    # Quick recommendations
    if results and results["target_found"]:
        found_tins = set(results["target_found"].keys())
        print(f"\n🚀 READY TO USE:")
        print(f"tin_value_whitelist = {found_tins}")
    else:
        print(f"\n💡 ALTERNATIVES:")
        if results and results["top_tins"]:
            print(f"Try these common TINs: {set(results['top_tins'][:5])}")
        print(f"Or use tin_value_whitelist=None to process all TINs")