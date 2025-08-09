# Data Extraction Tools

This directory contains tools for extracting and processing data from Machine-Readable Files (MRF). The tools are designed to work together in a pipeline for efficient data extraction and analysis.

## Tools Overview

### 1. `extract_providers.py` - Provider Reference Extraction
Extracts provider information including NPIs, TIN values, and provider group IDs from MRF files.

### 2. `extract_rates.py` - Negotiated Rates Extraction  
Extracts negotiated rates data with efficient filtering based on provider groups or CPT codes.

## Quick Pipeline Example

```bash
# Step 1: Extract providers with TIN filtering
python extract_providers.py file.json.gz --tin-whitelist my_tins.txt

# Step 2: Extract rates using the provider output for filtering
python extract_rates.py file.json.gz --provider-groups-parquet output/providers_20250108_143022.parquet
```

## extract_providers.py

### Basic Usage
```bash
# Extract all providers from a local file
python extract_providers.py path/to/file.json.gz

# Extract providers from a URL
python extract_providers.py https://example.com/mrf-file.json.gz

# Limit processing to first 1000 provider groups
python extract_providers.py file.json.gz --max-providers 1000
```

### Filtering Options

#### Filter by TIN Values
```bash
# Create TIN whitelist file
echo "123456789" > my_tins.txt
echo "987654321" >> my_tins.txt
echo "555666777" >> my_tins.txt

# Run extraction with TIN filtering
python extract_providers.py file.json.gz --tin-whitelist my_tins.txt
```

#### Filter by Provider Group IDs
```bash
python extract_providers.py file.json.gz --provider-whitelist existing_providers.parquet
```

#### Combine Both Filters
```bash
python extract_providers.py file.json.gz \
  --provider-whitelist existing_providers.parquet \
  --tin-whitelist my_tins.txt
```

### Command Line Arguments

| Argument | Short | Type | Description |
|----------|-------|------|-------------|
| `source` | - | str | **Required.** URL or local path to MRF file |
| `--max-providers` | `-m` | int | Maximum number of provider groups to process |
| `--provider-whitelist` | `-p` | str | Path to Parquet file with `provider_reference_id` column |
| `--tin-whitelist` | `-t` | str | Path to text file with TIN values (one per line) |
| `--batch-size` | `-b` | int | Batch size for writing (default: 1000) |

### Output Schema
| Column | Type | Description |
|--------|------|-------------|
| `provider_group_id` | int | Unique identifier for provider group |
| `npi` | str | National Provider Identifier |
| `tin_type` | str | Type of Tax Identification Number |
| `tin_value` | str | Tax Identification Number value |
| `reporting_entity_name` | str | Name of reporting entity |
| `reporting_entity_type` | str | Type of reporting entity |
| `last_updated_on` | str | Last update timestamp |
| `version` | str | File version |

## extract_rates.py

### Basic Usage
```bash
# Extract all rates from a local file
python extract_rates.py path/to/file.json.gz

# Extract rates from a URL with time limit
python extract_rates.py https://example.com/mrf-file.json.gz --time 30
```

### Filtering Options

#### Filter by Provider Groups from Parquet File (NEW!)
```bash
# Use output from extract_providers.py to filter rates
python extract_rates.py file.json.gz --provider-groups-parquet output/providers_20250108_143022.parquet
```

#### Filter by Specific Provider Group IDs
```bash
python extract_rates.py file.json.gz --provider-groups 12345 67890 11111
```

#### Filter by CPT Codes
```bash
# Create CPT whitelist file
echo "99213" > cpt_codes.txt
echo "99214" >> cpt_codes.txt
echo "27447" >> cpt_codes.txt

python extract_rates.py file.json.gz --cpt-whitelist cpt_codes.txt
```

#### Combine Multiple Filters
```bash
python extract_rates.py file.json.gz \
  --provider-groups-parquet output/providers_filtered.parquet \
  --cpt-whitelist orthopedic_cpts.txt \
  --items 10000
```

### Command Line Arguments

| Argument | Short | Type | Description |
|----------|-------|------|-------------|
| `source` | - | str | **Required.** URL or local path to MRF file |
| `--items` | `-i` | int | Maximum number of items to process |
| `--time` | `-t` | int | Maximum time to run in minutes |
| `--provider-groups` | `-p` | int+ | Provider group IDs to filter for |
| `--provider-groups-parquet` | `-pp` | str | **NEW!** Path to Parquet file with provider_group_id column |
| `--cpt-whitelist` | `-c` | str | Path to text file with CPT codes (one per line) |
| `--batch-size` | `-b` | int | Batch size for writing (default: 5) |

### Output Schema
| Column | Type | Description |
|--------|------|-------------|
| `provider_reference_id` | int | Provider group reference ID |
| `billing_code` | str | Billing/CPT code |
| `billing_code_type` | str | Type of billing code |
| `description` | str | Service description |
| `negotiated_rate` | float | Negotiated rate amount |
| `negotiated_type` | str | Type of negotiation |
| `billing_class` | str | Billing class |
| `expiration_date` | str | Rate expiration date |
| `service_codes` | str | Additional service codes |
| `reporting_entity_name` | str | Name of reporting entity |
| `reporting_entity_type` | str | Type of reporting entity |
| `last_updated_on` | str | Last update timestamp |
| `version` | str | File version |

## Complete Pipeline Examples

### Example 1: Orthopedic Practice Analysis
```bash
# Step 1: Create TIN whitelist for orthopedic practices
cat > orthopedic_tins.txt << EOF
123456789
234567890
345678901
EOF

# Step 2: Extract providers for these TINs
python extract_providers.py large_mrf_file.json.gz \
  --tin-whitelist orthopedic_tins.txt \
  --max-providers 5000

# Step 3: Extract rates using the filtered providers
python extract_rates.py large_mrf_file.json.gz \
  --provider-groups-parquet output/providers_20250108_143022.parquet \
  --cpt-whitelist orthopedic_cpts.txt
```

### Example 2: Incremental Processing for Large Files
```bash
# Step 1: Quick provider scan to identify relevant groups
python extract_providers.py huge_file.json.gz --max-providers 1000

# Step 2: Analyze results and create focused extraction
python extract_rates.py huge_file.json.gz \
  --provider-groups-parquet output/providers_sample.parquet \
  --items 50000 \
  --time 60
```

### Example 3: Multi-Filter Rate Extraction
```bash
# Combine provider filtering with CPT filtering for targeted analysis
python extract_rates.py file.json.gz \
  --provider-groups-parquet output/high_value_providers.parquet \
  --provider-groups 99999 88888 \
  --cpt-whitelist high_cost_procedures.txt \
  --batch-size 10
```

## Performance Tips

1. **Provider Filtering**: Always use provider filtering when possible - it's much more efficient than processing all rates
2. **Pipeline Approach**: Use `extract_providers.py` first to identify relevant providers, then use that output for rate extraction
3. **Batch Sizes**: 
   - Providers: 500-1000 for most files
   - Rates: 5-20 depending on filtering (more filtering = larger batches OK)
4. **Memory Management**: Use time/item limits for initial exploration of large files

## Output Files

All files are saved to the `output/` directory with timestamps:
```
output/
├── providers_20250108_143022.parquet
└── rates_20250108_144511.parquet
```

## Integration Notes

- Provider extraction output can be directly used as input for rate filtering
- Both tools support the same MRF file formats
- Parquet outputs are efficient for subsequent analysis
- Tools handle memory management automatically with batching

## Troubleshooting

### Common Issues

1. **No Output**: Check whitelist files contain valid data
2. **Memory Errors**: Reduce batch sizes or add more filtering
3. **Slow Processing**: Ensure provider group filtering is applied first

### Performance Monitoring

Both tools provide detailed progress information:
- Memory usage tracking
- Processing statistics
- Filtering effectiveness metrics
- Time estimates

For additional help, check the main repository documentation or examine the tool output logs for debugging information.