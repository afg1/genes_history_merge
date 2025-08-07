# RNAcentral GFF File Fetcher

This tool downloads GFF (General Feature Format) files from RNAcentral for multiple organisms across different releases.

## Overview

The fetcher:
1. Queries your database to get a list of organisms (taxids and names)
2. Downloads GFF files from RNAcentral FTP site for releases 12-25
3. Handles missing files gracefully
4. Decompresses the downloaded files
5. Provides detailed logging and summary reports

## Prerequisites

- Python 3.6+
- PostgreSQL database with organism data
- `wget` command-line tool
- Internet connection with access to RNAcentral FTP

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Ensure `wget` is installed:
```bash
# On Ubuntu/Debian:
sudo apt-get install wget

# On macOS:
brew install wget

# On CentOS/RHEL:
sudo yum install wget
```

## Configuration

1. Set the database connection string environment variable:
```bash
export PGDATABASE='postgresql://username:password@host:port/database'
```

2. Adjust settings in `config.py` if needed:
   - `RELEASE_START` and `RELEASE_END`: Range of RNAcentral releases to download
   - `MAX_PARALLEL_DOWNLOADS`: Number of concurrent downloads
   - `DOWNLOAD_TIMEOUT`: Timeout for each download in seconds
   - `MAX_RETRIES`: Number of retry attempts for failed downloads

## Usage

### Test Setup

First, verify your setup is working:

```bash
python test_setup.py
```

This will:
- Test database connectivity
- Show sample organisms from your database
- Test downloading a single file from RNAcentral

### Run the Fetcher

Once setup is verified, run the main script:

```bash
python fetch_rnacentral_gff.py
```

The script will:
1. Query the database for organisms
2. Generate download URLs for each organism/release combination
3. Download files in parallel
4. Decompress successful downloads
5. Generate a summary report

### Monitor Progress

- **Console output**: Real-time progress updates
- **Log files**: 
  - `logs/download.log`: Main activity log
  - `logs/errors.log`: Error-only log
  - `logs/download_summary.json`: Detailed JSON summary

## Output Structure

Downloaded files are organized as:
```
data/
├── release_12/
│   ├── homo_sapiens/
│   │   ├── homo_sapiens.gff3.gz (compressed)
│   │   └── homo_sapiens.gff3 (decompressed)
│   ├── mus_musculus/
│   │   └── ...
│   └── ...
├── release_13/
│   └── ...
└── ...
```

## Handling Common Issues

### Database Connection Errors
- Verify PGDATABASE environment variable is set correctly
- Check network connectivity to database
- Verify database credentials

### Download Failures
- Check internet connectivity
- Verify access to https://ftp.ebi.ac.uk
- Some organisms may not have GFF files in all releases (this is normal)
- Check `logs/errors.log` for specific error messages

### Insufficient Disk Space
- Each GFF file can be 10-100MB compressed
- Decompressed files are typically 3-10x larger
- Estimate: ~265 organisms × 14 releases × 100MB = ~365GB total (worst case)
- Monitor disk space during download

### Performance Tuning
- Adjust `MAX_PARALLEL_DOWNLOADS` based on your network capacity
- Increase `DOWNLOAD_TIMEOUT` for slow connections
- Consider downloading releases sequentially if bandwidth is limited

## Advanced Usage

### Download Specific Releases

Modify `config.py` to change the release range:
```python
RELEASE_START = 20  # Start from release 20
RELEASE_END = 25    # End at release 25
```

### Filter Organisms

Add a WHERE clause to the query in `config.py`:
```python
DB_QUERY = """
SELECT 
    esp.taxid,
    rnc_taxonomy.name as organism_name
FROM ensembl_stable_prefixes esp 
JOIN rnc_taxonomy ON esp.taxid = rnc_taxonomy.id
WHERE rnc_taxonomy.name LIKE 'Homo%'  -- Only human-related organisms
ORDER BY rnc_taxonomy.name
"""
```

### Resume Failed Downloads

The script creates a summary JSON file. You can modify the script to read this file and retry only failed downloads.

## File Format

The downloaded GFF3 files contain genomic annotations including:
- Non-coding RNA locations
- Gene features
- Transcript information
- Exon boundaries

Example GFF3 content:
```
##gff-version 3
chr1    RNAcentral    gene    11869    14409    .    +    .    ID=gene:URS0000000001;Name=DDX11L1
chr1    RNAcentral    transcript    11869    14409    .    +    .    ID=transcript:URS0000000001_9606;Parent=gene:URS0000000001
```

## Troubleshooting

### Enable Debug Logging

Change `LOG_LEVEL` in `config.py`:
```python
LOG_LEVEL = 'DEBUG'  # More verbose logging
```

### Test Single Organism Download

Create a test script to download files for a single organism:
```python
import subprocess

organism = "homo_sapiens"
release = 12
url = f"https://ftp.ebi.ac.uk/pub/databases/RNAcentral/releases/{release}.0/genome_coordinates/gff3/{organism}.GRCh38.gff3.gz"
output = f"test_{organism}_r{release}.gff3.gz"

subprocess.run(['wget', url, '-O', output])
```

## Support

For issues related to:
- **RNAcentral data**: Contact RNAcentral support or check https://rnacentral.org
- **Database queries**: Verify your database schema matches expected structure
- **Script bugs**: Check the error logs and debug output

## License

This tool is provided as-is for research purposes.

## Acknowledgments

Data source: RNAcentral (https://rnacentral.org) - a comprehensive database of non-coding RNA sequences.