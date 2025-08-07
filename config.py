"""
Configuration settings for RNAcentral GFF file fetching
"""
import os

# Database configuration
# Connection string will be read from environment variable PGDATABASE
DB_CONNECTION_ENV = 'PGDATABASE'

# RNAcentral FTP configuration
FTP_BASE_URL = "https://ftp.ebi.ac.uk/pub/databases/RNAcentral/releases"
RELEASE_START = 12
RELEASE_END = 25

# Directory configuration
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
LOG_DIR = os.path.join(BASE_DIR, 'logs')

# Download configuration
MAX_PARALLEL_DOWNLOADS = 5  # Number of parallel downloads
DOWNLOAD_TIMEOUT = 300  # Timeout in seconds for each download
MAX_RETRIES = 3  # Maximum number of retry attempts
RETRY_DELAY = 5  # Delay between retries in seconds

# Logging configuration
LOG_LEVEL = 'INFO'
MAIN_LOG_FILE = os.path.join(LOG_DIR, 'download.log')
ERROR_LOG_FILE = os.path.join(LOG_DIR, 'errors.log')
SUMMARY_FILE = os.path.join(LOG_DIR, 'download_summary.json')

# File patterns
GFF_FILE_PATTERN = "{organism}.{assembly}.gff3.gz"
GFF_URL_PATTERN = "{base_url}/{release}/genome_coordinates/gff3/{filename}"

# Database query
DB_QUERY = """
SELECT 
    esp.taxid,
    rnc_taxonomy.name as organism_name
FROM ensembl_stable_prefixes esp 
JOIN rnc_taxonomy ON esp.taxid = rnc_taxonomy.id
ORDER BY rnc_taxonomy.name
"""
