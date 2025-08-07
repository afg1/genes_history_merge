#!/usr/bin/env python3
"""
Utility script to test database connection and preview organisms
"""

import os
import sys
import psycopg2
from psycopg2.extras import RealDictCursor
import config
from fetch_rnacentral_gff import transform_organism_name

def test_db_connection():
    """Test database connection and show sample organisms"""
    
    # Get database connection string from environment
    db_conn_str = os.environ.get(config.DB_CONNECTION_ENV)
    if not db_conn_str:
        print(f"ERROR: Database connection string not found in environment variable {config.DB_CONNECTION_ENV}")
        print(f"Please set the environment variable with your PostgreSQL connection string")
        print(f"Example: export PGDATABASE='postgresql://user:password@host:port/dbname'")
        return False
    
    print(f"Testing database connection...")
    print("-" * 60)
    
    try:
        # Connect to database
        conn = psycopg2.connect(db_conn_str)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Test query - get count first
        count_query = """
        SELECT COUNT(*) as count
        FROM ensembl_stable_prefixes esp 
        JOIN rnc_taxonomy ON esp.taxid = rnc_taxonomy.id
        """
        cursor.execute(count_query)
        count_result = cursor.fetchone()
        total_organisms = count_result['count']
        
        print(f"✓ Database connection successful!")
        print(f"✓ Found {total_organisms} organisms in database")
        print("-" * 60)
        
        # Get sample organisms
        sample_query = """
        SELECT 
            esp.taxid,
            rnc_taxonomy.name as organism_name
        FROM ensembl_stable_prefixes esp 
        JOIN rnc_taxonomy ON esp.taxid = rnc_taxonomy.id
        ORDER BY rnc_taxonomy.name
        LIMIT 10
        """
        cursor.execute(sample_query)
        samples = cursor.fetchall()
        
        print("Sample organisms (first 10):")
        print("-" * 60)
        print(f"{'TaxID':<10} {'Organism Name':<30} {'Transformed Name':<30}")
        print("-" * 60)
        
        for organism in samples:
            transformed = transform_organism_name(organism['organism_name'])
            print(f"{organism['taxid']:<10} {organism['organism_name']:<30} {transformed:<30}")
        
        print("-" * 60)
        
        # Test specific organisms that should have GFF files
        test_organisms = ['Homo sapiens', 'Mus musculus', 'Drosophila melanogaster']
        test_query = """
        SELECT 
            esp.taxid,
            rnc_taxonomy.name as organism_name
        FROM ensembl_stable_prefixes esp 
        JOIN rnc_taxonomy ON esp.taxid = rnc_taxonomy.id
        WHERE rnc_taxonomy.name = ANY(%s)
        """
        cursor.execute(test_query, (test_organisms,))
        test_results = cursor.fetchall()
        
        if test_results:
            print("\nCommon model organisms found:")
            for organism in test_results:
                print(f"  - {organism['organism_name']} (taxid: {organism['taxid']})")
        
        # Close connection
        cursor.close()
        conn.close()
        
        return True
        
    except psycopg2.OperationalError as e:
        print(f"✗ Database connection failed!")
        print(f"Error: {str(e)}")
        print("\nPlease check:")
        print("1. The PGDATABASE environment variable is set correctly")
        print("2. The database server is accessible")
        print("3. Your credentials are correct")
        return False
        
    except Exception as e:
        print(f"✗ Unexpected error: {str(e)}")
        return False


def test_single_download():
    """Test downloading a single file to verify setup"""
    print("\n" + "="*60)
    print("Testing single file download")
    print("="*60)
    
    # Test with a known organism and release
    test_url = f"{config.FTP_BASE_URL}/12.0/genome_coordinates/gff3/homo_sapiens.GRCh38.gff3.gz"
    test_output = "/tmp/test_rnacentral.gff3.gz"
    
    print(f"Test URL: {test_url}")
    print(f"Testing download with wget...")
    
    import subprocess
    
    try:
        result = subprocess.run(
            ['wget', '--timeout=30', '--tries=1', '-O', test_output, test_url],
            capture_output=True,
            text=True,
            timeout=35
        )
        
        if result.returncode == 0:
            # Check file size
            if os.path.exists(test_output):
                size = os.path.getsize(test_output)
                print(f"✓ Download successful! File size: {size:,} bytes")
                os.remove(test_output)
                return True
            else:
                print("✗ Download appeared successful but file not found")
                return False
        else:
            print(f"✗ Download failed with return code: {result.returncode}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("✗ Download timeout")
        return False
    except FileNotFoundError:
        print("✗ wget not found. Please install wget: sudo apt-get install wget")
        return False
    except Exception as e:
        print(f"✗ Unexpected error: {str(e)}")
        return False


if __name__ == "__main__":
    print("RNAcentral GFF Fetcher - Setup Test")
    print("="*60)
    
    # Test database connection
    db_ok = test_db_connection()
    
    # Test download capability
    download_ok = test_single_download()
    
    # Summary
    print("\n" + "="*60)
    print("Setup Test Summary")
    print("="*60)
    print(f"Database connection: {'✓ OK' if db_ok else '✗ FAILED'}")
    print(f"Download capability: {'✓ OK' if download_ok else '✗ FAILED'}")
    
    if db_ok and download_ok:
        print("\n✓ All tests passed! You can run the main script:")
        print("  python fetch_rnacentral_gff.py")
    else:
        print("\n✗ Some tests failed. Please fix the issues above before running the main script.")
        sys.exit(1)
