#!/usr/bin/env python3
import sys
import json
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
import re
import traceback


def parse_arguments():
    """Parse job arguments from Glue."""
    try:
        args = getResolvedOptions(sys.argv, [
            'database',
            'table',
            'partitions-json' 
        ])
        if 'partitions-json' in args:
            args['partitions_json'] = args.pop('partitions-json')
    except Exception:
        args = getResolvedOptions(sys.argv, [
            'database',
            'table', 
            'partitions_json'
        ])
    
    return args


def parse_partitions_json(partitions_json):
    """Parse and validate partition configuration."""
    try:
        partitions = json.loads(partitions_json)
        
        if not isinstance(partitions, dict):
            raise ValueError("Partitions JSON must be an object/dict")
        
        # Validate transforms
        valid_transforms = ['day', 'month', 'year']
        for column, transform in partitions.items():
            if transform not in valid_transforms:
                raise ValueError(
                    f"Invalid transform '{transform}' for column '{column}'. "
                    f"Must be one of: {valid_transforms}"
                )
        
        return partitions
        
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON format: {e}")
        sys.exit(1)


def apply_partitions(spark, database, table, partitions):
    """Apply hidden partitions to the Iceberg table."""
    print(f"\nApplying partitions to glue_catalog.{database}.{table}...")
    success_count = 0
    
    for column, transform in partitions.items():
        transform_plural = f"{transform}s"
        
        alter_sql = (
            f"ALTER TABLE glue_catalog.{database}.{table} "
            f"ADD PARTITION FIELD {transform_plural}({column})"
        )
        
        print(f"\nExecuting: {alter_sql}")
        
        spark.sql(alter_sql)
        print(f" Added {transform} partition on {column}")
        success_count += 1
        
    
    if success_count == len(partitions):
        print(f"\n Successfully applied all {success_count} partition(s)")
    
    return success_count


def verify_partitions(spark, database, table, expected_partitions):
    """Verify that partitions were applied correctly."""
    query = f"SHOW CREATE TABLE glue_catalog.{database}.{table}"
    result = spark.sql(query)

    print("\nVerifying partitions by parsing DDL...")
    rows = result.collect()

    ddl_text = ""
    for row in rows:
        if len(row) >= 1:
            line = str(row[0])
            ddl_text += line + "\n"
    
    print(f"DDL text: {ddl_text[:200]}...")
    
    current_partitions = {}
    
    partition_pattern = r'PARTITIONED\s+BY\s*\((.+?)\)\s*(?:LOCATION|TBLPROPERTIES|$)'
    match = re.search(partition_pattern, ddl_text, re.IGNORECASE | re.DOTALL)
    
    if match:
        partition_clause = match.group(1).strip()
        print(f"Found partition clause: {partition_clause}")

        # Parse individual partition fields
        field_patterns = [
            (r'days?\s*\(\s*`?(\w+)`?\s*\)', 'day'),
            (r'months?\s*\(\s*`?(\w+)`?\s*\)', 'month'),
            (r'years?\s*\(\s*`?(\w+)`?\s*\)', 'year')
        ]

        for pattern, transform in field_patterns:
            matches = re.findall(pattern, partition_clause, re.IGNORECASE)
            for column in matches:
                current_partitions[column] = transform
    
    print(f"Current partitions found: {current_partitions}")
    print(f"Expected partitions: {expected_partitions}")
    
    # Check if partitions match expected
    if current_partitions != expected_partitions:
        raise RuntimeError(
            f"Partition verification failed for table {database}.{table}. "
            f"Expected: {expected_partitions}, Found: {current_partitions}"
        )
    
    print("\n Partitions verified successfully - all expected partitions found")


def main():
    
    # Parse arguments
    print("\n[STEP 1/5] Parsing job arguments...")
    args = parse_arguments()
    
    database = args['database']
    table = args['table']
    partitions_json = args['partitions_json']
    
    print(f"  Database: {database}")
    print(f"  Table: {table}")
    print(f"  Partitions JSON: {partitions_json}")
    
    # Parse partitions
    print("\n[STEP 2/5] Parsing partition configuration...")
    partitions = parse_partitions_json(partitions_json)
    print(f"  Parsed partitions: {partitions}")
    
    # Setup Spark session - use Iceberg catalog
    print("\n[STEP 3/5] Setting up Spark session with Iceberg catalog...")
    conf = SparkConf()
    conf.set("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set("spark.sql.catalog.glue_catalog",
            "org.apache.iceberg.spark.SparkCatalog")
    conf.set("spark.sql.catalog.glue_catalog.catalog-impl",
            "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set("spark.sql.catalog.glue_catalog.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")
    conf.set("spark.sql.catalog.glue_catalog.skip-archive", "true")

    sc = SparkContext(conf=conf)
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    
    # Apply partitions
    print("\n[STEP 4/5] Applying partitions...")
    try:
        success_count = apply_partitions(spark, database, table, partitions)
    except Exception as e:
        print(f"\n Error applying partitions: {e}")
        traceback.print_exc()
        raise
    
    # Verify partitions
    print("\n[STEP 5/5] Verifying partitions...")
    verify_partitions(spark, database, table, partitions)
    
    job.commit()


if __name__ == "__main__":
    main()
