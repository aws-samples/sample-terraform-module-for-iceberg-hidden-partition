#!/usr/bin/env python3
import argparse
import json
import sys
import boto3
from pathlib import Path
import time
import re


def parse_partitions_json(partitions_json):

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


def get_current_partitions_from_athena(database, table, s3_output_location, region='us-east-1'):

    athena_client = boto3.client('athena', region_name=region)
    # Wait for table to exist with exponential backoff
    print(f"  Waiting for table {database}.{table} to be ready...")
    wait_time = 1
    max_wait = 600
    elapsed = 0
    
    while elapsed < max_wait:
        query = f"SHOW CREATE TABLE {database}.{table}"
        
        try:
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={'OutputLocation': s3_output_location}
            )
            query_id = response['QueryExecutionId']

            query_wait = 0
            while query_wait < 30:
                result = athena_client.get_query_execution(QueryExecutionId=query_id)
                status = result['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    print(f"   Table ready, fetching partitions...")
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error = result['QueryExecution']['Status'].get('StateChangeReason', '')
                    if 'not found' in error.lower() or 'does not exist' in error.lower():
                        # Table doesn't exist yet
                        break
                    print(f"   Query failed: {error}")
                    return {}
                
                time.sleep(1)
                query_wait += 1
            
            if status == 'SUCCEEDED':
                # Parse results
                results = athena_client.get_query_results(QueryExecutionId=query_id)
                current_partitions = {}
                ddl_text = ""

                for row in results['ResultSet']['Rows']:
                    if len(row['Data']) >= 1 and 'VarCharValue' in row['Data'][0]:
                        line = row['Data'][0]['VarCharValue']
                        ddl_text += line + "\n"

                partition_pattern = r'PARTITIONED\s+BY\s*\((.+?)\)\s*(?:LOCATION|TBLPROPERTIES|$)'
                match = re.search(partition_pattern, ddl_text, re.IGNORECASE | re.DOTALL)
                
                if match:
                    partition_clause = match.group(1).strip()
                    field_patterns = [
                        (r'days?\s*\(\s*`?(\w+)`?\s*\)', 'day'),
                        (r'months?\s*\(\s*`?(\w+)`?\s*\)', 'month'),
                        (r'years?\s*\(\s*`?(\w+)`?\s*\)', 'year')
                    ]

                    for pattern, transform in field_patterns:
                        matches = re.findall(pattern, partition_clause, re.IGNORECASE)
                        for column in matches:
                            current_partitions[column] = transform

                return current_partitions
                
        except Exception as e:
            pass
        
        # Exponential backoff
        print(f"  [{elapsed}s] Table not ready, waiting {wait_time}s...")
        time.sleep(wait_time)
        elapsed += wait_time
        wait_time = min(wait_time * 2, 30)
    
    raise TimeoutError(f"Table {database}.{table} not ready after {max_wait}s")


def check_glue_job_exists(job_name, region='us-east-1'):
    """Check if Glue job exists."""
    try:
        glue_client = boto3.client('glue', region_name=region)
        
        print(f"\n Checking if Glue job exists...")
        print(f"  Job Name: {job_name}")
        
        response = glue_client.get_job(JobName=job_name)
        
        print(f" Glue job found!")
        return True, response['Job'].get('Role')
        
    except glue_client.exceptions.EntityNotFoundException:
        print(f"  Glue job '{job_name}' does not exist")
        return False, None


def create_database_glue_job(job_name, role_arn, script_path, s3_bucket, region='us-east-1'):
    """Create a shared Glue job for the database (if it doesn't exist)."""

    s3_client = boto3.client('s3', region_name=region)
    glue_client = boto3.client('glue', region_name=region)
    
    # Read script content
    with open(script_path, 'r') as f:
        script_content = f.read()
    
    # Upload script to S3 (use consistent key for the database)
    script_key = f"scripts/{job_name}.py"
    print(f"  Uploading script to s3://{s3_bucket}/{script_key}")
    
    s3_client.put_object(
        Bucket=s3_bucket,
        Key=script_key,
        Body=script_content
    )
    
    glue_client.create_job(
        Name=job_name,
        Role=role_arn,
        Command={
            'Name': 'glueetl',
            'ScriptLocation': f's3://{s3_bucket}/{script_key}',
            'PythonVersion': '3'
        },
        DefaultArguments={
            '--enable-metrics': 'true',
            '--enable-glue-datacatalog': 'true',
            '--job-bookmark-option': 'job-bookmark-disable',
            '--datalake-formats': 'iceberg',
            '--job-language': 'python',
            '--TempDir': f's3://{s3_bucket}/temporary/',
            '--conf': 'spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.glue_catalog.skip-archive=true'
        },
        GlueVersion='5.0',
        WorkerType='G.1X',
        NumberOfWorkers=2,
        Timeout=60,
        MaxRetries=0,
        ExecutionProperty={
            'MaxConcurrentRuns': 500
        }
    )
    return True, script_key
        


def trigger_glue_job(job_name, database, table, partitions_json, region='us-east-1'):
    glue_client = boto3.client('glue', region_name=region)
    
    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments={
            '--database': database,
            '--table': table,
            '--partitions-json': partitions_json,
            '--enable-glue-datacatalog': 'true'
        }
    )
    
    job_run_id = response['JobRunId']
    print(f"\n Started Glue job: {job_name}")
    print(f"  Job Run ID: {job_run_id}")
    
    return job_run_id


def wait_for_glue_job(job_name, job_run_id, region='us-east-1'):
    """Wait for Glue job to complete."""
    
    glue_client = boto3.client('glue', region_name=region)
    print(f"\nWaiting for Glue job to complete...")
    start_time = time.time()
    last_state = None
    
    while True:
        response = glue_client.get_job_run(
            JobName=job_name,
            RunId=job_run_id
        )
        
        job_run = response['JobRun']
        state = job_run['JobRunState']
        
        if state != last_state:
            elapsed = int(time.time() - start_time)
            print(f"  [{elapsed}s] Status: {state}")
            last_state = state
        
        if state == 'SUCCEEDED':
            elapsed = int(time.time() - start_time)
            print(f"\n Glue job completed successfully in {elapsed}s")
            return True
        
        elif state in ['FAILED', 'STOPPED', 'ERROR', 'TIMEOUT']:
            elapsed = int(time.time() - start_time)
            error_message = job_run.get('ErrorMessage', 'No error message')
            print(f"\n Glue job failed after {elapsed}s")
            print(f"  Error: {error_message}")
            return False
        
        time.sleep(10)


def delete_glue_job(job_name, s3_bucket, region='us-east-1'):
    """Delete the Glue job and its script from S3."""

    s3_client = boto3.client('s3', region_name=region)
    glue_client = boto3.client('glue', region_name=region)
    
    print(f"\n  Cleaning up Glue job: {job_name}")
    
    # Delete Glue-job which was just created for this partitioning task (if it exists)
    try:
        glue_client.delete_job(JobName=job_name)
        print(f"   Deleted Glue job: {job_name}")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"    Glue job '{job_name}' not found (may have been deleted already)")
    except Exception as e:
        print(f"   Could not delete Glue job: {e}")
    
    # Delete script from S3
    script_key = f"scripts/{job_name}.py"
    try:
        s3_client.delete_object(Bucket=s3_bucket, Key=script_key)
        print(f"   Deleted script from S3: {script_key}")
    except Exception as e:
        print(f"    Could not delete script from S3: {e}")
    
    print(f" Cleanup completed for {job_name}")
    return True


def cleanup_all_iceberg_jobs(s3_bucket, region='us-east-1', pattern='iceberg-partition-'):
    """Find and delete all Glue jobs matching the pattern, waiting for running jobs to complete first."""

    glue_client = boto3.client('glue', region_name=region)
    print(f"\n Finding all Glue jobs matching pattern: {pattern}*")
    
    # List all jobs
    paginator = glue_client.get_paginator('get_jobs')
    matching_jobs = []
    
    for page in paginator.paginate():
        for job in page['Jobs']:
            job_name = job['Name']
            if job_name.startswith(pattern):
                matching_jobs.append(job_name)
    
    if not matching_jobs:
        print(f"    No jobs found matching pattern '{pattern}*'")
        return True
    
    print(f"  Found {len(matching_jobs)} job(s) to delete:")
    for job_name in matching_jobs:
        print(f"    - {job_name}")
    
    # Check for running jobs and wait for them to complete
    print(f"\n Checking for running job instances...")
    max_wait_time = 600
    start_time = time.time()
    
    while True:
        running_jobs = []
        
        for job_name in matching_jobs:
            try:
                response = glue_client.get_job_runs(JobName=job_name, MaxResults=10)
                
                for run in response['JobRuns']:
                    if run['JobRunState'] in ['RUNNING', 'WAITING', 'STARTING']:
                        running_jobs.append({
                            'job': job_name,
                            'run_id': run['Id'],
                            'state': run['JobRunState']
                        })
            except Exception as e:
                print(f"    Could not check runs for {job_name}: {e}")
        
        if not running_jobs:
            print(f"   No running jobs found - safe to delete")
            break
        
        elapsed = int(time.time() - start_time)
        if elapsed > max_wait_time:
            print(f"\n    Timeout waiting for jobs to complete ({max_wait_time}s)")
            print(f"  Still running:")
            for job in running_jobs:
                print(f"    - {job['job']} (Run: {job['run_id'][:8]}..., State: {job['state']})")
            print(f"  Proceeding with deletion anyway...")
            break
        
        print(f"  [{elapsed}s] Waiting for {len(running_jobs)} job run(s) to complete...")
        for job in running_jobs[:3]:
            print(f"    - {job['job']}: {job['state']}")
        
        time.sleep(30)
    
    # Delete each job
    print(f"\n Deleting jobs...")
    success_count = 0
    for job_name in matching_jobs:
        if delete_glue_job(job_name, s3_bucket, region):
            success_count += 1
    
    print(f"\n Successfully deleted {success_count}/{len(matching_jobs)} job(s)")
    return success_count == len(matching_jobs)


def main():
    parser = argparse.ArgumentParser(
        description='Iceberg partitioning with database-level Glue job management'
    )
    
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--partitions-json', required=True, help='Partition config as JSON')
    parser.add_argument('--glue-job', required=True, help='Glue job name (database-level)')
    parser.add_argument('--glue-job-role', required=True, help='Deploy role ARN for Glue job')
    parser.add_argument('--script-path', required=True, help='Path to partition.py script')
    parser.add_argument('--s3-output', required=True, help='S3 location for Athena results')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    
    args = parser.parse_args()

    bucket_match = re.match(r's3://([^/]+)/', args.s3_output)
    if not bucket_match:
        print(" Invalid S3 output location")
        sys.exit(1)
    s3_bucket = bucket_match.group(1)

    # Step 1: Parse partitions
    print("\n[STEP 1/7] Parsing Partition Configuration")
    partitions = parse_partitions_json(args.partitions_json)
    print(f" Parsed partitions: {partitions}")
    
    # Step 2: Check current partitions
    print("\n[STEP 2/7] Checking Current Partitions")
    current_partitions = get_current_partitions_from_athena(
        database=args.database,
        table=args.table,
        s3_output_location=args.s3_output,
        region=args.region
    )
    
    print(f"  Current: {current_partitions if current_partitions else 'None'}")
    print(f"  Desired: {partitions}")
    
    if current_partitions == partitions:
        print(f"\n Partitions already match - no action needed")
        sys.exit(0)
    
    print(f"\n Partitions differ - applying changes")
    
    # Step 3: Check/Create Glue job
    print("\n[STEP 3/6] Managing Glue Job")
    job_exists, existing_role = check_glue_job_exists(args.glue_job, args.region)
    
    created_new_job = False
    script_key = None
    
    if not job_exists:
        print(f"  Creating shared Glue job for database...")
        success, script_key = create_database_glue_job(
            job_name=args.glue_job,
            role_arn=args.glue_job_role,
            script_path=args.script_path,
            s3_bucket=s3_bucket,
            region=args.region
        )
        if not success:
            print(" Failed to create Glue job")
            sys.exit(1)
        created_new_job = True
    else:
        print(f"  Using existing Glue job (shared across tables)")
    
    # Step 4: Trigger Glue job
    print("\n[STEP 4/6] Triggering Glue Job")
    job_run_id = trigger_glue_job(
        job_name=args.glue_job,
        database=args.database,
        table=args.table,
        partitions_json=args.partitions_json,
        region=args.region
    )
    
    # Step 5: Wait for completion
    print("\n[STEP 5/6] Waiting for Job Completion")
    success = wait_for_glue_job(
        job_name=args.glue_job,
        job_run_id=job_run_id,
        region=args.region
    )
    
    # Step 6: Keep job for reuse (cleanup handled separately)
    print("\n[STEP 6/7] Job Management")
    if created_new_job:
        print(f" Glue job '{args.glue_job}' created and will be reused for other tables")
    else:
        print(f" Glue job '{args.glue_job}' reused successfully")
    
    # Step 7: Cleanup ALL iceberg-partition-* jobs
    print("\n[STEP 7/7] Cleanup All Iceberg Jobs")
    print(f"  Deleting all iceberg-partition-* jobs...")
    cleanup_all_iceberg_jobs(s3_bucket, args.region, 'iceberg-partition-')

    if not success:
        print(" FAILED: Partition job failed!")
        sys.exit(1)
    
    print(" SUCCESS: Partitioning completed and all jobs cleaned up!")


if __name__ == "__main__":
    main()
