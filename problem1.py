#!/usr/bin/env python3
"""
Problem 1: Log Level Distribution
---------------------------------
Analyze the distribution of log levels (INFO, WARN, ERROR, DEBUG)
across all Spark log files in your cluster S3 bucket.
"""

import os
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, count, rand
import shutil
from glob import glob

def write_single_csv(df, output_dir, final_filename):
    """Write a Spark DataFrame to one flat CSV file using pandas."""
    # Convert to pandas (be careful with large datasets)
    pandas_df = df.toPandas()
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    final_path = os.path.join(output_dir, final_filename)
    
    # Write to CSV
    pandas_df.to_csv(final_path, index=False)
    print(f"✅ Wrote single CSV using pandas: {final_path}")
    
def create_spark_session(master_url):
    """Create a Spark session."""
    return (
        SparkSession.builder
        .appName("Problem1_LogLevelDistribution")
        .master(master_url)
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

def main():
    # Determine Spark master
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        master_private_ip = os.getenv("MASTER_PRIVATE_IP")
        if not master_private_ip:
            print("Error: Master URL not provided.")
            print("Usage: python problem1.py spark://MASTER_IP:7077")
            sys.exit(1)
        master_url = f"spark://{master_private_ip}:7077"

    spark = create_spark_session(master_url)
    print(f"✅ Connected to Spark master at {master_url}")

    # Recursive pattern to read all container logs
    data_path = "s3a://zw459-assignment-spark-cluster-logs/data/**/*.log"
    output_dir = "data/output"
    os.makedirs(output_dir, exist_ok=True)

    # Read raw logs
    df = spark.read.text(data_path)
    total_lines = df.count()

    # Extract log levels
    logs = (
        df.withColumn(
            "log_level",
            regexp_extract(col("value"),
                           r"\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+(INFO|WARN|ERROR|DEBUG)",
                           1)
        )
        .filter(col("log_level") != "")
    )

    # Count log levels
    counts_df = logs.groupBy("log_level").agg(count("*").alias("count"))

    # Random sample of 10 log entries
    sample_df = logs.orderBy(rand()).limit(10).select("value", "log_level")

    # Force compute to trigger job execution
    print("Triggering computation for counts_df and sample_df...")
    counts_df.cache()
    sample_df.cache()
    counts_df.count()
    sample_df.count()
    print("✅ DataFrames materialized, proceeding to write...")

    # Write outputs to single CSV files
    write_single_csv(counts_df, "data/output", "problem1_counts.csv")
    write_single_csv(sample_df, "data/output", "problem1_sample.csv")

    # Prepare summary
    total_logs = logs.count()
    unique_levels = [r["log_level"] for r in counts_df.collect()]
    summary_text = (
        f"Total log lines processed: {total_lines}\n"
        f"Total lines with log levels: {total_logs}\n"
        f"Unique log levels found: {len(unique_levels)}\n\n"
        "Log level distribution:\n"
    )
    for row in counts_df.collect():
        pct = (row['count'] / total_logs * 100) if total_logs else 0
        summary_text += f"  {row['log_level']:<6}: {row['count']:>10,} ({pct:5.2f}%)\n"

    with open(f"{output_dir}/problem1_summary.txt", "w") as f:
        f.write(summary_text)

    print(summary_text)
    print("✅ Results written to data/output/")
    spark.stop()

if __name__ == "__main__":
    main()