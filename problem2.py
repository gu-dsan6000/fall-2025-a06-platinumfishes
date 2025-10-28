#!/usr/bin/env python3
"""
Problem 2: Cluster Usage Analysis
---------------------------------
Generates:
  data/output/problem2_timeline.csv
  data/output/problem2_cluster_summary.csv
  data/output/problem2_stats.txt
  data/output/problem2_bar_chart.png
  data/output/problem2_density_plot.png

Usage:
  # Full Spark processing (10–20 min)
  python problem2.py spark://MASTER_IP:7077 --net-id YOUR-NET-ID

  # Re-generate visualizations/statistics from existing CSVs
  python problem2.py --skip-spark
"""

import os
import sys
import argparse
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    regexp_extract, col, to_timestamp, input_file_name,
    min as spark_min, max as spark_max
)

# ----------------------------------------
# Helpers
# ----------------------------------------
def write_single_csv(sdf, out_path):
    """Write small Spark DF to a single CSV via pandas."""
    pdf = sdf.toPandas()
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    pdf.to_csv(out_path, index=False)

def create_spark(master_url):
    return (
        SparkSession.builder
        .appName("Problem2_ClusterUsage")
        .master(master_url)
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

def make_plots(timeline_csv, cluster_csv, out_dir):
    os.makedirs(out_dir, exist_ok=True)
    tl = pd.read_csv(timeline_csv, parse_dates=["start_time", "end_time"])
    cs = pd.read_csv(cluster_csv, parse_dates=["cluster_first_app", "cluster_last_app"])

    # --- Bar chart: applications per cluster ---
    plt.figure(figsize=(10, 5))
    bar = sns.barplot(
        data=cs.sort_values("num_applications", ascending=False),
        x="cluster_id", y="num_applications", hue="cluster_id", dodge=False
    )
    # Value labels
    for p in bar.patches:
        bar.annotate(f"{int(p.get_height())}",
                     (p.get_x() + p.get_width()/2, p.get_height()),
                     ha="center", va="bottom", fontsize=9, rotation=0, xytext=(0, 3),
                     textcoords="offset points")
    plt.xlabel("Cluster ID", fontweight="bold")
    plt.ylabel("Number of Applications", fontweight="bold")
    plt.title("Applications per Cluster")
    plt.legend(title="Cluster", loc="best", fontsize=8)
    plt.tight_layout()
    bar_path = os.path.join(out_dir, "problem2_bar_chart.png")
    plt.savefig(bar_path, dpi=160)
    plt.close()

    # --- Density plot: job duration distribution for the largest cluster ---
    tl = tl.copy()
    tl["duration_minutes"] = (tl["end_time"] - tl["start_time"]).dt.total_seconds() / 60.0

    largest_cluster = cs.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"]
    largest_df = tl[tl["cluster_id"] == largest_cluster].copy()

    plt.figure(figsize=(10, 5))
    ax = sns.histplot(largest_df["duration_minutes"], bins=40, kde=True)
    ax.set_xscale("log")  # log scale for skew
    plt.xlabel("Duration (minutes, log scale)", fontweight="bold")
    plt.ylabel("Count", fontweight="bold")
    plt.title(f"Job Duration Distribution (Cluster {largest_cluster}) — n={len(largest_df)}")
    plt.tight_layout()
    density_path = os.path.join(out_dir, "problem2_density_plot.png")
    plt.savefig(density_path, dpi=160)
    plt.close()

    return bar_path, density_path

# ----------------------------------------
# Main
# ----------------------------------------
def run_spark_pipeline(master_url, net_id):
    spark = create_spark(master_url)

    # Input path: all container logs in your bucket
    data_glob = f"s3a://{net_id}-assignment-spark-cluster-logs/data/**/*.log"
    outdir = "data/output"
    os.makedirs(outdir, exist_ok=True)

    # Read all logs
    logs = spark.read.text(data_glob)

    # From file path extract application_id and cluster_id and app_number
    # Example path component: .../application_1485248649253_0001/container_... .log
    logs = logs.withColumn("file_path", input_file_name()
    ).withColumn(
        "application_id", regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1)
    ).withColumn(
        "cluster_id", regexp_extract(col("application_id"), r"application_(\d+)_\d+", 1)
    ).withColumn(
        "app_number", regexp_extract(col("application_id"), r"application_\d+_(\d+)", 1)
    )

    # Parse timestamp at beginning of each line: yy/MM/dd HH:mm:ss ...
    # e.g., "17/03/29 10:04:41 INFO ..."  -> to_timestamp(..., 'yy/MM/dd HH:mm:ss')
    logs = logs.withColumn(
        "ts_str", regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1)
    ).withColumn(
        "timestamp", to_timestamp(col("ts_str"), "yy/MM/dd HH:mm:ss")
    )

    # Keep only rows that could be timestamped & have an application id
    logs = logs.filter((col("application_id") != "") & col("timestamp").isNotNull())

    # Timeline per application: min/max timestamp
    timeline = (
        logs.groupBy("cluster_id", "application_id", "app_number")
            .agg(
                spark_min("timestamp").alias("start_time"),
                spark_max("timestamp").alias("end_time")
            )
    )

    # Cluster summary
    cluster_summary = (
        timeline.groupBy("cluster_id")
                .agg(
                    spark_min("start_time").alias("cluster_first_app"),
                    spark_max("end_time").alias("cluster_last_app"),
                )
    )
    # Add counts by joining counts
    counts = timeline.groupBy("cluster_id").count().withColumnRenamed("count", "num_applications")
    cluster_summary = cluster_summary.join(counts, on="cluster_id", how="inner") \
                                     .select("cluster_id", "num_applications",
                                             "cluster_first_app", "cluster_last_app") \
                                     .orderBy(col("num_applications").desc())

    # Write single CSVs
    timeline_csv = os.path.join(outdir, "problem2_timeline.csv")
    cluster_csv = os.path.join(outdir, "problem2_cluster_summary.csv")
    write_single_csv(timeline, timeline_csv)
    write_single_csv(cluster_summary, cluster_csv)

    # Stats file
    n_clusters = cluster_summary.count()
    n_apps = timeline.count()
    avg_apps = n_apps / n_clusters if n_clusters else 0.0

    top_lines = []
    for row in cluster_summary.limit(5).collect():
        top_lines.append(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")

    stats = (
        f"Total unique clusters: {n_clusters}\n"
        f"Total applications: {n_apps}\n"
        f"Average applications per cluster: {avg_apps:.2f}\n\n"
        f"Most heavily used clusters:\n" + "\n".join(top_lines) + "\n"
    )
    with open(os.path.join(outdir, "problem2_stats.txt"), "w") as f:
        f.write(stats)

    # Plots
    bar_path, density_path = make_plots(timeline_csv, cluster_csv, outdir)

    spark.stop()
    return timeline_csv, cluster_csv, os.path.join(outdir, "problem2_stats.txt"), bar_path, density_path

def regen_only():
    """Re-generate plots and stats from existing CSVs."""
    outdir = "data/output"
    timeline_csv = os.path.join(outdir, "problem2_timeline.csv")
    cluster_csv = os.path.join(outdir, "problem2_cluster_summary.csv")

    if not (os.path.exists(timeline_csv) and os.path.exists(cluster_csv)):
        print("Missing CSVs. Run full Spark pipeline first.")
        sys.exit(1)

    # Simple stats from existing files
    tl = pd.read_csv(timeline_csv, parse_dates=["start_time", "end_time"])
    cs = pd.read_csv(cluster_csv, parse_dates=["cluster_first_app", "cluster_last_app"])
    n_clusters = cs.shape[0]
    n_apps = tl.shape[0]
    avg_apps = n_apps / n_clusters if n_clusters else 0.0

    top = cs.sort_values("num_applications", ascending=False).head(5)
    lines = [f"  Cluster {r.cluster_id}: {int(r.num_applications)} applications" for _, r in top.iterrows()]
    stats = (
        f"Total unique clusters: {n_clusters}\n"
        f"Total applications: {n_apps}\n"
        f"Average applications per cluster: {avg_apps:.2f}\n\n"
        f"Most heavily used clusters:\n" + "\n".join(lines) + "\n"
    )
    with open(os.path.join(outdir, "problem2_stats.txt"), "w") as f:
        f.write(stats)

    bar_path, density_path = make_plots(timeline_csv, cluster_csv, outdir)
    return timeline_csv, cluster_csv, os.path.join(outdir, "problem2_stats.txt"), bar_path, density_path

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("master", nargs="?", help="spark://MASTER_IP:7077")
    p.add_argument("--net-id", help="Your net id (for bucket prefix)", default=None)
    p.add_argument("--skip-spark", action="store_true", help="Regenerate outputs from existing CSVs")
    return p.parse_args()

if __name__ == "__main__":
    args = parse_args()

    if args.skip_spark:
        paths = regen_only()
    else:
        if args.master:
            master_url = args.master
        else:
            mip = os.getenv("MASTER_PRIVATE_IP")
            if not mip:
                print("Provide master URL or set MASTER_PRIVATE_IP.")
                sys.exit(1)
            master_url = f"spark://{mip}:7077"

        net_id = args.net_id
        if not net_id:
            print("--net-id is required for S3 path (e.g., zw459).")
            sys.exit(1)

        paths = run_spark_pipeline(master_url, net_id)

    print("\nArtifacts written:")
    for p in paths:
        print(f"  - {p}")
