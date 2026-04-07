# spark_transformer.py
# Run locally with: python3 spark_transformer.py
# Run on Databricks: paste into a notebook cell, change the read to spark.read.table()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os

def get_spark():
    """
    Local mode: runs Spark on your laptop, no cluster needed.
    On Databricks: spark is already available, skip this function.
    """
    return (
        SparkSession.builder
        .appName("github_etl_transforms")
        .config("spark.driver.memory", "2g")
        # JDBC driver to read from your Postgres/SQLite
        .config("spark.jars.packages", "org.xerial:sqlite-jdbc:3.43.0.0")
        .master("local[*]")   # use all local CPU cores
        .getOrCreate()
    )

def run_transforms():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")  # suppress noisy INFO logs

    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "etl_data.db")

    # --- Read from SQLite into a Spark DataFrame ---
    raw = (
        spark.read
        .format("jdbc")
        .option("url", f"jdbc:sqlite:{db_path}")
        .option("dbtable", "github_repos")
        .option("driver", "org.sqlite.JDBC")
        .load()
    )

    print(f"Loaded {raw.count()} rows into Spark")
    raw.printSchema()

    # ── Transform 1: clean and enrich ──────────────────────────────
    enriched = (
        raw
        .withColumn("language", F.coalesce(F.col("language"), F.lit("Unknown")))
        .withColumn("fork_ratio",
            F.round(F.col("forks_count") / F.col("stargazers_count"), 3))
        .withColumn("star_tier", F.when(F.col("stargazers_count") >= 100_000, "100k+")
                                   .when(F.col("stargazers_count") >= 50_000,  "50k-100k")
                                   .when(F.col("stargazers_count") >= 10_000,  "10k-50k")
                                   .otherwise("under 10k"))
        .withColumn("updated_at", F.to_timestamp("updated_at"))
        .withColumn("created_at", F.to_timestamp("created_at"))
    )

    # ── Transform 2: language-level aggregations ───────────────────
    lang_stats = (
        enriched
        .groupBy("language")
        .agg(
            F.count("*").alias("repo_count"),
            F.round(F.avg("stargazers_count")).alias("avg_stars"),
            F.max("stargazers_count").alias("max_stars"),
            F.round(F.avg("fork_ratio"), 3).alias("avg_fork_ratio"),
        )
        .orderBy(F.desc("repo_count"))
    )

    # ── Transform 3: window function — rank repos within language ──
    window = Window.partitionBy("language").orderBy(F.desc("stargazers_count"))

    ranked = (
        enriched
        .withColumn("rank_in_language", F.rank().over(window))
        .filter(F.col("rank_in_language") <= 3)   # top 3 per language
        .select("language", "name", "stargazers_count", "rank_in_language")
        .orderBy("language", "rank_in_language")
    )

    # ── Show results ───────────────────────────────────────────────
    print("\n--- Language stats ---")
    lang_stats.show(20, truncate=False)

    print("\n--- Top 3 repos per language ---")
    ranked.show(40, truncate=False)

    # ── Write transformed tables back (as Parquet for speed) ───────
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "spark_output")
    lang_stats.write.mode("overwrite").parquet(f"{output_dir}/lang_stats")
    ranked.write.mode("overwrite").parquet(f"{output_dir}/top_repos_by_language")
    print(f"\nWrote Parquet output to: {output_dir}/")

    spark.stop()

if __name__ == "__main__":
    run_transforms()