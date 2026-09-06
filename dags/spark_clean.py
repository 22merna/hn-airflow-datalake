
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    when,
    lit,
    to_timestamp,
    row_number
)
from pyspark.sql.window import Window

import json
import os
import shutil


# ==========================================
# 1. Start Spark
# ==========================================

spark = (
    SparkSession.builder
    .appName("HackerNewsCleaning")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 2. Paths
# ==========================================

BASE_PATH = "/opt/airflow/data-lake"

RAW_STORIES_PATH = f"{BASE_PATH}/raw/stories"
RAW_COMMENTS_PATH = f"{BASE_PATH}/raw/comments"

PROCESSED_STORIES_PATH = f"{BASE_PATH}/processed/stories"
PROCESSED_COMMENTS_PATH = f"{BASE_PATH}/processed/comments"

TEMP_STORIES_PATH = f"{BASE_PATH}/processed/stories_temp"
TEMP_COMMENTS_PATH = f"{BASE_PATH}/processed/comments_temp"


# ==========================================
# 3. Check if real Parquet data exists
# ==========================================

def parquet_exists(path):

    if not os.path.isdir(path):
        return False

    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(".parquet"):
                return True

    return False


# ==========================================
# 4. Find latest raw batch
# ==========================================

def find_latest_batch(base_path):

    batches = []

    if not os.path.exists(base_path):
        return None

    for date_folder in os.listdir(base_path):

        if not date_folder.startswith("dt="):
            continue

        date_path = os.path.join(base_path, date_folder)

        if not os.path.isdir(date_path):
            continue

        for batch_folder in os.listdir(date_path):

            if not batch_folder.startswith("batch_"):
                continue

            batch_path = os.path.join(
                date_path,
                batch_folder
            )

            if not os.path.isdir(batch_path):
                continue

            batches.append(
                (
                    date_folder,
                    batch_folder,
                    batch_path
                )
            )

    if not batches:
        return None

    # Sort by date first, then batch number
    batches.sort(
        key=lambda x: (x[0], x[1])
    )

    return batches[-1][2]


# ==========================================
# 5. Get latest raw batches
# ==========================================

stories_path = find_latest_batch(
    RAW_STORIES_PATH
)

comments_path = find_latest_batch(
    RAW_COMMENTS_PATH
)


if stories_path is None:
    raise Exception(
        "No raw stories batch found."
    )

if comments_path is None:
    raise Exception(
        "No raw comments batch found."
    )


print("=" * 60)
print("LATEST RAW BATCH")
print("=" * 60)

print("Stories:")
print(stories_path)

print("Comments:")
print(comments_path)


# ==========================================
# 6. Read Stories JSON
# ==========================================

stories_file = os.path.join(
    stories_path,
    "stories.json"
)

with open(
    stories_file,
    "r",
    encoding="utf-8"
) as f:

    stories_data = json.load(f)

stories_df = spark.createDataFrame(
    stories_data
)


# ==========================================
# 7. Clean Stories
# ==========================================

stories_clean = (
    stories_df
    .filter(col("id").isNotNull())
    .filter(col("title").isNotNull())
    .withColumn(
        "title",
        when(
            col("title") == "",
            None
        ).otherwise(col("title"))
    )
    .withColumn(
        "url",
        when(
            col("url").isNull(),
            lit("")
        ).otherwise(col("url"))
    )
    .withColumn(
        "score",
        when(
            col("score").isNull(),
            lit(0)
        ).otherwise(col("score"))
    )
    .withColumn(
        "by",
        when(
            col("by").isNull(),
            lit("unknown")
        ).otherwise(col("by"))
    )
    .withColumn(
        "time",
        to_timestamp(col("time"))
    )
    .select(
        "id",
        "title",
        "url",
        "score",
        "by",
        "time",
        "descendants",
        "kids"
    )
)


# ==========================================
# 8. Read Comments JSON
# ==========================================

comments_file = os.path.join(
    comments_path,
    "comments.json"
)

with open(
    comments_file,
    "r",
    encoding="utf-8"
) as f:

    comments_data = json.load(f)

comments_df = spark.createDataFrame(
    comments_data
)


# ==========================================
# 9. Clean Comments
# ==========================================

comments_clean = (
    comments_df
    .filter(col("id").isNotNull())
    .filter(col("story_id").isNotNull())
    .filter(col("text").isNotNull())
    .withColumn(
        "by",
        when(
            col("by").isNull(),
            lit("unknown")
        ).otherwise(col("by"))
    )
    .withColumn(
        "time",
        to_timestamp(col("time"))
    )
    .select(
        "id",
        "story_id",
        "by",
        "text",
        "time"
    )
)


# ==========================================
# 10. Combine Existing + New Stories
# ==========================================

if parquet_exists(
    PROCESSED_STORIES_PATH
):

    print("=" * 60)
    print("READING EXISTING STORIES PARQUET")
    print("=" * 60)

    old_stories = spark.read.parquet(
        PROCESSED_STORIES_PATH
    )

    combined_stories = (
        old_stories
        .unionByName(stories_clean)
    )

else:

    combined_stories = stories_clean


# ==========================================
# 11. Deduplicate Stories
# ==========================================

story_window = (
    Window
    .partitionBy("id")
    .orderBy(
        col("time").desc_nulls_last()
    )
)

stories_deduplicated = (
    combined_stories
    .withColumn(
        "row_number",
        row_number().over(story_window)
    )
    .filter(
        col("row_number") == 1
    )
    .drop("row_number")
)


# ==========================================
# 12. Combine Existing + New Comments
# ==========================================

if parquet_exists(
    PROCESSED_COMMENTS_PATH
):

    print("=" * 60)
    print("READING EXISTING COMMENTS PARQUET")
    print("=" * 60)

    old_comments = spark.read.parquet(
        PROCESSED_COMMENTS_PATH
    )

    combined_comments = (
        old_comments
        .unionByName(comments_clean)
    )

else:

    combined_comments = comments_clean


# ==========================================
# 13. Deduplicate Comments
# ==========================================

comment_window = (
    Window
    .partitionBy("id")
    .orderBy(
        col("time").desc_nulls_last()
    )
)

comments_deduplicated = (
    combined_comments
    .withColumn(
        "row_number",
        row_number().over(comment_window)
    )
    .filter(
        col("row_number") == 1
    )
    .drop("row_number")
)


# ==========================================
# 14. Clean Temporary Paths
# ==========================================

if os.path.exists(
    TEMP_STORIES_PATH
):
    shutil.rmtree(
        TEMP_STORIES_PATH
    )

if os.path.exists(
    TEMP_COMMENTS_PATH
):
    shutil.rmtree(
        TEMP_COMMENTS_PATH
    )


# ==========================================
# 15. Write Stories to Temporary Path
# ==========================================

print("=" * 60)
print("WRITING STORIES TO TEMPORARY PATH")
print("=" * 60)

(
    stories_deduplicated
    .write
    .mode("overwrite")
    .parquet(
        TEMP_STORIES_PATH
    )
)


# ==========================================
# 16. Write Comments to Temporary Path
# ==========================================

print("=" * 60)
print("WRITING COMMENTS TO TEMPORARY PATH")
print("=" * 60)

(
    comments_deduplicated
    .write
    .mode("overwrite")
    .parquet(
        TEMP_COMMENTS_PATH
    )
)


# ==========================================
# 17. IMPORTANT:
# Read the newly written Parquet files
# ==========================================

# This breaks the lazy lineage from the old
# processed Parquet files.
#
# After this point, the result DataFrames
# depend on the temporary files, not the
# old processed files.

final_stories = spark.read.parquet(
    TEMP_STORIES_PATH
)

final_comments = spark.read.parquet(
    TEMP_COMMENTS_PATH
)


# ==========================================
# 18. Materialize + Verify Results
# ==========================================

stories_count = final_stories.count()

comments_count = final_comments.count()


print("=" * 60)
print("CLEANING + DEDUPLICATION COMPLETED")
print("=" * 60)

print(
    f"Stories count after deduplication: "
    f"{stories_count}"
)

print(
    f"Comments count after deduplication: "
    f"{comments_count}"
)


print("=" * 60)
print("SAMPLE STORIES")
print("=" * 60)

final_stories.show(
    5,
    truncate=False
)


print("=" * 60)
print("SAMPLE COMMENTS")
print("=" * 60)

final_comments.show(
    5,
    truncate=False
)


# ==========================================
# 19. Replace Stories
# ==========================================

print("=" * 60)
print("REPLACING STORIES PARQUET")
print("=" * 60)

if os.path.exists(
    PROCESSED_STORIES_PATH
):

    shutil.rmtree(
        PROCESSED_STORIES_PATH
    )

shutil.move(
    TEMP_STORIES_PATH,
    PROCESSED_STORIES_PATH
)


# ==========================================
# 20. Replace Comments
# ==========================================

print("=" * 60)
print("REPLACING COMMENTS PARQUET")
print("=" * 60)

if os.path.exists(
    PROCESSED_COMMENTS_PATH
):

    shutil.rmtree(
        PROCESSED_COMMENTS_PATH
    )

shutil.move(
    TEMP_COMMENTS_PATH,
    PROCESSED_COMMENTS_PATH
)


# ==========================================
# 21. Final Message
# ==========================================

print("=" * 60)
print("PIPELINE COMPLETED SUCCESSFULLY")
print("=" * 60)

print(
    f"Final Stories: {stories_count}"
)

print(
    f"Final Comments: {comments_count}"
)

print("=" * 60)


# ==========================================
# 22. Stop Spark
# ==========================================

spark.stop()

