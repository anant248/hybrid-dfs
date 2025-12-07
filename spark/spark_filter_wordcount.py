#!/usr/bin/env python3
"""
spark_filter_wordcount.py

Usage:
  spark-submit --master spark://vm1:7077 \
    --conf spark.sql.shuffle.partitions=<num_tasks> \
    spark_filter_wordcount.py \
    <input_dir> <output_dir> "<pattern>" <log_csv_path> <num_tasks>

- input_dir: directory Spark will watch for new text files
- output_dir: directory to write word-count CSVs (per-batch)
- "<pattern>": filter pattern (case-insensitive)
- log_csv_path: path to append throughput log CSV
- num_tasks: logical parallelism (set to 3 for MP4)

Example:
  spark-submit --master spark://vm1:7077 \
    --conf spark.sql.shuffle.partitions=3 \
    spark_filter_wordcount.py /home/anantg2/input /home/anantg2/out "stop" /home/anantg2/wc_log.csv 3
"""
import sys, os, time, re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, explode, split
from pyspark.sql import DataFrame

if len(sys.argv) != 6:
    print("Usage: spark_filter_wordcount.py <input_dir> <output_dir> \"<pattern>\" <log_csv_path> <num_tasks>")
    sys.exit(1)

input_dir = sys.argv[1]
output_dir = sys.argv[2]
pattern_arg = sys.argv[3].strip()
log_csv = sys.argv[4]
num_tasks = int(sys.argv[5])

spark = SparkSession.builder.appName("FilterWordCount").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# control shuffle partitions to match tasks
spark.conf.set("spark.sql.shuffle.partitions", str(num_tasks))

lines = spark.readStream.format("text").load(input_dir)

# filter by pattern (case-insensitive)
pattern_lower = pattern_arg.lower()
filtered = lines.filter(lower(col("value")).contains(pattern_lower))

start_time = time.time()
total_count = 0

def process_batch(df: DataFrame, epoch_id):
    global total_count, start_time

    batch_count = df.count()
    total_count += batch_count
    elapsed = time.time() - start_time
    throughput = total_count / elapsed if elapsed > 0 else 0.0

    now = time.time()
    line = f"{int(now)},{epoch_id},{batch_count},{total_count},{elapsed:.3f},{throughput:.3f}\n"
    os.makedirs(os.path.dirname(log_csv), exist_ok=True)
    with open(log_csv, "a") as f:
        f.write(line)

    if batch_count == 0:
        return

    # tokenise and count words in this batch
    words = df.select(explode(split(lower(col("value")), "[^A-Za-z]+")).alias("word")) \
              .filter(col("word") != "")

    counts = words.groupBy("word").count()

    # write per-batch counts to CSV files
    out_subdir = os.path.join(output_dir, f"batch_{int(time.time())}")
    counts.write.mode("append").csv(out_subdir, header=True)

# start streaming query
query = filtered.writeStream.foreachBatch(process_batch).start()
print("Started Filter+WordCount streaming. Press Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping...")
    query.stop()
    spark.stop()