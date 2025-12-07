#!/usr/bin/env python3
"""
spark_filter_replace.py

Usage:
  spark-submit \
    --master spark://vm1:7077 \
    --conf spark.sql.shuffle.partitions=<num_tasks> \
    spark_filter_replace.py \
    <input_dir> <output_dir> "<oldword newword>" <log_csv_path> <num_tasks>

- input_dir: directory Spark will watch for new text files (each file is a batch)
- output_dir: directory to write replaced lines (text files)
- "<oldword newword>": single quoted argument containing the old and new words separated by space/comma/colon/semicolon
- log_csv_path: path where throughput log CSV will be appended
- num_tasks: desired logical parallelism (set to 3 for MP4 tests)

Example:
  spark-submit --master spark://vm1:7077 \
    --conf spark.sql.shuffle.partitions=3 \
    spark_filter_replace.py /home/anantg2/input /home/anantg2/out "hello world" /home/anantg2/replace_log.csv 3
"""
import sys, os, time, re
from pyspark.sql import SparkSession
from pyspark.sql.functions import lower, regexp_replace, col
from pyspark.sql import DataFrame

if len(sys.argv) != 6:
    print("Usage: spark_filter_replace.py <input_dir> <output_dir> \"<old new>\" <log_csv_path> <num_tasks>")
    sys.exit(1)

input_dir = sys.argv[1]
output_dir = sys.argv[2]
arg = sys.argv[3].strip()
log_csv = sys.argv[4]
num_tasks = int(sys.argv[5])

# Parse the single argument into old and new words (split on space, comma, colon, semicolon)
parts = re.split(r"[ ,:;]+", arg)
if len(parts) != 2:
    print("Error: expected exactly two tokens in the argument: \"old new\"")
    sys.exit(1)
old_word, new_word = parts[0], parts[1]

# regex for case-insensitive replacement in Spark (use (?i) prefix)
pattern_regex = "(?i)" + re.escape(old_word)

spark = SparkSession.builder.appName("FilterReplace").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

# enforce shuffle partitions to match logical parallelism if not supplied via spark-submit
spark.conf.set("spark.sql.shuffle.partitions", str(num_tasks))

# streaming read of text files (column 'value')
lines = spark.readStream.format("text").load(input_dir)

# filter (case-insensitive)
filtered = lines.filter(lower(col("value")).contains(old_word.lower()))

# We'll use foreachBatch to:
#  - compute batch count and write throughput to CSV
#  - perform the replace using regexp_replace and write replaced lines to output (append)
start_time = time.time()
total_count = 0

def process_batch(df: DataFrame, epoch_id):
    global total_count, start_time

    batch_count = df.count()  # triggers computation for this micro-batch
    total_count += batch_count
    elapsed = time.time() - start_time
    throughput = total_count / elapsed if elapsed > 0 else 0.0

    # log throughput line to CSV (timestamp, epoch_id, batch_count, cumulative, elapsed, throughput)
    now = time.time()
    line = f"{int(now)},{epoch_id},{batch_count},{total_count},{elapsed:.3f},{throughput:.3f}\n"
    # ensure directory exists
    os.makedirs(os.path.dirname(log_csv), exist_ok=True)
    with open(log_csv, "a") as f:
        f.write(line)

    if batch_count == 0:
        return

    # perform replace and write output as text files (append mode)
    replaced = df.withColumn("value", regexp_replace(col("value"), pattern_regex, new_word))
    # write as text: DataFrameWriter.text expects column named "value"
    out_subdir = os.path.join(output_dir, f"batch_{int(time.time())}")
    replaced.write.mode("append").text(out_subdir)


# start streaming query
query = filtered.writeStream.foreachBatch(process_batch).start()
print("Started Filter+Replace streaming. Press Ctrl+C to stop.")
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("Stopping...")
    query.stop()
    spark.stop()