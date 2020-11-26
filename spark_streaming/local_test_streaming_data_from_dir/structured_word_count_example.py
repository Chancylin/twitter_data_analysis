import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    """Usage:
    nc -lk 9999
    spark-submit this_file.py localhost 9999

    """
    if len(sys.argv) != 3:
        print("Usage: spark-submit structured_word_count_example.py <hostname> <port>", file=sys.stderr)
        sys.exit(-1)
    
    # create a SparkSession
    spark = SparkSession.builder.master("local").appName("streaming local test").getOrCreate()

    # Create DataFrame representing the stream of input lines from connection to localhost:9999
    lines = spark \
        .readStream \
        .format("socket") \
        .option("host", sys.argv[1]) \
        .option("port", int(sys.argv[2])) \
        .load()

    # Split the lines into words
    words = lines.select(
       F.explode(
           F.split(lines.value, " ")
       ).alias("word")
    )

    # Generate running word count
    wordCounts = words.groupBy("word").count()


     # Start running the query that prints the running counts to the console
    query = wordCounts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .trigger(processingTime="5 second") \
        .start()

    query.awaitTermination()