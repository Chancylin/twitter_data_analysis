import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import PipelineModel

# Import stemmer library
from nltk.stem.porter import PorterStemmer

from config import *

if __name__ == "__main__":
    """Usage:
    nc -lk 9999
    spark-submit this_file.py localhost 9999

    """
    
    # create a SparkSession
    spark = SparkSession.builder.master("local").appName("streaming local test").getOrCreate()


    inputPath = "/Users/lcx/Documents/weclouddata/my_project/spark_streaming/input_files"

    df = (spark
        .readStream
        .format("csv")
        .options(header=False, sep="\t", enforceSchema=True)
        .schema(file_schema)
        .load(inputPath))

    # columns of interest
    cols_select = ['tweet_text', 'hash_tag', 'created_at', 'retweet_count', 'favorite_count']

    df_select = df.dropna(subset=["tweet_text"]).select(cols_select)

    # 2. text processing

    # training data
    df_select_clean = (df_select.withColumn("tweet_text", F.regexp_replace("tweet_text", r"[@#&][A-Za-z0-9_-]+", " "))
                       .withColumn("tweet_text", F.regexp_replace("tweet_text", r"\w+:\/\/\S+", " "))
                       .withColumn("tweet_text", F.regexp_replace("tweet_text", r"[^A-Za-z]", " "))
                       .withColumn("tweet_text", F.regexp_replace("tweet_text", r"\s+", " "))
                       .withColumn("tweet_text", F.lower(F.col("tweet_text")))
                       .withColumn("tweet_text", F.trim(F.col("tweet_text")))
                      )

    #============================================
    # preprocessing
    #============================================
    # 2.1. tokenize
    tokenizer = Tokenizer(inputCol="tweet_text", outputCol="tokens")

    # 2.2. remove stopwords
    stopword_remover = StopWordsRemover(inputCol="tokens", outputCol="remove_stop")

    stopwords_list = stopword_remover.getStopWords()
    stopwords_list = stopwords_list + more_stopwords
    stopword_remover.setStopWords(stopwords_list)
    #2.3. stemming
    # TODO: how to modify the stemming function into a transformer?
    stemmer = PorterStemmer()
    # more straightforward to use lambda
    stem_udf = F.udf(lambda l : [stemmer.stem(word) for word in l], returnType = ArrayType(StringType()))


    df_tokenized = tokenizer.transform(df_select_clean)
    df_rmstop = stopword_remover.transform(df_tokenized)
    df_stemmed = df_rmstop.withColumn("stemmed", stem_udf(F.col("remove_stop")))


    # Load the trained LDAmodel
    savedPipelineModel = PipelineModel.load(pipelinePath)

    df_with_topics = savedPipelineModel.transform(df_stemmed).select("tweet_text", "topicDistribution")

    to_array = F.udf(lambda v: v.toArray().tolist(), ArrayType(FloatType()))
    df_with_topics_toArray = df_with_topics.select("tweet_text", to_array("topicDistribution").alias("topicDistributionArray"))

    df_with_topics_final = df_with_topics_toArray.select(["tweet_text"] + 
                                                     [(F.col("topicDistributionArray")[i]).alias("topic_"+str(i)) for i in range(10)])

    df_topic_info = df_with_topics_final.agg(*[F.sum(F.col("topic_"+str(i))).alias("topic_"+str(i)+ "_count") for i in range(n_topics)])

    df_topic_info = df_topic_info.selectExpr("stack(10, 'topic_0', topic_0_count, \
                               'topic_1', topic_1_count, \
                               'topic_2', topic_2_count, \
                               'topic_3', topic_3_count, \
                               'topic_4', topic_4_count, \
                               'topic_5', topic_5_count, \
                               'topic_6', topic_6_count, \
                               'topic_7', topic_7_count, \
                               'topic_8', topic_8_count, \
                               'topic_9', topic_9_count)").withColumnRenamed("col0","topic").withColumnRenamed("col1","count").show()

    query = (df_topic_info
        .writeStream
        .outputMode("complete")
        .format("console")
        .trigger(processingTime="5 second")
        .start())

    query.awaitTermination()


