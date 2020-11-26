from pyspark.sql.types import *

n_topics = 10

pipelinePath = "/Users/lcx/Documents/weclouddata/my_project/spark_ML/models/LDA-pipeline-model_test"

stopword_list = "/Users/lcx/Documents/weclouddata/my_project/spark_ML/stopwords_list.json"

file_schema = StructType([StructField("tweet_text", StringType(), True), 
                          # StructField("hash_tag", ArrayType(StringType(), True), True), 
                          StructField("hash_tag", StringType(), True), 
                          StructField("created_at", StringType(), True), 
                          StructField("retweet_count", IntegerType(), True), 
                          StructField("favorite_count", IntegerType(), True), 
                          StructField("retweeted", BooleanType(), True), 
                          StructField("truncated", BooleanType(), True), 
                          StructField("id", StringType(), True), 
                          StructField("user_name", StringType(), True), 
                          StructField("screen_name", StringType(), True), 
                          StructField("followers_count", IntegerType(), True), 
                          StructField("location", StringType(), True), 
                          StructField("geo", StringType(), True),
                          StructField("invalid", StringType(), True)])


more_stopwords = ["also", "via", "cc", "rt", "must", "always", 
"learn", "ai", "data", "machin", "use", "intellig", "new", "artifici", "technolog", 
'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

# [|[learn, use, machin, data, ai, shape, friend, answer, intellig, part]             |
# |[technolog, advanc, time, creat, leverag, user, approach, revolution, mine, learn]|
# |[learn, ai, data, use, machin, intellig, predict, join, artifici, new]            |
# |[learn, ai, data, use, machin, intellig, artifici, new, help, latest]             |
# |[learn, ai, use, data, machin, digit, intellig, new, artifici, h]                 |
# |[learn, ai, data, machin, use, intellig, artifici, read, new, industri]           |
# |[learn, ai, like, use, data, machin, intellig, new, free, artifici]               |
# |[learn, ai, data, use, machin, intellig, artifici, new, scienc, get]              |
# |[learn, ai, data, use, machin, intellig, top, websit, challeng, artifici]         |
# |[learn, ai, data, machin, use, intellig, new, artifici, technolog, p]  ]