# to submit in terminal
# spark-submit LDA_model_train.py 

from pyspark.sql import SparkSession

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA

# Import stemmer library
from nltk.stem.porter import PorterStemmer

# create a SparkSession
spark = SparkSession.builder.master("local").appName("twitter ML").getOrCreate()

# set seed
seedNum = 1234

# set parameters
more_stopwords = ["also", "via", "cc", "rt", "must", "always"]
n_topics = 10

#
pipelinePath = "/Users/lcx/Documents/weclouddata/my_project/spark_ML/models/LDA-pipeline-model_test"

# 
files_path_train = "/Users/lcx/Documents/weclouddata/my_project/twitter_data/sample_data/2020_10_30/*/*"

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

# 
def termsIdx2Term(vocabulary):
    def termsIdx2Term(termIndices):
        return [vocabulary[int(index)] for index in termIndices]
    return F.udf(termsIdx2Term, ArrayType(StringType()))


# 1. load file
# how to prepare the train and test dataset
df = (spark
    .read
  .format("csv")
  .options(header=False, sep="\t", enforceSchema=True)
  .schema(file_schema)
  .load(files_path_train))

# df_test = (spark
#     .read
#   .format("csv")
#   .options(header=False, sep="\t", enforceSchema=True)
#   .schema(file_schema)
#   .load(files_path_train))

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


df_select_clean.cache()
print("total tweets: ", df_select_clean.count())

# # test data
# df_select_clean = (df_select.withColumn("tweet_text", F.regexp_replace("tweet_text", r"[@#&][A-Za-z0-9_-]+", " "))
#                    .withColumn("tweet_text", F.regexp_replace("tweet_text", r"\w+:\/\/\S+", " "))
#                    .withColumn("tweet_text", F.regexp_replace("tweet_text", r"[^A-Za-z]", " "))
#                    .withColumn("tweet_text", F.regexp_replace("tweet_text", r"\s+", " "))
#                    .withColumn("tweet_text", F.lower(F.col("tweet_text")))
#                    .withColumn("tweet_text", F.trim(F.col("tweet_text")))
#                   )

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

df_stemmed.cache()
df_select_clean.unpersist()
df_train = df_stemmed


#============================================
# build the pipeline
#============================================

# 2.4. CountVectorizer
vectorizer = CountVectorizer(inputCol= "stemmed", outputCol="rawFeatures")
# 2.5. IDf
idf = IDF(inputCol="rawFeatures", outputCol="features")



# 3. train the LDA model
lda = LDA(k=10, seed=seedNum, optimizer="em")

pipeline = Pipeline(stages=[vectorizer, idf, lda])


pipeline_model = pipeline.fit(df_train)

pipeline_model.write().overwrite().save(pipelinePath)


# get the LDA model and other useful part
lda_model = pipeline_model.stages[-1]


vectorizer_model = pipeline_model.stages[0]
vocabList = vectorizer_model.vocabulary

idf_model = pipeline_model.stages[1]

# show the topics and the associated words
topics_describe = lda_model.describeTopics()

final = topics_describe.withColumn("Terms", termsIdx2Term(vocabList)("termIndices"))

final.select("Terms").show(truncate=False)

# 4. evaluate
logll_train = lda_model.trainingLogLikelihood()
logppl_train = lda_model.logPerplexity(idf_model.transform(vectorizer_model.transform(df_train)))
print("Model performance on training data:")
print(f"Log Likelihood: {logll_train: .4f}")
print(f"Log Perplexity: {logppl_train: .4f}")

# logppl_test = lda_model.logPerplexity(idf_model.transform(vectorizer_model.transform(df_test)))
# logll_test = lda_model.logLikelihood(idf_model.transform(vectorizer_model.transform(df_test)))
# print("\n")
# print("Model performance on test data:")
# print(f"LogLikelihood: {logll_test: .4f}")
# print(f"Log Perplexity: {logppl_test: .4f}")






