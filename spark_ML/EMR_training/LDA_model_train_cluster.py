# This script is to train/tune the LDA model in a cluster. Use
# the following command to submit the job. You may want to modify the
# configuration and repartition the data for the performance optimization.
# spark-submit LDA_model_train_cluster.py --py-files sparkLDA.zip
# --master yarn --deploy-mode cluster > info_output.txt 2>&1 &
from pyspark.sql import SparkSession

from pyspark.ml.feature import StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA

# Import stemmer library
from os import path

# create a SparkSession
spark = SparkSession.builder.\
    config("spark.driver.cores", 4).\
    config("spark.driver.memory", "10g").\
    config("spark.executor.instances", 1).\
    config("spark.executor.memory", "10g").\
    config("spark.executor.cores", 4).\
    config("spark.sql.shuffle.partitions", 4).\
    appName("twitter LDA").getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")
sc.addPyFile("sparkLDA.zip")

print(spark.sparkContext.getConf().getAll())

from sparkLDA.config import n_topics, extra_for_stemmed, seedNum
from sparkLDA.utils import show_topics, evaluate
from sparkLDA.processing import preprocess_text

skip_process = True

pipelinePath = "twitter-data-collection/ML_models/LDA-pipeline-model_Nov20"

if not skip_process:
    s3_bucket = "s3://bigdata-chuangxin-week2/"
    #
    files_path_train = "twitter-data-collection/parquet/Oct_data"
    files_path_test = "twitter-data-collection/parquet/Nov_1_2"

    # use parquet for local test
    df_train = spark.read.format("parquet").load(files_path_train).repartition(4)
    df_test = spark.read.format("parquet").load(files_path_test).repartition(4)


    print("model will be save in \n", s3_bucket + pipelinePath)
    # ============================================
    # preprocessing
    # ============================================

    df_train = preprocess_text(df_train)
    df_test = preprocess_text(df_test)

    # repartition if necessary
    df_train = df_train.coalesce(4)
    df_test = df_test.coalesce(4)
else:
    #
    s3_bucket = "s3://bigdata-chuangxin-week2/"
    files_path_train = "twitter-data-collection/parquet/Oct_data_processed"
    files_path_test = "twitter-data-collection/parquet/Nov_1_2_processed"

    df_train = spark.read.format("parquet").load(s3_bucket + files_path_train).repartition(4)
    df_test = spark.read.format("parquet").load(s3_bucket + files_path_test).repartition(4)

    print("model will be save in \n", s3_bucket + pipelinePath)

# ============================================
# extra step to remove frequent words
# ============================================

# one extra step to remove the frequent words
stopword_remover_stem = StopWordsRemover(inputCol="stemmed", outputCol="stemmed_rm")
stopword_remover_stem.setStopWords(extra_for_stemmed)

df_train = stopword_remover_stem.transform(df_train)
df_test = stopword_remover_stem.transform(df_test)

# ============================================
# cache and print some basic information
# ============================================
df_train.cache()
df_test.cache()
print("Train/test data info:")
print(50*"=")
print("Load training data from: ")
print(files_path_train)
print("Load test data from: ")
print(files_path_test)
print(f"nums of training data: {df_train.count(): 10d}")
print(f"nums of test data: {df_test.count(): 10d}")
print(50*"=")

# ============================================
# build the pipeline
# ============================================

# 2.4. CountVectorizer
vectorizer = CountVectorizer(inputCol= "stemmed_rm", outputCol="rawFeatures")
# 2.5. IDf
idf = IDF(inputCol="rawFeatures", outputCol="features")

# 3. train the LDA model
n_topics_list = [5, 8, 10]
pipeline_model_list = []
for n_topics in n_topics_list:

    print("training LDA with n topics: ", n_topics)

    lda = LDA(k=n_topics, seed=seedNum, optimizer="em", maxIter=20)

    pipeline = Pipeline(stages=[vectorizer, idf, lda])

    pipeline_model = pipeline.fit(df_train)
    pipeline_model.write().overwrite().save(s3_bucket + pipelinePath + "_ntopics_" + str(n_topics))

    show_topics(pipeline_model)

    print("Model performance on training data:")
    evaluate(pipeline_model, df_train, isTrain=True)
    print("Model performance on test data:")
    evaluate(pipeline_model, df_test, isTrain=False, callogll=False, calppl=True)

    pipeline_model_list.append(pipeline_model)

spark.stop()
