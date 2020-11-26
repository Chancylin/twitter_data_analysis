# This script is to test the LDA model training script quickly in local machine.
# To submit in terminal
# spark-submit LDA_model_train.py --py-files ../sparkLDA/dist/sparkLDA-0.1
from pyspark.sql import SparkSession

from pyspark.ml.feature import StopWordsRemover, CountVectorizer, IDF
from pyspark.ml import Pipeline
from pyspark.ml.clustering import LDA

# Import stemmer library
from os import path

from sparkLDA.config import n_topics, extra_for_stemmed, seedNum
from sparkLDA.utils import show_topics, evaluate
from sparkLDA.processing import preprocess_text

if __name__ == "__main__":
    # create a SparkSession
    spark = SparkSession.builder.master("local").appName("twitter ML").getOrCreate()

    s3_bucket = "s3://bigdata-chuangxin-week2/"
    #
    pipelinePath = "twitter-data-collection/ML_models/LDA-pipeline-model_test_Nov19"
    files_path_train = "twitter-data-collection/2020_10_30_parquet"
    files_path_test = "twitter-data-collection/2020_10_30_subsample_parquet"

    # local only
    # pipelinePath = "/Users/lcx/Documents/weclouddata/my_project/spark_ML/models/LDA-pipeline-model_test_Nov19"
    # files_path_train = "/Users/lcx/Documents/weclouddata/my_project/twitter_data/sample_data/2020_10_30_parquet"
    # files_path_test = "/Users/lcx/Documents/weclouddata/my_project/twitter_data/" \
    #                   "sample_data/2020_10_30_subsample_parquet"

    #
    # files_path_train = "/Users/lcx/Documents/weclouddata/my_project/twitter_data/sample_data/2020_10_30/*/*"
    # files_path_test = "/Users/lcx/Documents/weclouddata/my_project/twitter_data/sample_data/2020_10_30/*/*"
    # # 1. load file
    # # how to prepare the train and test dataset
    # df_train = (spark
    #             .read
    #             .format("csv")
    #             .options(header=False, sep="\t", enforceSchema=True)
    #             .schema(file_schema)
    #             .load(files_path_train))
    #
    # df_test = (spark
    #            .read
    #            .format("csv")
    #            .options(header=False, sep="\t", enforceSchema=True)
    #            .schema(file_schema)
    #            .load(files_path_test))

    # use parquet for local test

    df_train = spark.read.format("parquet").load(files_path_train)
    df_test = spark.read.format("parquet").load(files_path_test)

    # make sure the directory to save the model doesnot exist
    # if path.exists(pipelinePath):
    #     print(f"directory to save pipeline model already exists: {pipelinePath}")
    #     raise Exception()
    print("model will be save in \n", s3_bucket + pipelinePath)
    # ============================================
    # preprocessing
    # ============================================

    df_train = preprocess_text(df_train)
    df_test = preprocess_text(df_test)

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

    # one extra step to remove the frequent words
    stopword_remover_stem = StopWordsRemover(inputCol="stemmed", outputCol="stemmed_rm")
    stopword_remover_stem.setStopWords(extra_for_stemmed)

    df_train = stopword_remover_stem.transform(df_train)
    df_test = stopword_remover_stem.transform(df_test)
    # 2.4. CountVectorizer
    vectorizer = CountVectorizer(inputCol= "stemmed_rm", outputCol="rawFeatures")
    # 2.5. IDf
    idf = IDF(inputCol="rawFeatures", outputCol="features")

    # 3. train the LDA model
    lda = LDA(k=n_topics, seed=seedNum, optimizer="em", maxIter=5)

    pipeline = Pipeline(stages=[vectorizer, idf, lda])

    pipeline_model = pipeline.fit(df_train)
    pipeline_model.write().overwrite().save(pipelinePath)

    show_topics(pipeline_model)

    print("Model performance on training data:")
    evaluate(pipeline_model, df_train, isTrain=True)
    print("Model performance on test data:")
    evaluate(pipeline_model, df_test, isTrain=False)
