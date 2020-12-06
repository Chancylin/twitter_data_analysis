import pyspark.sql.functions as F
from pyspark.sql.types import *


def termsIdx2Term(vocabulary):
    """Convert word index to word"""

    def termsIdx2Term(termIndices):
        return [vocabulary[int(index)] for index in termIndices]

    return F.udf(termsIdx2Term, ArrayType(StringType()))

def show_topics(pipeline_model, topics_zero_offset=False):
    """Shows the model topics and associated words"""
    # get the LDA model and other useful part
    lda_model = pipeline_model.stages[-1]

    vectorizer_model = pipeline_model.stages[0]
    vocabList = vectorizer_model.vocabulary

    # show the topics and the associated words
    topics_describe = lda_model.describeTopics()

    final = topics_describe.withColumn("Terms", termsIdx2Term(vocabList)("termIndices"))\
            .withColumn("topic", F.col("topic") + 1)

    final.select("topic", "Terms").show(truncate=False)

def evaluate(pipeline_model, df, isTrain = True, callogll = False, calppl = False):
    """evalute the LDA model performance based on log Likelihood and log Perplexity"""
    lda_model = pipeline_model.stages[-1]
    vectorizer_model = pipeline_model.stages[0]
    idf_model = pipeline_model.stages[1]
    if (not isTrain) and (not calppl) and (not calppl):
        print("do nothing")

    if isTrain:
        logll = lda_model.trainingLogLikelihood()
        print(f"Log Likelihood: {logll: .4f}")
    elif callogll:
        print(f"calculating log likelihood...")
        logll = lda_model.logLikelihood(idf_model.transform(vectorizer_model.transform(df)))
        print(f"Log Likelihood: {logll: .4f}")
    else:
        print(f"Skip the calculation of log Likelihood on test set to save time")

    if calppl:
        print(f"calculating log perplexity...")
        logppl = lda_model.logPerplexity(idf_model.transform(vectorizer_model.transform(df)))
        print(f"Log Perplexity: {logppl: .4f}")


def mount_s3_bucket(access_key, secret_key, bucket_name, mount_folder):
    """mount s3 bucket in databricks"""
    ACCESS_KEY_ID = access_key
    SECRET_ACCESS_KEY = secret_key
    ENCODED_SECRET_KEY = SECRET_ACCESS_KEY.replace("/", "%2F")

    print("Mounting", bucket_name)

    try:
        # Unmount the data in case it was already mounted.
        dbutils.fs.unmount("/mnt/%s" % mount_folder)

    except:
        # If it fails to unmount it most likely wasn't mounted in the first place
        print("Directory not unmounted: ", mount_folder)

    finally:
        # Lastly, mount our bucket.
        dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY_ID, ENCODED_SECRET_KEY, bucket_name), "/mnt/%s" % mount_folder)
        # dbutils.fs.mount("s3a://"+ ACCESS_KEY_ID + ":" + ENCODED_SECRET_KEY + "@" + bucket_name, mount_folder)
        print("The bucket", bucket_name, "was mounted to", mount_folder, "\n")


def topics_distribution_on_data(df, pipeline_model, n_topics):
    df_with_topics = pipeline_model.transform(df)
    to_array = F.udf(lambda v: v.toArray().tolist(), ArrayType(FloatType()))

    df_with_topics_toArray = df_with_topics.select(to_array("topicDistribution").alias("topicDistributionArray"))

    df_with_topics_final = df_with_topics_toArray.select([(F.col("topicDistributionArray")[i]).alias("topic_"+str(i)) for i in range(n_topics)])

    df_topic_info = df_with_topics_final.agg(*[F.avg(F.col("topic_"+str(i))).alias("topic_"+str(i)+ "(pecent)") for i in range(n_topics)])
    
    return df_topic_info

def convert_raw_csv_to_parquet(df, parquet_file_to_save):
    n_partitions = 8

    # df = (spark
    #   .read
    #   .format("csv")
    #   .options(header=False, sep="\t", enforceSchema=True)
    #   .schema(file_schema)
    #   .load(files_path_csv))

    df = df.repartition(numPartitions=n_partitions)

    (df.write.format("parquet").mode("overwrite").option("compression", "snappy").save(parquet_file_to_save))

    return df
