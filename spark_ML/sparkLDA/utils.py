import pyspark.sql.functions as F
from pyspark.sql.types import *


def termsIdx2Term(vocabulary):
    """Convert word index to word"""

    def termsIdx2Term(termIndices):
        return [vocabulary[int(index)] for index in termIndices]

    return F.udf(termsIdx2Term, ArrayType(StringType()))

def show_topics(pipeline_model):
    """Shows the model topics and associated words"""
    # get the LDA model and other useful part
    lda_model = pipeline_model.stages[-1]

    vectorizer_model = pipeline_model.stages[0]
    vocabList = vectorizer_model.vocabulary

    # show the topics and the associated words
    topics_describe = lda_model.describeTopics()

    final = topics_describe.withColumn("Terms", termsIdx2Term(vocabList)("termIndices"))

    final.select("Terms").show(truncate=False)

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
