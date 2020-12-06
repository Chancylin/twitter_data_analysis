import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover

# Import stemmer library
from nltk.stem.porter import PorterStemmer
from .config import stopwords_list, cols_select

def preprocess_text(df):

    df_select = df.dropna(subset=["raw_tweet_text"]).select(cols_select)
    # 1. clean text
    df_select_clean = (df_select.withColumn("tweet_text", F.regexp_replace("raw_tweet_text", r"[@#&][A-Za-z0-9_-]+", " "))
                       .withColumn("tweet_text", F.regexp_replace("tweet_text", r"\w+:\/\/\S+", " "))
                       .withColumn("tweet_text", F.regexp_replace("tweet_text", r"[^A-Za-z]", " "))
                       .withColumn("tweet_text", F.regexp_replace("tweet_text", r"\s+", " "))
                       .withColumn("tweet_text", F.lower(F.col("tweet_text")))
                       .withColumn("tweet_text", F.trim(F.col("tweet_text")))
                      )

    # tokenize
    tokenizer = Tokenizer(inputCol="tweet_text", outputCol="tokens")

    # 2.2. remove stopwords
    stopword_remover = StopWordsRemover(inputCol="tokens", outputCol="remove_stop")
    stopword_remover.setStopWords(stopwords_list)

    #2.3. stemming
    # TODO: how to modify the stemming function into a transformer?
    stemmer = PorterStemmer()
    # more straightforward to use lambda
    stem_udf = F.udf(lambda l : [stemmer.stem(word) for word in l], returnType = ArrayType(StringType()))

    df_tokenized = tokenizer.transform(df_select_clean)
    df_rmstop = stopword_remover.transform(df_tokenized)
    df_stemmed = df_rmstop.withColumn("stemmed", stem_udf(F.col("remove_stop")))

    return df_stemmed