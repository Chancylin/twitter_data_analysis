from pyspark.sql.types import *

n_topics = 10

# set seed
seedNum = 1234

pipelinePath = "./models/LDA-pipeline-model_test"

# one can load the stop word from json file
stopwords_list_file = "../stopwords_list.json"

stopwords_list = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself",
                  "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself",
                  "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that",
                  "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had",
                  "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as",
                  "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through",
                  "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off",
                  "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how",
                  "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not",
                  "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should",
                  "now", "i'll", "you'll", "he'll", "she'll", "we'll", "they'll", "i'd", "you'd", "he'd", "she'd",
                  "we'd", "they'd", "i'm", "you're", "he's", "she's", "it's", "we're", "they're", "i've", "we've",
                  "you've", "they've", "isn't", "aren't", "wasn't", "weren't", "haven't", "hasn't", "hadn't", "don't",
                  "doesn't", "didn't", "won't", "wouldn't", "shan't", "shouldn't", "mustn't", "can't", "couldn't",
                  "cannot", "could", "here's", "how's", "let's", "ought", "that's", "there's", "what's", "when's",
                  "where's", "who's", "why's", "would", "also", "via", "cc", "rt", "must", "always"]

extra_for_stemmed = ["learn", "ai", "machin", "use", "intellig", "new", "artifici", "read",
                     'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                     'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']

file_schema = StructType([StructField("raw_tweet_text", StringType(), True),
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

file_schema_str = StructType([StructField("raw_tweet_text", StringType(), True),
                          StructField("hash_tag", StringType(), True),
                          StructField("created_at", StringType(), True),
                          StructField("retweet_count", StringType(), True),
                          StructField("favorite_count", StringType(), True),
                          StructField("retweeted", StringType(), True),
                          StructField("truncated", StringType(), True),
                          StructField("id", StringType(), True),
                          StructField("user_name", StringType(), True),
                          StructField("screen_name", StringType(), True),
                          StructField("followers_count", StringType(), True),
                          StructField("location", StringType(), True),
                          StructField("geo", StringType(), True),
                          StructField("invalid", StringType(), True)])

cols_select = ['raw_tweet_text', 'hash_tag', 'created_at', 'retweet_count', 'favorite_count']
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