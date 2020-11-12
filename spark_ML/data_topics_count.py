
from pyspark.ml import PipelineModel 
import pyspark.sql.functions as F

pipelinePath = "./LDA-pipeline-model" 

pipeline_model = PipelineModel.load(pipelinePath)

# 5. check the topic distribution among dataset
df_with_topics = pipeline_model.transform(df).select("tweet_text", "topicDistribution")
to_array = F.udf(lambda v: v.toArray().tolist(), ArrayType(FloatType()))
df_with_topics_toArray = df_with_topics.select("tweet_text", to_array("topicDistribution").alias("topicDistributionArray"))

df_with_topics_final = df_with_topics_toArray.select("tweet_text" + [(F.col("topicDistributionArray")[i]).alias("topic_"+str(i)) for i in range(10)])

df_with_topics_final.agg(*[F.sum(F.col("topic_"+str(i) for i in range(n_topics)))])