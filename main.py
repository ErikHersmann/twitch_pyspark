import pyspark, glob, random

from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, col, sha2, split, regexp_replace, count_distinct, udf, sum,avg,max,min,mean,count, corr
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, FloatType

from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

model_name = 'distilbert-base-uncased-finetuned-sst-2-english'
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)
nlp = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)

sentiment_message_udf = udf(lambda x: nlp(x)[0]['score'], FloatType())
length_udf = udf(lambda x: len(x.split(',')) if x != '[]' else 0, IntegerType())


# Initialize SparkSession
spark = SparkSession.builder \
    .appName("irc") \
    .config("spark.executor.memory", "500M").config("spark.executor.instances", 4).config("spark.executor.cores", 1) \
    .getOrCreate()


logfiles = glob.glob("./dataset1/*.txt")  # get all logs (populate this with files)

# Read text files into DataFrame
df1 = spark.read.text(logfiles)
df1 = df1.filter(df1.value != "")

df1 = df1.withColumn('value', split(df1.value, ' ', limit=5).cast('array<string>'))
df1 = df1.withColumn('date', df1.value.getItem(0))
df1 = df1.withColumn('time', df1.value.getItem(1))
df1 = df1.withColumn('channel', df1.value.getItem(2))
df1 = df1.withColumn('user', df1.value.getItem(3))
df1 = df1.withColumn('message', df1.value.getItem(4))
df1 = df1.drop('value') #redundant

# replace regex with substring ?
df1 = df1.withColumn('date', regexp_replace(df1.date, r'\[', ''))
df1 = df1.withColumn('time', regexp_replace(df1.time, r'\]', ''))
df1 = df1.withColumn('channel', regexp_replace(df1.channel, r'\#', ''))
df1 = df1.withColumn('user', regexp_replace(df1.user, r'\:', ''))
df1 = df1.withColumn('user', sha2(col('user'), 224)) # hash users
df1 = df1.withColumn('sentiment', sentiment_message_udf(df1.message))
df1_channels = df1.groupBy('channel').agg(avg('sentiment').alias("avg_sentiment"), count('user').alias('total_chatters'))


#df1_duplicates = df1.groupBy(df1.columns).count().filter("count > 1")
#df1_duplicates.show(5)

df1_users = df1.groupBy("user").agg(avg('sentiment').alias("avg_sentiment"), count('user').alias('total_messages'), count_distinct('message').alias('uniq_messages'))
df1_users = df1_users.withColumn('uniqueness', df1_users.uniq_messages / df1_users.total_messages)




df2 = spark.read.csv("./dataset2/metadata.csv", sep=',', header=True)

df3 = spark.read.csv("./dataset3/emotelist_per_channel.csv", sep=';')
df3 = df3.select(col("_c0").alias("channel"), col("_c1").alias("emotelist"))
df3 = df3.withColumn('emotelist', regexp_replace(regexp_replace(regexp_replace(df3.emotelist, r"', '", ','), r"\['", ""), r"'\]", ""))
df3 = df3.withColumn('length', length_udf(df3.emotelist))


df1.show(50)
#df2.show(10, truncate=100)
#df3.show(10, truncate=100)
#df1_users.show(10, truncate=100)
#df1_channels.show(10, truncate=100)
#print(df1_users.agg(corr("uniqueness", "avg_sentiment").alias('corr_1')).collect())