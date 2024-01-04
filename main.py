import pyspark, glob, random

from pyspark import SparkConf , SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import substring, col, sha2, split, regexp_replace, count_distinct, udf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, FloatType

from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

model_name = 'distilbert-base-uncased-finetuned-sst-2-english'
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)
nlp = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)

sentiment_message_udf = udf(lambda x: nlp(x)[0]['score'], FloatType())


confCluster = SparkConf().setAppName("twitch_freq")
sc = SparkContext(conf=confCluster)
sqlContext=SQLContext(sc)
logfiles = glob.glob("./dataset1/*.txt") # get all logs (populate this with files)


# sqlContext.createDataFrame(sc.emptyRDD())
df1 = sqlContext.read.text(logfiles)
df1 = df1.filter(df1.value != "")

df1 = df1.withColumn('value', split(df1.value, ' ', limit=5).cast('array<string>'))
df1 = df1.withColumn('date', df1.value.getItem(0))
df1 = df1.withColumn('time', df1.value.getItem(1))
df1 = df1.withColumn('channel', df1.value.getItem(2))
df1 = df1.withColumn('user', df1.value.getItem(3))
df1 = df1.withColumn('message', df1.value.getItem(4))

# replace regex with substring ?
df1 = df1.withColumn('date', regexp_replace(df1.date, r'\[', ''))
df1 = df1.withColumn('time', regexp_replace(df1.time, r'\]', ''))
df1 = df1.withColumn('channel', regexp_replace(df1.channel, r'\#', ''))
df1 = df1.withColumn('user', regexp_replace(df1.user, r'\:', ''))
df1 = df1.drop('value') #redundant
df1 = df1.withColumn('user', sha2(col('user'), 256)) # hash users
df1 = df1.withColumn('sentiment', sentiment_message_udf(df1.message))


#df1_duplicates = df1.groupBy(df1.columns).count().filter("count > 1")
#df1_duplicates.show(5)

# create new dataframe of users that has the count of users messages 
# or directly append to df1 ?
# df1_users = df1.groupBy("user").count()
df1_users = df1.groupBy("user").agg({'sentiment': 'avg', 'user': 'count'}) # is this wrong ?????????
#df1_user_frequency = df1_user_frequency.select(col("").alias("avg_sentiment"))
# df1_user_frequency.show(8)



df2 = sqlContext.read.csv("./dataset2/metadata.csv", sep=',', header=True)
#schema=["channel","watchtime", "streamtime", "peakviewers", "avgviewers", "followers", "followersgained", "viewsgained", "partnered", "mature", "language"]



df3 = sqlContext.read.csv("./dataset3/emotelist_per_channel.csv", sep=';')
df3 = df3.select(col("_c0").alias("channel"), col("_c1").alias("emotelist"))
df3 = df3.withColumn('emotelist', regexp_replace(regexp_replace(regexp_replace(df3.emotelist, r"', '", ','), r"\['", ""), r"'\]", ""))



df1.show(10, truncate=150)
df2.show(10, truncate=150)
df3.show(10, truncate=150)
#df1_users.show(10, truncate=150)