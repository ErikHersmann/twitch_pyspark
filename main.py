import pyspark, glob, random

from pyspark import SparkConf , SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import substring, col, sha2, split, regexp_replace
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


confCluster = SparkConf().setAppName("twitch_freq")
sc = SparkContext(conf=confCluster)
sqlContext=SQLContext(sc)
logfiles = glob.glob("./dataset1/*.txt") # get all logs (populate this maybe)
print(logfiles)

# df1 = sqlContext.createDataFrame(sc.emptyRDD(), schema=["date","time","channel","user", "message"])
df1 = sqlContext.read.text(logfiles)
df1 = df1.filter(df1.value != "")

df1 = df1.withColumn('value', split(df1.value, ' ', limit=5).cast('array<string>'))
df1 = df1.withColumn('date', df1.value.getItem(0))
df1 = df1.withColumn('time', df1.value.getItem(1))
df1 = df1.withColumn('channel', df1.value.getItem(2))
df1 = df1.withColumn('user', df1.value.getItem(3))
df1 = df1.withColumn('message', df1.value.getItem(4))


df1 = df1.withColumn('date', regexp_replace(df1.date, r'\[', ''))
df1 = df1.withColumn('time', regexp_replace(df1.time, r'\]', ''))
df1 = df1.withColumn('channel', regexp_replace(df1.channel, r'\#', ''))
df1 = df1.withColumn('user', regexp_replace(df1.user, r'\:', ''))
df1 = df1.drop('value')
df1 = df1.withColumn('user', sha2(col('user'), 256))

print(df1.columns)
df1.show(5)
# hash users


"""

metadata = sc.textFile("./dataset3/emotelist_per_channel.csv")
metadata = metadata.map(lambda x: x.split(","))
df2 = sqlContext.createDataFrame(textlogs, schema=
                                 ["channel","watchtime", "streamtime", "peakviewers", "avgviewers", "followers", "followersgained", "viewsgained", "partnered", "mature", "language"])


emotelogs = sc.textFile("./dataset3/emotelist_per_channel.csv")
emotelogs = emotelogs.map(lambda x: x.split(";", 1))
df3 = sqlContext.createDataFrame(textlogs, schema=["channel","emotes"])


print((df1.count(), len(df1.columns)))
print((df2.count(), len(df2.columns)))
print((df3.count(), len(df3.columns)))






###############################################################
###############################################################
###############################################################

from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

model_name = 'distilbert-base-uncased-finetuned-sst-2-english'
# get lang from dataset2 and choose a corresponding different model
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)
nlp = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)
messages = df1['message'].tolist()
results = nlp(messages)
df1['sentiment'] = [result['label'] for result in results]

"""