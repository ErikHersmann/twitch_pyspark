import pyspark, glob, random

from pyspark import SparkConf , SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import substring, col, sha2


confCluster = SparkConf().setAppName("twitch_freq")
sc = SparkContext(conf=confCluster)
sqlContext=SQLContext(sc)
logfiles = glob.glob("./dataset1/*.txt") # get all logs


less_logfiles = []
for _ in range(30): # for testing
    less_logfiles.append(random.choice(logfiles))

df1 = sqlContext.createDataFrame(sc.emptyRDD(), schema=["date","time","channel","user", "message"])
for filename in less_logfiles:
    textlogs = sc.textFile(filename)
    textlogs = textlogs.map(lambda x: [element[1:] if idx in [0,2] else element[:-1] if idx in [1,3] else element 
                                       for idx,element in enumerate(x.split(" ", 4))
                                       ])
    df1 = df1.union(sqlContext.createDataFrame(textlogs, schema=["date","time","channel","user", "message"]))

# clean up df1 afterwards if the map is not efficient

# hash users
df1 = df1.withColumn('user', sha2(col('user'), 256))



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

