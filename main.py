import pyspark, glob, random

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import substring, col, sha2, split, regexp_replace, count_distinct, udf, sum,avg,max,min,mean,count, corr, explode
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, FloatType

from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

model_name = 'distilbert-base-uncased-finetuned-sst-2-english'
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)
nlp = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)

def sent_nlp(message):
    result = nlp(message)[0]
    if result['label'] == 'NEGATIVE':
        return -result['score']
    return result['score']


def find_words_in_list(message, word_list):
   words = message.split(" ")
   word_list = word_list.split(',')
   output = [word for word in words if word in word_list]
   return ",".join(set(output))

sentiment_message_udf = udf(lambda x: sent_nlp(x), FloatType())
length_udf = udf(lambda x: len(x.split(',')) if x != '[]' else 0, IntegerType())
find_words_udf = udf(find_words_in_list, StringType())


# Initialize SparkSession
spark = SparkSession.Builder().appName(name="irc").getOrCreate()
#     .config("spark.executor.memory", "8g")\


logfiles = glob.glob("./dataset1/*.txt")[2]  # get all logs (populate this with files)
print(logfiles)
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
df1 = df1.withColumn('sentiment', sentiment_message_udf(df1.message))#.persist()
df1_channels = df1.groupBy('channel').agg(avg('sentiment').alias("avg_sentiment"), count('user').alias('total_chatters'))


#df1_duplicates = df1.groupBy(df1.columns).count().filter("count > 1")
#df1_duplicates.show(5)

df1_users = df1.groupBy("user").agg(avg('sentiment').alias("avg_sentiment"), count('user').alias('total_messages'), count_distinct('message').alias('uniq_messages'))
df1_users = df1_users.withColumn('uniqueness', df1_users.uniq_messages / df1_users.total_messages)




df2 = spark.read.csv("./dataset2/metadata.csv", sep=',', header=True)



df3 = spark.read.csv("./dataset3/emotelist_per_channel.csv", sep=';')
df3 = df3.select(col("_c0").alias("channel"), col("_c1").alias("emotelist"))
df3 = df3.filter(df3.emotelist != '[]')
df3 = df3.withColumn('emotelist', regexp_replace(regexp_replace(regexp_replace(df3.emotelist, r"', '", ','), r"\['", ""), r"'\]", ""))
df3 = df3.withColumn('length', length_udf(df3.emotelist))

joined_df = df1.join(df3, on="channel")
# udf: if emotes from list in message return emotes
# new df4 with sentiment and emotes(basically message reduced to emotes)
joined_df = joined_df.withColumn('emotes_in_msg', find_words_udf(joined_df.message, joined_df.emotelist))


# explode df4 to have emote,sent pairs then groupby emote and avg sent
emote_sents = joined_df.withColumn("emote", explode(F.split(joined_df["emotes_in_msg"], ","))).drop('emotelist').drop('length').drop('message').drop('user').drop('date').drop('time').drop('channel').drop('emotes_in_msg')
emote_sents = emote_sents.filter(emote_sents.emote != '')
emote_sents = emote_sents.groupBy('emote').avg('sentiment').select('emote', 'avg(sentiment)')



df1.show(10)
joined_df.show(10)
emote_sents.show(10)
#result_df.show(10)
#channel_emote_pairs.show(10)
#df2.show(10, truncate=100)
#df3.show(10, truncate=100)
#df1_users.show(10, truncate=100)
#df1_channels.show(10, truncate=100)
#print(df1_users.agg(corr("uniqueness", "avg_sentiment").alias('corr_1')).collect())