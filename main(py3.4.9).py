import pyspark, glob, random

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import substring, col, sha2, split, regexp_replace, countDistinct, udf, sum,avg,max,min,mean,count, corr, explode
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, FloatType

"""
from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline

model_name = 'distilbert-base-uncased-finetuned-sst-2-english'
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)
nlp = pipeline('sentiment-analysis', model=model, tokenizer=tokenizer)
"""
def sent_nlp(message):
    result = nlp(message)[0]
    if result['label'] == 'NEGATIVE':
        return -result['score']
    return result['score']
def sent_nlp(message):
	return random.uniform(-1, 1)

def find_words_in_list(message, word_list):
   words = message.split(" ")
   word_list = word_list.split(',')
   output = [word for word in words if word in word_list]
   return ",".join(set(output))

sentiment_message_udf = udf(lambda x: sent_nlp(x), FloatType())
length_udf = udf(lambda x: len(x.split(',')) if x != '[]' else 0, IntegerType())
find_words_udf = udf(find_words_in_list, StringType())


# Initialize SparkSession
spark = SparkSession.Builder().appName(name="irc").getOrCreate() #.set("spark.executor.memory", "12g")


logfiles = glob.glob("./dataset1/*.txt")[:25]  # get all logs (populate this with files)

print(logfiles)

##################################################################################################
##################################################################################################
##################################################################################################

# Read text files into DataFrame
df1 = spark.read.text(logfiles)
df1 = df1.filter(df1.value != "")

df1 = df1.withColumn('value',split(df1.value, ' ').cast('array<string>'))
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
df1 = df1.withColumn('sentiment', sentiment_message_udf(df1.message)).persist()

print("df1/messages after preprocessing")
df1.show(1, False)

df1_channels = df1.groupBy('channel').agg(avg('sentiment').alias("avg_sentiment"), countDistinct('user').alias('total_chatters')).persist()


print("positive sent channels")
df1_channels.sort("avg_sentiment", ascending=False).show(10, False)
print("negative sent channels")
df1_channels.sort("avg_sentiment", ascending=True).show(10, False)



df1_users = df1.groupBy("user").agg(avg('sentiment').alias("avg_sentiment"), count('user').alias('total_messages'), countDistinct('message').alias('uniq_messages'))
df1_users = df1_users.withColumn('uniqueness', df1_users.uniq_messages / df1_users.total_messages)


print("most active users")
df1_users.sort("total_messages", ascending=False).show(20, False)


##################################################################################################
##################################################################################################
##################################################################################################

df2 = spark.read.csv("./dataset2/metadata.csv", sep=',', header=True)
df2 = df2.withColumnRenamed("Channel", "channel")
df2 = df2.withColumn('channel', F.lower(df2.channel))


print("metadata")
df2.show(1, False)

##################################################################################################
##################################################################################################
##################################################################################################


df3 = spark.read.csv("./dataset3/emotelist_per_channel.csv", sep=';')
df3 = df3.select(col("_c0").alias("channel"), col("_c1").alias("emotelist"))
df3 = df3.filter(df3.emotelist != '[]')
df3 = df3.withColumn('emotelist', regexp_replace(regexp_replace(regexp_replace(df3.emotelist, r"', '", ','), r"\['", ""), r"'\]", ""))
df3 = df3.withColumn('length', length_udf(df3.emotelist)).persist()



print("emotelists")
df3.show(1, True)

##################################################################################################
##################################################################################################
##################################################################################################

df1 = df1.drop("date").drop("time").drop("user")


joined_df = df1.join(df3, on="channel")
joined_df = joined_df.withColumn('emotes_in_msg', find_words_udf(joined_df.message, joined_df.emotelist))
joined_df = joined_df.drop("emotelist").drop("channel").drop("length").filter(joined_df.emotes_in_msg != "").persist()



print("emotes in messages")
joined_df.show(1, truncate=False)


emote_sents = joined_df.withColumn("emote", explode(F.split(joined_df["emotes_in_msg"], ",")))
emote_sents = emote_sents.filter(emote_sents.emote != '').drop("emotes_in_msg")
emote_sents = emote_sents.groupBy('emote').avg('sentiment').select('emote', 'avg(sentiment)')
emote_sents = emote_sents.withColumn('actual_sent', sentiment_message_udf(emote_sents.emote)).persist()


print("reverse engineered sentiments for each emote")
print("positive emotes")
emote_sents.sort("avg(sentiment)", ascending=False).show(10, False)
print("negative emotes")
emote_sents.sort("avg(sentiment)", ascending=True).show(10, False)



"""
df1.write.mode("overwrite").csv("output/df1")
joined_df.write.mode("overwrite").csv("output/joined_df3_df1")
emote_sents.write.mode("overwrite").csv("output/emote_sentiment")
df2.write.mode("overwrite").csv("output/metadata")
df3.write.mode("overwrite").csv("output/df3")
df1_users.write.mode("overwrite").csv("output/users")
df1_channels.write.mode("overwrite").csv("output/channels")
"""


print("correlation: uniqueness, average sentiment (by user)", str(df1_users.agg(corr("uniqueness", "avg_sentiment").alias('corr')).collect()).split("corr=")[1].replace(")]", ""))
print("correlation: total messages, average sentiment (by user)",str(df1_users.agg(corr("total_messages", "avg_sentiment").alias('corr')).collect()).split("corr=")[1].replace(")]",""))

# join df2 on channel, drop non matched
df1_channels = df1_channels.join(df2, on="channel", how="inner")
print("correlation: average viewers, average sentiment (by channel)",str(df1_channels.agg(corr("Average viewers", "avg_sentiment").alias('corr')).collect()).split("corr=")[1].replace(")]", ""))

# join df3 on channel, drop non matched
df1_channels = df1_channels.join(df3, on='channel', how="inner")
print("correlation: length emoteset, average sentiment (by channel)", str(df1_channels.agg(corr("length", "avg_sentiment").alias('corr')).collect()).split("corr=")[1].replace(")]", ""))

#df1_channels.show(10, False)
