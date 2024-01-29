import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px


pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 2000)
pd.set_option('display.max_colwidth', None)
"""
emotes = pd.read_json("emotes.json", lines=True)
emotes.sort_values("avg(sentiment)", ascending=False, inplace=True)
emotes.avg_sentiment = emotes["avg(sentiment)"] * (1/1)
emotes.index = range(1, len(emotes["avg(sentiment)"])+1)
emotes.to_csv("emotes_2.csv")


channels = pd.read_json("channels.json", lines=True)
channels.sort_values("avg_sentiment", ascending=False, inplace=True)
channels.avg_sentiment = channels.avg_sentiment
channels.index = range(1, len(channels.avg_sentiment)+1)
print(channels.columns)
channels.to_csv("channels_4.csv")


chunks = pd.read_json("users.json", lines=True, chunksize=100000)
users = pd.concat(chunks, ignore_index=True)
users.drop("user", inplace=True, axis=1)
users.to_csv("users_2.csv")
"""



channels = pd.read_csv("channels.csv")
channels = channels[["channel", "avg_sentiment", "total_chatters"]]
print(channels.columns)
#print(channels.head(3))
emotes = pd.read_csv("emotes.csv")
emotes = emotes[emotes.columns[1:]]
print(emotes.columns)
#print(emotes.head(3))


plt.hist(emotes["avg(sentiment)"], bins=50, range=(-0.2, 0.2))
plt.xlabel('Values')
plt.ylabel('Frequency')
plt.title('Histogram of emote sentiments') 
plt.savefig("plots/plot_emotes.png")


plt.hist(channels["avg_sentiment"], bins=50, range=(-0.2, 0.2))
plt.xlabel('Values')
plt.ylabel('Frequency')
plt.title('Histogram of channel sentiments') 
plt.savefig("plots/plot_channels.png")


users = pd.read_csv("users.csv")
users = users[users.columns[1:]]
sampled = users.sample(frac=0.1, random_state=7)
#users.drop(users.tail(1000000).index, inplace=True)
print(users.columns, users.shape)
print(sampled.columns, sampled.shape)


fig = px.scatter(sampled, x="avg_sentiment", y="uniqueness", trendline="ols", range_y=(0,1),
                 title="user sentiment and uniqueness", range_x=(-1, 1), trendline_color_override="red")
fig.write_image("plots/plot_users_uniq+sent.png")

fig = px.scatter(sampled, x="avg_sentiment", y="total_messages", trendline="ols",
                 title="user sentiment and total messages", range_x=(-1, 1), trendline_color_override="red")
fig.write_image("plots/plot_users_msgs+sent.png")


fig = plt.figure(figsize =(10, 7))
plt.boxplot(sampled["total_messages"])
plt.title('boxplot of user message count') 
plt.savefig("plots/plot_users_msgs.png")