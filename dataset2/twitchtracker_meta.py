import requests, json


base = 'https://twitchtracker.com/api/channels/summary/placeholder'
channel_list = requests.get("https://logs.ivr.fi/channels").json()

channel_list = [channel['name'] for channel in channel_list['channels']]
df = {'data':[]}

for channel in channel_list:
    try:
        df['data'].append({channel: requests.get(base.replace('placeholder', channel)).json()})
    except:
        print(channel)

with open('twitchtracker_metadata.json', 'w', encoding='utf-8') as f:
    json.dump(df, f, ensure_ascii=False, indent=4)






