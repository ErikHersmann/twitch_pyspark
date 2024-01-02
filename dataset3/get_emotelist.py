# ran this on 1/1/2024
import requests


def fetch_emotelist(channel_name):
    # open 7tv home/emote tab respectively and open the console on chrome, navigate to the network tab and select the bottom-most wql and right click copy as curl bash
    # input that into https://web.archive.org/web/20210408111108/https://curl.trillworks.com/
    # change the \\\\ into \\ and remove starting $
    headers = {
        'authority': '7tv.io',
        'accept': '*/*',
        'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1IjoiNjE0YjlhODU0M2IyZDlkYTBkMzIwYjdkIiwidiI6MSwiaXNzIjoic2V2ZW50di1hcGkiLCJleHAiOjE3MDM0ODQwOTEsIm5iZiI6MTY5NTcwODA5MSwiaWF0IjoxNjk1NzA4MDkxfQ.9L05lQe9RyHAVhSBdoefHUwwjhJzfcUE7jA21ELL5BA',
        'content-type': 'application/json',
        'origin': 'https://7tv.app',
        'referer': 'https://7tv.app/',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    data = '{"operationName":"SearchUsers","variables":{"query":"placeholder"},"query":"query SearchUsers($query: String!) {\\n users(query: $query) {\\n id\\n username\\n display_name\\n roles\\n style {\\n color\\n __typename\\n }\\n avatar_url\\n __typename\\n }\\n}"}'.replace('placeholder', channel_name)

    response = requests.post('https://7tv.io/v3/gql', headers=headers, data=data)
    # id
    channel_id = response.json()['data']['users'][0]['id']

    headers = {
        'authority': '7tv.io',
        'accept': '*/*',
        'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
        'authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1IjoiNjE0YjlhODU0M2IyZDlkYTBkMzIwYjdkIiwidiI6MSwiaXNzIjoic2V2ZW50di1hcGkiLCJleHAiOjE3MDM0ODQwOTEsIm5iZiI6MTY5NTcwODA5MSwiaWF0IjoxNjk1NzA4MDkxfQ.9L05lQe9RyHAVhSBdoefHUwwjhJzfcUE7jA21ELL5BA',
        'content-type': 'application/json',
        'origin': 'https://7tv.app',
        'referer': 'https://7tv.app/',
        'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'cross-site',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    }

    data = '{"operationName":"GetEmoteSet","variables":{"id":"placeholder_id"},"query":"query GetEmoteSet($id: ObjectID\u0021, $formats: [ImageFormat\\u0021]) {\\n emoteSet(id: $id) {\\n id\\n name\\n flags\\n capacity\\n origins {\\n id\\n weight\\n __typename\\n }\\n emotes {\\n id\\n name\\n actor {\\n id\\n display_name\\n avatar_url\\n __typename\\n }\\n origin_id\\n data {\\n id\\n name\\n flags\\n state\\n lifecycle\\n host {\\n url\\n files(formats: $formats) {\\n name\\n format\\n __typename\\n }\\n __typename\\n }\\n owner {\\n id\\n display_name\\n style {\\n color\\n __typename\\n }\\n roles\\n __typename\\n }\\n __typename\\n }\\n __typename\\n }\\n owner {\\n id\\n username\\n display_name\\n style {\\n color\\n __typename\\n }\\n avatar_url\\n roles\\n connections {\\n id\\n display_name\\n emote_capacity\\n platform\\n __typename\\n }\\n __typename\\n }\\n __typename\\n }\\n}"}'.replace('placeholder_id', channel_id)

    response = requests.post('https://7tv.io/v3/gql', headers=headers, data=data)
    # emote list
    data = response.json()
    emotelist = []
    try:
        for emote in data['data']['emoteSet']['emotes']:
            emotelist.append(emote['data']['name'])
        return emotelist
    except TypeError:
        return emotelist


channel_list = requests.get("https://logs.ivr.fi/channels").json()
channel_list = [channel['name'] for channel in channel_list['channels']]

output = ""

for channel in channel_list:
    output += channel + ';' + str(fetch_emotelist(channel)) + "\n"
    print('finished:', channel)

with open('emotelist_per_channel.csv', 'w') as f:
    f.write(output.strip())