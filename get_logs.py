import glob
from requests import get
months = {
    1: 31, # January
    2: 28, # February
    3: 31, # March
    4: 30, # April
    5: 31, # May
    6: 30, # June
    7: 31, # July
    8: 31, # August
    9: 30, # September
    10: 31, # October
    11: 30, # November
    12: 31 # December
    }
logfiles = [i.split('/')[-1] for i in glob.glob("./dataset1/*.txt")] # get all lognames
finished = []
for file in logfiles:
    channel = file.split('-')[0]
    month = file.split('-')[2]
    if month == '12' and channel not in finished:
        finished.append(channel)

def get_log_and_write_to_txt(url, channel_name):
    name = channel_name + url.split(channel_name)[1] + ".txt"
    name = name.replace("/", "-")
    if channel_name not in finished and name not in logfiles:
        try:
            response = get(url).text.strip()
        except:
            print(f"Error downloading {name}")
            return -1
        if len(response) < 100:
            print(f"No logs for {name}")
            return -1
        with open('dataset1/' + name, 'w', encoding="utf-8") as f:
            f.write(response)
        print(f"Finished downloading {name}")
        return 1
    else:
        print(f"File already exists {name}")
        return 1


def download_logs(channel_name, year='2023'):
    base = "https://logs.ivr.fi/channel/placeholder/year/".replace('placeholder', channel_name).replace("year", year)
    # maybe include 2018-2022 too ?
    counter = 0
    for month in range(1, 13):
        for day in range(1, months[month]+1):
            url = base + str(month) + "/" + str(day)
            counter += get_log_and_write_to_txt(url, channel_name=channel_name)
        if counter < -32:
            print(f"quitting {channel_name} prematurely")
            break

# lirik logs

channel_list = get("https://logs.ivr.fi/channels").json()

#import json
#with open('channel_list.json', 'w', encoding='utf-8') as f:
#    json.dump(channel_list, f, ensure_ascii=False, indent=4)

channel_list = [channel['name'] for channel in channel_list['channels']]
for channel in channel_list:
    download_logs(channel, year='2022')
