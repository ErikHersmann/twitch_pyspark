import glob, json


logfiles = [i.split('/')[-1] for i in glob.glob("./dataset1/*.txt")] # get all lognames
logged = {}
for file in logfiles:
    channel = file.split('-')[0]
    if channel not in logged.keys():
        logged[channel] = 1
    else:
        logged[channel] += 1

with open('dataset2/channel_list.json') as f:
    full_list = json.load(f)

for key in full_list['channels']:
    channel = key['name']
    if channel not in logged.keys():
        logged[channel] = 0

dont_log_these = []
for item in sorted(logged.items()):
    #print(item)
    if item[1] == 0:
        dont_log_these.append(item[0])

print(dont_log_these)
