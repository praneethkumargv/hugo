import json

input_file = open ('4121195473.fjson')
json_array = json.load(input_file)
store_list = []

for item in json_array:
    store_details = {}
    store_details['cpu'] = item['mean_CPU_usage_rate'] * 1000000
    store_details['mem'] = item['sum_memory'] * 1000000
    print(store_details['cpu'], store_details['mem'])
