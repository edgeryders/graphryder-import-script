import json

def dumpSplit(data_topic, data_set, data, stats):
    path = './db/'
    n = 1000
    data_chunks = [data[i * n:(i + 1) * n] for i in range((len(data) + n - 1) // n )]
    for num, item in enumerate(data_chunks):
        with open(f'{path}{data_set}_{data_topic}_{str(num+1)}.json', 'w') as file:
            json.dump(item, file, default=str)
    stats['chunk_sizes'][data_topic] = len(data_chunks)
    return stats