import json
import logging

def dumpSplit(data_topic, data_set, data, stats):
    path = './db/'
    n = 1000
    data_chunks = [data[i * n:(i + 1) * n] for i in range((len(data) + n - 1) // n )]
    for num, item in enumerate(data_chunks):
        with open(f'{path}{data_set}_{data_topic}_{str(num+1)}.json', 'w') as file:
            json.dump(item, file, default=str)
    stats['chunk_sizes'][data_topic] = len(data_chunks)
    
    return stats

def init_logs():
    mylogs = logging.getLogger(__name__)
    mylogs.setLevel(logging.DEBUG)

    file = logging.FileHandler("ryderex-import.log")
    file.setLevel(logging.INFO)
    fileformat = logging.Formatter("%(asctime)s:%(levelname)s: %(message)s",datefmt="%H:%M:%S")
    file.setFormatter(fileformat)

    stream = logging.StreamHandler()
    stream.setLevel(logging.DEBUG)
    streamformat = logging.Formatter("%(asctime)s: %(message)s")
    stream.setFormatter(streamformat)

    mylogs.addHandler(file)
    mylogs.addHandler(stream)

    return mylogs