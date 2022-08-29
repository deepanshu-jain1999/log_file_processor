from collections import defaultdict
from multiprocessing import Pool
from urllib import request
from datetime import datetime


def process_log(log_file):
    file_data = request.urlopen(log_file)
    result = defaultdict(dict)
    for data in file_data:
        _, time, exc = data.decode().strip().split(" ")
        date_time = datetime.fromtimestamp(int(time) // 1000)
        hour, minute = date_time.hour, date_time.minute
        minute = (minute // 15) * 15
        result[f"{hour}:{minute}"][exc] = result[f"{hour}:{minute}"].get(exc, 0) + 1
    return result


def pool_handler(log_files, parallel_process):
    pool = Pool(parallel_process)
    res = pool.map(process_log, log_files)
    return res
