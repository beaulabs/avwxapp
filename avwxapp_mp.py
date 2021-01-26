import os
import requests
from datetime import datetime
import time
import json
import pymongo
import re
from multiprocessing import Process, Queue, current_process, freeze_support
import queue

# Header for passing api key for checkwxapi calls
hdr = {"X-API-KEY": os.environ['CHECKWXAPI']}

# Connection variables to Atlas database
atlasconn = "mongodb+srv://{}@c108-avwxapp.9evcs.mongodb.net/<dbname>?retryWrites=true&w=majority".format(
    os.environ['ATMDBUSER'])
client = pymongo.MongoClient(atlasconn)
db = client.test
collection = db.testing
dbav = client.avweather
metarcol = dbav.metar
tafcol = dbav.taf
stncol = dbav.airports

# Processes


def stn_worker(stn):
    stn_icao = stn
    stn_format1 = '"results": 1, "data": [{'
    stn_format2 = '}]}'
    stn_url = 'https://api.checkwx.com/station/%s' % (stn_icao)
    stn_req = requests.get(stn_url, headers=hdr)
    str_stn = json.dumps(stn_req.json())
    str_stn_format = str_stn.replace(stn_format1, '')
    str_stn_formatted = str_stn_format.replace(stn_format2, '}')
    dict_stn = json.loads(str_stn_formatted)
    stn_rec_insert = stncol.insert_one(dict_stn)


def taf_worker(stn):
    taf_stn_icao = stn
    taf_format1 = '"results": 1, "data": [{'
    taf_format2 = '}]}'
    taf_url = 'https://api.checkwx.com/taf/%s/decoded' % (taf_stn_icao)
    taf_stn_req = requests.get(taf_url, headers=hdr)
    str_taf = json.dumps(taf_stn_req.json())
    str_taf_format = str_taf.replace(taf_format1, '')
    str_taf_formatted = str_taf_format.replace(taf_format2, '}')
    dict_taf = json.loads(str_taf_formatted)
    taf_rec_insert = tafcol.insert_one(dict_taf)


def metar_worker(stn):
    metar_stn_icao = stn
    metar_url = 'https://api.checkwx.com/metar/%s/decoded' % (
        metar_stn_icao)
    metar_format1 = '"results": 1, "data": [{'
    metar_format2 = '}]}'
    metar_stn_req = requests.get(metar_url, headers=hdr)
    str_metar = json.dumps(metar_stn_req.json())
    str_metar_format = str_metar.replace(metar_format1, '')
    str_metar_formatted = str_metar_format.replace(metar_format2, '}')
    dict_metar = json.loads(str_metar_formatted)
    metar_rec_insert = metarcol.insert_one(dict_metar)


def wx_worker(stn_to_process, stn_completed, skip_stnworker):
    while True:
        try:
            '''
                try to get task from queue. get_nowait() function will
                raise queue. Empty exception if the queue is empty.
                queue(False) function would do the same task also.
            '''
            skip_stnworker = skip_stnworker
            stn = stn_to_process.get_nowait()
            print("Processing Station: " + stn)
            metar_worker(stn)
            taf_worker(stn)
            if skip_stnworker == True:
                continue
            else:
                stn_worker(stn)

        except queue.Empty:
            break
        else:
            '''
                if no exception raised, add the stn completion message
                to stn_completed queue
            '''
            print(stn)
            stn_completed.put(
                stn + ' has been processed by: ' + current_process().name)
    return True


def wx_process():
    #number_of_stn = 50
    number_of_processes = 10
    stn_to_process = Queue()
    stn_completed = Queue()
    processes = []
    skip_stnworker = False

    # Check and ensure station collection hasn't already been populated. Airport information doesn't change very often (except operational flag)
    # which I'll build more logic in as I have time.
    stn_exist = stncol.count_documents({})
    if stn_exist == 50:
        skip_stnworker = True
    else:
        stncol.drop()

    # Tafs are only issued at 0000 / 0600 / 1200 / 1800 UTC. There is no need to poll the tafs with each run. This checks current UTC time
    # and if within range will trigger to run the taf_worker
    datetime_utcobj = datetime.utcnow()

    stn_list = ["katl", "zbaa", "klax", "omdb", "rjtt", "kord", "egll", "zspd", "lfpg", "kdfw", "zggg", "eham", "vhhh", "rksi", "eddf", "kden", "vidp", "wsss", "vtbs", "kjfk",
                "wmkk", "lemd", "ksfo", "zuuu", "wiii", "zgsz", "lebl", "ltfm", "ksea", "klas", "kmco", "cyyz", "mmmx", "kclt", "uuee", "rctp", "zppp", "eddm", "rpll", "zlxy", "vabb",
                "egkk", "kewr", "kphx", "kmia", "zsss", "kiah", "zuck", "yssy", "rjaa"]

    for i in stn_list:
        stn_to_process.put(i)

    # Create the processes
    for w in range(number_of_processes):
        p = Process(target=wx_worker, args=(
            stn_to_process, stn_completed, skip_stnworker))
        processes.append(p)
        p.start()

    # Completing the process
    for p in processes:
        p.join()

    # Print the station
    while not stn_completed.empty():
        print(stn_completed.get())

    return True


if __name__ == '__main__':
    wx_process()
