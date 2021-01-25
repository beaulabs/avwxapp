# Import additional modules
import os
import requests
import datetime
import json
import pymongo
import re


# Set environment api key for CHECKWXAPI - test os function / discard
# print(os.environ['CHECKWXAPI'])

# VARIABLES
# Set core variables in this section
# ------------------------------------------------
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


#stncol = dbav.station

# FUNCTIONS


def pull_metar():

    metar_stn_list = ["katl", "zbaa", "klax", "omdb", "rjtt", "kord", "egll", "zspd", "lfpg", "kdfw", "zggg", "eham", "vhhh", "rksi", "eddf", "kden", "vidp", "wsss", "vtbs", "kjfk",
                      "wmkk", "lemd", "ksfo", "zuuu", "wiii", "zgsz", "lebl", "ltfm", "ksea", "klas", "kmco", "cyyz", "mmmx", "kclt", "uuee", "rctp", "zppp", "eddm", "rpll", "zlxy", "vabb",
                      "egkk", "kewr", "kphx", "kmia", "zsss", "kiah", "zuck", "yssy", "rjaa"]
    metar_stn_icao = ''
    metar_format1 = '"results": 1, "data": [{'
    metar_format2 = '}]}'

    for i in metar_stn_list:
        metar_stn_icao = i
        metar_url = 'https://api.checkwx.com/metar/%s/decoded' % (
            metar_stn_icao)
        metar_stn_req = requests.get(metar_url, headers=hdr)
        str_metar = json.dumps(metar_stn_req.json())
        str_metar_format = str_metar.replace(metar_format1, '')
        str_metar_formatted = str_metar_format.replace(metar_format2, '}')
        dict_metar = json.loads(str_metar_formatted)
        metar_rec_insert = metarcol.insert_one(dict_metar)


def pull_taf():

    taf_stn_list = ["katl", "zbaa", "klax", "omdb", "rjtt", "kord", "egll", "zspd", "lfpg", "kdfw", "zggg", "eham", "vhhh", "rksi", "eddf", "kden", "vidp", "wsss", "vtbs", "kjfk",
                    "wmkk", "lemd", "ksfo", "zuuu", "wiii", "zgsz", "lebl", "ltfm", "ksea", "klas", "kmco", "cyyz", "mmmx", "kclt", "uuee", "rctp", "zppp", "eddm", "rpll", "zlxy", "vabb",
                    "egkk", "kewr", "kphx", "kmia", "zsss", "kiah", "zuck", "yssy", "rjaa"]
    taf_stn_icao = ''
    taf_format1 = '"results": 1, "data": [{'
    taf_format2 = '}]}'

    for i in taf_stn_list:
        taf_stn_icao = i
        taf_url = 'https://api.checkwx.com/taf/%s/decoded' % (taf_stn_icao)
        taf_stn_req = requests.get(taf_url, headers=hdr)
        str_taf = json.dumps(taf_stn_req.json())
        str_taf_format = str_taf.replace(taf_format1, '')
        str_taf_formatted = str_taf_format.replace(taf_format2, '}')
        dict_taf = json.loads(str_taf_formatted)
        taf_rec_insert = tafcol.insert_one(dict_taf)


def pull_stn():

    stn_list = ["katl", "zbaa", "klax", "omdb", "rjtt", "kord", "egll", "zspd", "lfpg", "kdfw", "zggg", "eham", "vhhh", "rksi", "eddf", "kden", "vidp", "wsss", "vtbs", "kjfk",
                "wmkk", "lemd", "ksfo", "zuuu", "wiii", "zgsz", "lebl", "ltfm", "ksea", "klas", "kmco", "cyyz", "mmmx", "kclt", "uuee", "rctp", "zppp", "eddm", "rpll", "zlxy", "vabb",
                "egkk", "kewr", "kphx", "kmia", "zsss", "kiah", "zuck", "yssy", "rjaa"]

    stn_icao = ''
    stn_format1 = '"results": 1, "data": [{'
    stn_format2 = '}]}'

    for i in stn_list:
        stn_icao = i
        stn_url = 'https://api.checkwx.com/station/%s' % (stn_icao)
        stn_req = requests.get(stn_url, headers=hdr)
        str_stn = json.dumps(stn_req.json())
        str_stn_format = str_stn.replace(stn_format1, '')
        str_stn_formatted = str_stn_format.replace(stn_format2, '}')
        dict_stn = json.loads(str_stn_formatted)
        stn_rec_insert = stncol.insert_one(dict_stn)


# MAIN CALLS
pull_metar()
pull_taf()
pull_stn()
