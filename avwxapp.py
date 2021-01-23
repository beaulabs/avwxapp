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
#stncol = dbav.station

# FUNCTIONS


def pull_metar():

    metar_stn_list = ["katl", "klax", "kord", "kdfw", "kden", "kjfk",
                      "ksfo", "klas", "ksea", "kclt", "kmco", "kmia", "kphx", "kewr", "kiah"]
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

# MAIN CALLS


pull_metar()
