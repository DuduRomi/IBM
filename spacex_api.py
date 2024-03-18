# Requests allows us to make HTTP requests which we will use to get data from an API
import requests
# Pandas is a software library written for the Python programming language for data manipulation and analysis.
import pandas as pd
# NumPy is a library for the Python programming language, adding support for large, multi-dimensional arrays and matrices, along with a large collection of high-level mathematical functions to operate on these arrays
import numpy as np
# Datetime is a library that allows us to represent dates
import datetime

# # Setting this option will print all collumns of a dataframe
# pd.set_option('display.max_columns', None)
# # Setting this option will print all of the data in a feature
# pd.set_option('display.max_colwidth', None)
# API = ""
# url = "https://api.spacexdata.com/v4/launches/past"
# response = requests.get(url)

# response.json()

# # Wrangling Data using an API
# data = pd.json_normalize(response.json())

# # web scraping Falcon 9 Launch record 
# # using BeautifulSoup

"""
Data Wrangling problems
- Wrangling data using an API
getBoosterVersion -> https://api.spacexdata.com/v4/rockets
getLaunchSite -> https://api.spacexdata.com/v4/launchpads
getPayloadData -> https://api.spacexdata.com/v4/payloads
getCoreData -> https://api.spacexdata.com/v4/cores
- Sampling Data
Remove falcon1 launchs , or any other BoosterVersion and keep only falcon9
- Dealing with data
"""

#Global variables 
BoosterVersion = []
PayloadMass = []
Orbit = []
LaunchSite = []
Outcome = []
Flights = []
GridFins = []
Reused = []
Legs = []
LandingPad = []
Block = []
ReusedCount = []
Serial = []
Longitude = []
Latitude = []
import json 

def load_json_by_id(filename,id,value:str=""):
    # print(f"filename = {filename}, id = {id}, value = {value}")
    with open(filename,'r', encoding='utf8',errors='ignore') as json_file:
        temp =  json.load(json_file)
        for item in temp:
            if item[id] == value:
                # print(f"item = {item[id]}")
                return item
    return {}
    
def load_json(filename):
    print(f"filename {filename}")
    with open(filename,'r', encoding='utf8',errors='ignore') as json_file:
        temp =  json.load(json_file)
        # print(temp)
    return temp


def getBoosterVersion(data):
    for x in data['rocket']:
        if x:
            response = load_json_by_id("../datasets/spaceX/rockets.json","id",x)
            # response = requests.get("https://api.spacexdata.com/v4/rockets/"+str(x),verify='panw.pem').json()
            BoosterVersion.append(response['name'])

def getLaunchSite(data):
    for x in data['launchpad']:
        if x:
            response = load_json_by_id("../datasets/spaceX/launchpads.json","id",x)
            # response = requests.get("https://api.spacexdata.com/v4/launchpads/"+str(x),verify='panw.pem').json()
            Longitude.append(response['longitude'])
            Latitude.append(response['latitude'])
            LaunchSite.append(response['name'])

# Takes the dataset and uses the payloads column to call the API and append the data to the lists
def getPayloadData(data):
    for loads in data['payloads']:
        if loads:
            for load in loads:
                response = response = load_json_by_id("../datasets/spaceX/payloads.json","id",load)
                # response = requests.get("https://api.spacexdata.com/v4/payloads/"+load,verify='panw.pem').json()
                PayloadMass.append(response['mass_kg'])
                Orbit.append(response['orbit'])

# Takes the dataset and uses the cores column to call the API and append the data to the lists
def getCoreData(data):
    print("\ngetCoreData")
    for cores in data['cores']:
        for core in cores:
            # print(core['core'])
            if core['core'] != None:
                response  = load_json_by_id("../datasets/spaceX/cores.json","id",core['core'])
                # response = requests.get("https://api.spacexdata.com/v4/cores/"+core['core'],verify='panw.pem').json()
                Block.append(response['block'])
                ReusedCount.append(response['reuse_count'])
                Serial.append(response['serial'])
            else:
                Block.append(None)
                ReusedCount.append(None)
                Serial.append(None)
            Outcome.append(str(core['landing_success'])+' '+str(core['landing_type']))
            Flights.append(core['flight'])
            GridFins.append(core['gridfins'])
            Reused.append(core['reused'])
            Legs.append(core['legs'])
            LandingPad.append(core['landpad'])


spacex_url="https://api.spacexdata.com/v4/launches/past"
response = requests.get(spacex_url,verify='panw.pem')

print(response.content[0:100])

static_json_url='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DS0321EN-SkillsNetwork/datasets/API_call_spacex_api.json'
response = requests.get(static_json_url,verify='panw.pem')
# Get the head of the dataframe
data = pd.json_normalize(response.json())
print(data.head())
print(data.describe())

# # data = pd.json_normalize(pd.read_json("../datasets/API_call_spacex_api.json"))
data = pd.read_json("../datasets/spaceX/API_call_spacex_api.json")

getBoosterVersion(data)
getLaunchSite(data)
getPayloadData(data)
getCoreData(data)

print("BoosterVersion")
print(len(BoosterVersion))
print(BoosterVersion)
print(sum("Falcon 9" in x for x in BoosterVersion))

# Lets take a subset of our dataframe keeping only the features we want and the flight number, and date_utc.
data = data[['rocket', 'payloads', 'launchpad', 'cores', 'flight_number', 'date_utc']]

# We will remove rows with multiple cores because those are falcon rockets with 2 extra rocket boosters and rows that have multiple payloads in a single rocket.
data = data[data['cores'].map(len)==1]
data = data[data['payloads'].map(len)==1]

# Since payloads and cores are lists of size 1 we will also extract the single value in the list and replace the feature.
data['cores'] = data['cores'].map(lambda x : x[0])
data['payloads'] = data['payloads'].map(lambda x : x[0])

# We also want to convert the date_utc to a datetime datatype and then extracting the date leaving the time
data['date'] = pd.to_datetime(data['date_utc']).dt.date

# Using the date we will restrict the dates of the launches
data = data[data['date'] <= datetime.date(2020, 11, 13)]

'''
From the rocket we would like to learn the booster name

From the payload we would like to learn the mass of the payload and the orbit that it is going to

From the launchpad we would like to know the name of the launch site being used, the longitude, and the latitude.

From cores we would like to learn the outcome of the landing, the type of the landing, number of flights with that core, whether gridfins were used, whether the core is reused, whether legs were used, the landing pad used, the block of the core which is a number used to seperate version of cores, the number of times this specific core has been reused, and the serial of the core.
'''


launch_dict = {'FlightNumber': list(data['flight_number']),
'Date': list(data['date']),
'BoosterVersion':BoosterVersion,
'PayloadMass':PayloadMass,
'Orbit':Orbit,
'LaunchSite':LaunchSite,
'Outcome':Outcome,
'Flights':Flights,
'GridFins':GridFins,
'Reused':Reused,
'Legs':Legs,
'LandingPad':LandingPad,
'Block':Block,
'ReusedCount':ReusedCount,
'Serial':Serial,
'Longitude': Longitude,
'Latitude': Latitude
}

print("\nlaunch_dict")
# print(launch_dict)
# print(launch_dict['BoosterVersion'])
# print(launch_dict['LandingPad'])
# # print(len(launch_dict['LandingPad']))
print(sum(x == "Falcon 9" for x in launch_dict['BoosterVersion']))
print(sum(x == "Falcon 1" for x in launch_dict['BoosterVersion']))

print(sum(x is None for x in launch_dict['LandingPad']))
# Identify and calculate the percentage of the missing values in each attribute

# Using the API, how many Falcon 9 launches are there after we remove Falcon 1 launches?  90
# At the end of the API data collection process, how many missing values are there for the column landingPad? - 26
# print("df.isnull().sum()/len(df)*100")
# # print(df.isnull().sum()/len(df)*100)
# print(df.isnull().sum())


# spacex_url="https://api.spacexdata.com/v4/launches/past"
# response = requests.get(spacex_url,verify='panw.pem')

# print(response.content[0:100])

# static_json_url='https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DS0321EN-SkillsNetwork/datasets/API_call_spacex_api.json'
# response = requests.get(static_json_url,verify='panw.pem')
# # Get the head of the dataframe
# data = pd.json_normalize(response.json())
# df = pd.DataFrame(data)
# print(df.head(2))
# print(df.columns)
# # df = data[data['launchpad']]
# # print("df.head()")
# # print(df)
# # print(df.head())
# # print(data['launchpad'].isna())
# print(df['launchpad'])
# print(df['launchpad'].isnull())

# print(df['name'].head())
# # falcon9 = df[df.applymap(lambda x: isinstance(x, str) and 'falcon' in x).any(axis=1)]
# # falcon9 = df['name'].str.contains('falcon').sum()
# falcon9 = df['name'].str.contains('Falcon')
# falcon9_desc = df['name'].str.contains('Falcon')
      
# # falcon9 = df["falcon" in df['name']]
# print("\n\nfalcon9")
# print(falcon9)
# print(falcon9.sum())
