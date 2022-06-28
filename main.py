from gspread_dataframe import set_with_dataframe
from datetime import datetime as dt
from sys import exit
from pytz import timezone
import warnings
import requests
import re
import gspread
import pandas as pd
import time
import airportsdata

# P1 - bgr_tatl_flights created 6/23/2022 for PythonAnywhere deployment

warnings.filterwarnings("ignore")

# Two CSVs that we import as dictionaries, essentially.
# One maps airline ICAO identifiers (e.g. BAW for British Airways)
# to airline names.
# Another maps aircraft ICAO identifiers (e.g. B744 for Boeing 747-400)
# to aircraft names.
cols = ['FA', 'SS']

ac = pd.read_csv("AC.csv", names=cols)
ac_dict = dict(zip(ac['FA'], ac['SS']))

al = pd.read_csv("AL.csv", names=cols)
al_dict = dict(zip(al['FA'], al['SS']))

# Loading the spreadsheet.
gc = gspread.service_account("bgr-tatl-flights-1cf26373c7fd.json")
df_sheet = gc.open("BGR_TATL_flights")
df_worksheet = df_sheet.get_worksheet(0)
df = pd.DataFrame(df_worksheet.get_all_values())
df.columns = df.iloc[0, :]
df = df.iloc[1:, :]
init_len = len(df)

user = "hloughlin9"
key = "5fb2d8528088de31addbf004f20248875e3a804d"
payload = {"airport": "KBGR", "howMany": 15}
url = "https://flightxml.flightaware.com/json/FlightXML2/"


class Arrivals:
    """Returns an arrival object."""

    def __init__(self):
        self.user = user
        self.key = key
        self.payload = payload
        self.url = url
        self.req = requests.get(url + "Arrived", params=self.payload,
                                auth=(self.user, self.key)).json()
        self.df = pd.DataFrame(self.req['ArrivedResult']['arrivals'])


class Departures:
    """Returns a departure object."""

    def __init__(self):
        self.user = user
        self.key = key
        self.payload = payload
        self.url = url
        self.req = requests.get(url + "Departed", params=self.payload,
                                auth=(self.user, self.key)).json()
        self.df = pd.DataFrame(self.req['DepartedResult']['departures'])


# IMPORTANT NOTE #
# This is a modified version of #
# main.py. #


warnings.filterwarnings("ignore")

et = timezone("US/Eastern")

now = dt.now(tz=et)

# STR capped at 19 (YYYY-MM-DD HH:MM:SS). 6/27/2022
print(f"Flights pulled from FlightAware API query at {str(now)[:19]}.")

# The main objective for the program is to automate the addition
# of flights to an existing spreadsheet.
# Previously, there were nine fields to be filled out per observation:
# Date (if none, current datetime date entered)
# Airline (if applicable)
# Flight # (if applicable)
# Aircraft type (required)
# Origin IATA code
# Origin Country
# Destination IATA code
# Destination Country
# Direction
#
# With this program, we will scrape the FlightAware API to see
# if there are any new transatlantic flights to add to our spreadsheet.
# If there are no flights to add, the program exits and nothing changes.
# If there are flights to add, it will format them properly by:
# Reducing the departure/arrival times to strf dates
# Splitting the ICAO identifier into an Airline and Flight # (if applicable)
# Recording aircraft type
# Converting ICAO codes to IATA codes via dictionary
# Adding origin and destination countries (derived from IATA codes
# via dictionary)
# Direction based on origin country (e.g. If flight
# in US, it's "E" for East; otherwise, "W" for West.)
# In the interest of cardinality, we have also added an
# ID field to provide a singular field that can function as a primary key.

# arr and dep changed to arrivals and departures 2/24/2022
# Dictionaries to convert actualarrivaltime and actualdeparturetime
# to "Date" added 2/24/2022
# keeps added 6/23/2022
keeps = ['actualarrivaltime', 'ident', 'aircrafttype', 'origin', 'destination']

arrivals = Arrivals().df
arrivals = arrivals[['actualarrivaltime', 'ident', 'aircrafttype',
                     'origin', 'destination']] \
    .rename(columns={"actualarrivaltime": "Date", "aircrafttype": "Type",
                     "origin": "Origin", "destination": "Destination"})

departures = Departures().df
departures = departures[['actualdeparturetime', 'ident',
                         'aircrafttype', 'origin', 'destination']] \
    .rename(columns={"actualdeparturetime": "Date",
                     "aircrafttype": "Type", "origin": "Origin",
                     "destination": "Destination"})

# This combines the arrivals and departures into our single spreadsheet of
# new flights to add.
# reset_index appended 1/15/2022

bgr = arrivals.append(departures).reset_index(drop=True)

# str.len() == 4 checks on ['Origin'] and ['Destination'] 1/7/2022
# This ensures that our ICAO identifiers all have 4 letters.

bgr = bgr[(bgr['Origin'].str.len() == 4) &
          (bgr['Destination'].str.len() == 4)]

# Another quick filter (divided for readability) filtering out airport
# identifiers with digits at the start or the end.
# Any with digits in the middle will automatically be filtered out,
# but we don't want any random 4-letter identifiers (e.g. 46ME or ME33)
# being in there.

bgr = bgr[(bgr['Origin'].str[0] != str(bgr['Origin'].str[0]).isdigit()) &
          (bgr['Destination'].str[0] != str(bgr['Destination'].str[0]).isdigit()) &
          (bgr['Origin'].str[3] != str(bgr['Origin'].str[3]).isdigit()) &
          (bgr['Destination'].str[3] != str(bgr['Destination'].str[3]).isdigit())]

# We want to filter out flights that are entirely arriving and departing
# from the US (starting with "K"), Canada (starting with "C"), Mexico
# (starting with "M"), and
# Greenland (starting with "BG"). There are other possible airports
# but most can be dealt with ad hoc.
# "BG" and " " on bgr['Type'][0] checks 12:10 12/31/2021. (Greenland)
# "T" checks on bgr['Origin'][0] 4/23/2022 (Carribean/)
bgr = bgr[(((bgr['Origin'].str[0] != "K") & (bgr['Origin'].str[0] != "C")
            & (bgr['Origin'].str[1] != " ") & (bgr['Origin'].str[0] != "M") &
            (bgr['Origin'].str[0:2] != "BG") & (bgr['Origin'].str[0] != "T"))
           | ((bgr['Destination'].str[0] != "K") &
              (bgr['Destination'].str[0] != "C") &
              (bgr['Destination'].str[1] != " ") &
              (bgr['Destination'].str[0] != "M") &
              (bgr['Destination'].str[0:2] != "BG") &
              (bgr['Destination'].str[0] != "T")))]

# All airport data, pulled by ICAO code.
icaos = airportsdata.load()

# The ICAO-IATA and ICAO-country maps via airportsdata necessitated two fewer
# dictionaries than before. 6/25/2022
bgr['Origin Country'] = bgr['Origin'].apply(lambda x: icaos[x]['country'])
bgr['Destination Country'] = bgr['Destination'].apply(lambda x: icaos[x]['country'])

# Scan ICAOs to confirm they have an IATA counterpart, else use the ICAO.
for i in bgr['Origin']:
    j = icaos[i]['iata']
    if j == '':
        bgr['Origin'] = bgr['Origin']
    else:
        bgr['Origin'] = icaos[i]['iata']

for i in bgr['Destination']:
    j = icaos[i]['iata']
    if j == '':
        bgr['Destination'] = bgr['Destination']
    else:
        bgr['Destination'] = icaos[i]['iata']

bgr['Origin'] = bgr['Origin'].apply(lambda x: icaos[x]['iata'])
bgr['Destination'] = bgr['Destination'].apply(lambda x: icaos[x]['iata'])

# First thing we will do now that we have the arrivals and departures stacked
# is pull the date. 2/24/2022
# fromtimestamp(x) given tz 6/24/2022.
bgr['Date'] = bgr['Date'].apply(lambda x: dt.fromtimestamp(x, tz=et).strftime("%Y-%m-%d"))

# Now that we have our arrival and departures together (for a given pull),
# we will capture the identifiers.
idents_a = []
idents_b = []

# This grabs the airline (ICAO).
for col in bgr['ident']:
    idents_a.append(re.split("(\\d+)", col)[0])

# Excluding singular letter-based identifiers without any numeric values,
# we'll be able to grab most flight numbers here.
for col in bgr['ident']:
    try:
        idents_b.append(re.split("(\\d+)", col)[1])
    except IndexError:
        idents_b.append("None")

# Converting the airline ICAO codes from the API pull to a dictionary of
# codes listed as they are in the spreadsheet.
bgr['Airline_SYM'] = pd.Series(idents_a)
bgr['Airline_SYM'].fillna("None", inplace=True)
bgr['Airline'] = bgr['Airline_SYM'].map(al_dict)
bgr['Flight'] = idents_b
bgr['Type'] = bgr['Type'].map(ac_dict)

# Navy/USAF logic included 6/11/2022. There may be the odd C-130 or C-5
# in the case of USAF but that is OK and can be dealt with ad hoc during
# ongoing validation.
bgr['Type'].loc[bgr['Airline'] == "US Navy"] = "Boeing 737-700"
bgr['Type'].loc[bgr['Airline'] == "US Air Force"] = "Boeing C-17 Globemaster"

# No medical flights.
# (The identifiers for Maine's LifeFlights usually start with "N901.")
bgr = bgr[(bgr['Flight'] != "901") | (bgr['Airline'] != "N")]

# Drop the null aircraft 1/8/2022
bgr = bgr[bgr['Type'].notna()]

# ID serialization.
bgr['ID'] = bgr['Date'].astype(str) + bgr['Airline_SYM'].astype(str) \
            + bgr['Flight'].astype(str)
bgr['ID'] = bgr['ID'].str.replace("-", "")
bgr['ID'] = bgr['ID'].str[2:]
bgr['ID'] = bgr['ID'].str.replace("nan", "")
# Replacing None flight numbers with nothing. 1/5/2022
bgr['ID'] = bgr['ID'].str.replace("None", "")
bgr['Flight'] = bgr['Flight'].str.replace("None", "")

# Dropping ident and Airline_SYM 2/24/2022
bgr = bgr.drop(columns=["ident", "Airline_SYM"])

ordered = ['ID', 'Date', 'Airline', 'Flight', 'Type',
           'Origin', 'Origin Country', 'Destination', 'Destination Country']

# Reordering columns 2/24/2022
bgr = bgr[ordered]

directions = []

# Direction logic 4/28/2022
for origin in bgr['Origin Country']:
    if origin == "US":
        directions.append("E")
    else:
        directions.append("W")

bgr['Direction'] = directions

bgr = bgr[['ID', 'Date', 'Airline', 'Flight', 'Type', 'Origin',
           'Origin Country', 'Destination', 'Destination Country', 'Direction']]

# Previous flights added 6/27/2022
prev_flights = set(df['ID'])

bgr_len = len(bgr)

# Drop duplicate code chained 13:01 1/1/2022
df = df.append(bgr)

# Sort values isolated 10:34 2/5/2022
df = df.sort_values(by=['Date']).reset_index(drop=True) \
    .drop_duplicates(subset=['ID'])

# Adj 5/27/2022
df_end_len = len(df)

# Bool length calc rebuilt 6/25/2022
if init_len == df_end_len:
    print("No flights to add. Program exiting.")
    time.sleep(4)
    exit()
else:
    pass

# Logic to include only new flights 6/27/2022
bgr = bgr[~bgr['ID'].isin(prev_flights)]

# bgr_final created 5/28/2022, amended 6/25/2022
bgr_final = bgr[['ID', 'Date', 'Type', 'Origin', 'Destination']]

# Print flights logic created 5/26/2022, amended 5/28/2022, replaced 6/4/2022
print(f"{bgr_len} flights added. {df_end_len} flights total")
print()
print("Flight(s) Added:")
print()
print(bgr_final)

# Set the worksheet as the new version.
set_with_dataframe(df_worksheet, df)

# Sleep logic set to 4 19:02 2/12/2022
time.sleep(4)

# Exit given sys import for structural integrity 6/18/2022
exit()
