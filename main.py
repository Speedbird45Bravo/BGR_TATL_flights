from src import Arrivals, Departures
from gs import df, df_ws, init_len
from dicts import ac_dict, al_dict, apc_dict, code_dict
from datetime import datetime as dt
from gspread_dataframe import set_with_dataframe
from sys import exit
import re
import pandas as pd
import warnings
import time

warnings.filterwarnings("ignore")

now = dt.now()

# pep8 maximum line Length compliant 6/6/2022

print(f"Flights pulled from FlightAware API query at {now}.")

# The main objective for the program is to automate the addition
# of flights to an existing spreadsheet.
# Previously, there were nine fields to be filled out per observation:
#
# 	# Date (if none, current datetime date entered)
# 	# Airline (if applicable)
#	# Flight # (if applicable)
# 	# Aircraft type (required)
# 	# Origin IATA code
# 	# Origin Country
# 	# Destination IATA code
# 	# Destination Country
# 	# Direction
#
# With this program, we will scrape the FlightAware API to see
# if there are any new transatlantic flights to add to our spreadsheet.
# If there are no flights to add, the program exits and nothing changes.
# If there are flights to add, it will format them properly by:
#
# 	# Reducing the departure/arrival times to strf dates
#	# Splitting the ICAO identifier into an Airline and Flight # (if applicable)
#  	# Recording aircraft type
#  	# Converting ICAO codes to IATA codes via dictionary
# 	# Adding origin and destination countries (derived from IATA codes
#   # via dictionary)
# 	# Direction based on origin country (e.g. If flight
# 	# in USA, it's "E" for East; otherwise, "W" for West.)
#
#   # In the interest of cardinality, we have also added an
#   ID field to provide a singular field that can function as a primary key.

# arr and dep changed to arrivals and departures 2/24/2022
# Dictionaries to convert actualarrivaltime and actualdeparturetime
# to "Date" added 2/24/2022
arrivals = Arrivals().df
arrivals = arrivals[['actualarrivaltime','ident','aircrafttype',\
                     'origin','destination']]\
                    .rename(columns={"actualarrivaltime":"Date",\
                                     "aircrafttype":"Type"})
departures = Departures().df
departures = departures[['actualdeparturetime','ident',\
    'aircrafttype','origin','destination']]\
    .rename(columns={"actualdeparturetime":"Date",\
    "aircrafttype":"Type"})

# RIDT added 21:29 1/15/2022
# Renaming of columns added 2/24/2022
bgr = arrivals.append(departures).reset_index(drop=True)\
    .rename(columns={"origin":"Origin", "destination":"Destination"})

# First thing we will do now that we have the arrivals and departures stacked
# is pull the date. 2/24/2022

bgr['Date'] = bgr['Date'].apply(lambda x:\
    dt.fromtimestamp(x).strftime("%Y-%m-%d"))

# Now that we have our arrival and departures together (for a given pull),
# we will capture the identifiers.
idents_a = []
idents_b = []

# This grabs the airline (ICAO).
for col in bgr['ident']:
    idents_a.append(re.split("(\d+)", col)[0])

# Excluding singular letter-based identifiers without any numeric values,
# we'll be able to grab most flight numbers here.
for col in bgr['ident']:
    try:
        idents_b.append(re.split("(\d+)", col)[1])
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
bgr['Type'].loc[bgr['Airline']=="US Navy"] = "Boeing 737-700"
bgr['Type'].loc[bgr['Airline']=="US Air Force"] = "Boeing C-17 Globemaster"

# We want to filter out flights that are entirely arriving and departing
# from the US (starting with "K"), Canada (starting with "C"), Mexico
# (starting with "M"), and
# Greenland (starting with "BG"). There are other possible airports
# outside of these but most can be dealt with ad hoc.
# "BG" and " " on bgr['Type'][0] checks 12:10 12/31/2021.
# "T" checks on bgr['Origin'][0] 4/23/2022
# str.len() == 4 checks on ['Origin'] and ['Destination'] 1/7/2022
bgr = bgr[(((bgr['Origin'].str[0] != "K") & (bgr['Origin'].str[0]\
    != "C") & (bgr['Origin'].str[1] != " ") & (bgr['Origin']\
    .str[0] != "M") & (bgr['Origin'].str[0:2] != "BG") \
    & (bgr['Origin'].str[0] != "T")) | ((bgr['Destination'].str[0]\
    != "K") & (bgr['Destination'].str[0] != "C") & (bgr['Destination']\
    .str[1] != " ") & (bgr['Destination'].str[0] != "M") & \
    (bgr['Destination'].str[0:2] != "BG") & (bgr['Destination']\
    .str[0] != "T"))) & (bgr['Origin'].str.len() == 4) & \
    (bgr['Destination'].str.len() == 4) & (bgr['Type'].str[0] != " ")]

# Drop the null aircraft 1/8/2022
bgr = bgr[bgr['Type'].notna()]

# Mapping the origin and destination from ICAO (4-letter) codes to IATA
# (3-letter) codes.
bgr['Origin'] = bgr['Origin'].map(code_dict)
bgr['Destination'] = bgr['Destination'].map(code_dict)

# Mapping the IATA codes to countries of origin and destination.
bgr['Origin Country'] = bgr['Origin'].map(apc_dict)
bgr['Destination Country'] = bgr['Destination'].map(apc_dict)

# ID serialization.
bgr['ID'] = bgr['Date'].astype(str) + bgr['Airline_SYM'].astype(str) \
    + bgr['Flight'].astype(str)
bgr['ID'] = bgr['ID'].str.replace("-", "")
bgr['ID'] = bgr['ID'].str[2:]
bgr['ID'] = bgr['ID'].str.replace("nan", "")
# Replacing None flight numbers with nothing. 1/5/2022
bgr['ID'] = bgr['ID'].str.replace("None","")
bgr['Flight'] = bgr['Flight'].str.replace("None","")

# Dropping ident and Airline_SYM 2/24/2022
bgr = bgr.drop(columns=["ident","Airline_SYM"])

# Reordering columns 2/24/2022
bgr = bgr[['ID','Date','Airline','Flight','Type',\
    'Origin','Origin Country','Destination','Destination Country']]

# End string adjusted 5/23/2022
end_string = "No flights to be added. Program exiting."

# No medical flights.
bgr = bgr[(bgr['Flight'] != "901") | (bgr['Airline'] != "N")]

directions = []

# Direction logic 4/28/2022
for origin in bgr['Origin Country']:
    if origin == "USA":
        directions.append("E")
    else:
        directions.append("W")

bgr['Direction'] = directions

bgr = bgr[['ID','Date','Airline','Flight','Type','Origin'\
    ,'Origin Country','Destination','Destination Country','Direction']]

bgr_len = len(bgr)

# If there are no flights to add, the program exits.
# Order adjusted 5/27/2022
if len(bgr) != 0:
    pass
else:
    print(end_string)
    # end_string position adjusted 4/9/2022
    # Sleep logic added 9:42 1/15/2022
    time.sleep(6)
    exit()

# Drop duplicate code chained 13:01 1/1/2022
df = df.append(bgr)

# Sort values isolated 10:34 2/5/2022
df = df.sort_values(by=['Date']).reset_index(drop=True)\
    .drop_duplicates(subset=['ID'])

# Adj 5/27/2022
df_end_len = len(df)

# len calcs and logic inserted 5/25/2022
if init_len == df_end_len:
    print(end_string)
    time.sleep(4)
    exit()
else:
    # Converted from insights to pass 5/28/2022
    pass

# bgr_final created 5/28/2022
bgr_final = bgr[['Type','Origin','Destination']]

# Print flights logic created 5/26/2022, amended 5/28/2022, replaced 6/4/2022
print(f"{bgr_len} flights added. {df_end_len} flights total")
print()
print("Flight(s) Added:")
print()
print(bgr_final)

# Last fix 18:30 12/29/2021
set_with_dataframe(df_ws, df)

# Sleep logic set to 4 19:02 2/12/2022
time.sleep(4)

# Exit given sys import for structural integrity 6/18/2022
exit()
