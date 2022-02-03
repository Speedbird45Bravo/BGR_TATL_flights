from src import Arrivals, Departures
from gs import df, df_ws, init_len
from dicts import ac_dict, al_dict, apc_dict, code_dict
from datetime import datetime as dt
from sklearn.preprocessing import LabelEncoder
from imblearn.ensemble import BalancedRandomForestClassifier
from gspread_dataframe import set_with_dataframe
import re
import pandas as pd
import warnings
import time
warnings.filterwarnings("ignore")

now = dt.now()

print(f"Program run at {now}.")

# The main objective for the program is to automate the addition of flights to an existing spreadsheet.
# Previously, there were nine (reduced to seven with datetime and ML, respectively) fields that had to be filled out per observation:
# 
# 	# Date (if none, current datetime date entered)
# 	# Airline (if applicable)
#	# Flight # (if applicable)
# 	# Aircraft type (required)
# 	# Origin IATA code
# 	# Origin Country
# 	# Destination IATA code
# 	# Destination Country
# 	# Direction (predicted by ML)
# 
# With this program, we will scrape the FlightAware API to see if there are any new transatlantic flights to add to our spreadsheet.
# If there are no flights to add, the program exits and nothing changes.
# If there are flights to add, it will format them properly by (future plans after "EVENTUALLY" if applicable):
#    	
# 	# Reducing the departure/arrival times to strf dates
#	# Splitting the ICAO identifier into an Airline and Flight # (if applicable)
#  	# Recording aircraft type
#  	# Converting ICAO codes to IATA codes via dictionary
# 	# Adding origin and destination countries (derived from IATA codes via dictionary)
# 	# Predicting direction based on origin country (e.g. If flight originates in USA, it's "E" for East; otherwise, "W" for West.)
#
#   # In the interest of cardinality, we have also added an ID field to provide a singular field that can function as a primary key.

arr = Arrivals().df
dep = Departures().df
# RIDT added 21:29 1/15/2022
bgr = arr.append(dep).reset_index(drop=True)

# When formatted as a datetime, the actual arrival date is often listed as 1-1-1970 if there is no actualarrivaltime yet (a value of 0). 
# If that is the case we will just default to the estimatedarrivaltime.
bgr['arrivaltime'] = [bgr['estimatedarrivaltime'] if 0 else i for i in bgr['actualarrivaltime']]
bgr = bgr.drop(['estimatedarrivaltime','actualarrivaltime'], axis=1)

# Here we will capture the identifiers.
idents_a = []
idents_b = []

# This grabs the airline (ICAO).
for col in bgr['ident']:
    idents_a.append(re.split("(\d+)", col)[0])

# Excluding singular letter-based identifiers without any numeric values, we'll be able to grab most flight numbers here.
for col in bgr['ident']:
    try:
        idents_b.append(re.split("(\d+)", col)[1])
    except IndexError:
        idents_b.append("None")

# Converting the airline ICAO codes from the API pull to a dictionary of codes listed as they are in the spreadsheet.
bgr['Airline_SYM'] = pd.Series(idents_a)
bgr['Airline_SYM'].fillna("None", inplace=True)
bgr['Airline'] = bgr['Airline_SYM'].map(al_dict)
bgr['Flight'] = idents_b
bgr['Type'] = bgr['aircrafttype'].map(ac_dict)
bgr = bgr.drop(["aircrafttype","ident"], axis=1)
bgr['Origin'] = bgr['origin'].str.strip()
bgr['Destination'] = bgr['destination'].str.strip()

# We want to filter out flights that are entirely arriving and departing from the US (starting with "K"), Canada (starting with "C"), Mexico (starting with "M"), and
# Greenland (starting with "BG"). There are other possible airports outside of these but most can be dealt with ad hoc.
# "BG" and " " on bgr['Type'][0] checks 12:10 12/31/2021.
# "TJ" checks on bgr['Origin'].str[0:2] 1/3/2022
# str.len() == 4 checks on ['Origin'] and ['Destination'] 1/7/2022
bgr = bgr[(((bgr['Origin'].str[0] != "K") & (bgr['Origin'].str[0] != "C") & (bgr['Origin'].str[1] != " ") & (bgr['Origin'].str[0] != "M") & (bgr['Origin'].str[0:2] != "BG") & (bgr['Origin'].str[0:2] != "TJ")) | ((bgr['Destination'].str[0] != "K") & (bgr['Destination'].str[0] != "C") & (bgr['Destination'].str[1] != " ") & (bgr['Destination'].str[0] != "M") & (bgr['Origin'].str[0:2] != "BG") & (bgr['Origin'].str[0:2] != "TJ"))) & (bgr['Origin'].str.len() == 4) & (bgr['Destination'].str.len() == 4) & (bgr['Type'].str[0] != " ")]

# Drop the null aircraft 1/8/2022
bgr = bgr[bgr['Type'].notna()]

# Mapping the origin and destination from ICAO (4-letter) codes to IATA (3-letter) codes.
bgr['Origin'] = bgr['Origin'].map(code_dict)
bgr['Destination'] = bgr['Destination'].map(code_dict)

# Mapping the IATA codes to countries of origin and destination.
bgr['Origin Country'] = bgr['Origin'].map(apc_dict)
bgr['Destination Country'] = bgr['Destination'].map(apc_dict)

# Condensing the timestamp into a strftime. Since we need to choose a singular date field, we will go with the actual departure time (in BGR time). There may be some anomalies
# in terms of the rare westbound flight that left Europe before midnight and landed after midnight, but those are few and far between and are not germane to the overall
# records.
bgr["Date"] = bgr["actualdeparturetime"].apply(lambda x: dt.fromtimestamp(x).strftime("%Y-%m-%d"))

# Getting rid of the unnecessary columns.
bgr = bgr.drop(["actualdeparturetime","origin","destination","originName","destinationName","originCity","destinationCity","arrivaltime"], axis=1)

# If there are no flights to add, the program exits.
if len(bgr) == 0:
    # Sleep logic added 9:42 1/15/2022
    print("No flights to add.")
    time.sleep(6)
    exit()
else:
    pass

# No medical flights.
bgr = bgr[(bgr['Flight'] != "901") | (bgr['Airline'] != "N")]

# ID serialization.
bgr['ID'] = bgr['Date'].astype(str) + bgr['Airline_SYM'].astype(str) + bgr['Flight'].astype(str)
bgr['ID'] = bgr['ID'].str.replace("-", "")
bgr['ID'] = bgr['ID'].str[2:]
bgr['ID'] = bgr['ID'].str.replace("nan", "")
# Replacing None flight numbers with nothing. 1/5/2022
bgr['ID'] = bgr['ID'].str.replace("None","")
bgr['Flight'] = bgr['Flight'].str.replace("None","")

# ML model to predict Direction based on Origin Country.
X, y = df[['Origin Country']], df['Direction']

le = LabelEncoder()

X = le.fit_transform(X.astype(str)).reshape(-1,1)

if len(bgr) != 1:
    X_test = le.transform(bgr['Origin Country'].astype(str)).reshape(-1,1)
else:
    X_test = le.transform(bgr['Origin Country'].astype(str)).reshape(1,-1)

# The Scikit-learn RF Classifier oversamples westbound flights so the imbalanced-learn RF classifier does a better job overall.
model = BalancedRandomForestClassifier()

model.fit(X, y)

predictions = model.predict(X_test)

bgr['Direction'] = predictions

bgr = bgr[['ID','Date','Airline','Flight','Type','Origin','Origin Country','Destination','Destination Country','Direction']]

# Drop duplicate code chained 13:01 1/1/2022
df = df.append(bgr).sort_values(by=['Date']).reset_index(drop=True).drop_duplicates(subset=['ID'])

if init_len == len(df):
    print("No flights added. Program exiting.")
else:
    print(f"{len(df) - init_len} flights added. {len(df)} flights total")

# Last fix 18:30 12/29/2021
set_with_dataframe(df_ws, df)

# Sleep logic 11:34 1/8/2022
time.sleep(6)

exit()
