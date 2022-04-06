import requests
import pandas as pd

user = "#user#"
key = "#key#"
payload = {"airport":"KBGR", "howMany":15}
url = "https://flightxml.flightaware.com/json/FlightXML2/"

class Arrivals:
    
    """Returns an arrival object."""
    
    def __init__(self):
        self.user = user
        self.key = key
        self.payload = payload
        self.url = url
        self.req = requests.get(url + "Arrived", params=self.payload, auth=(self.user,self.key)).json()
        self.df = pd.DataFrame(self.req['ArrivedResult']['arrivals'])

class Departures:
    
    """Returns a departure object."""

    def __init__(self):
        self.user = user
        self.key = key
        self.payload = payload
        self.url = url
        self.req = requests.get(url + "Departed", params=self.payload, auth=(self.user,self.key)).json()
        self.df = pd.DataFrame(self.req['DepartedResult']['departures'])
