import pandas as pd
import os

os.chdir("C:\\Users\\Henry\\Desktop\\BGR_TATL_flights\\Data")

cols = ['FA','SS']

ac = pd.read_csv("AC.csv", names=cols)
ac_dict = dict(zip(ac['FA'],ac['SS']))

al = pd.read_csv("AL.csv", names=cols)
al_dict = dict(zip(al['FA'],al['SS']))

codes = pd.read_csv("ICAOIATA.csv", names=cols)
code_dict = dict(zip(codes['FA'],codes['SS']))

apc = pd.read_csv("APC.csv", names=['IATA','Country'])
apc_dict = dict(zip(apc['IATA'],apc['Country']))
