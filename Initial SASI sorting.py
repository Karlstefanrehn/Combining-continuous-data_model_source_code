## Initial SASI sorting 


#Imports 
from types import LambdaType
from geopandas import geodataframe
from geopandas.geodataframe import _dataframe_set_geometry
from geopandas.plotting import plot_dataframe
import numpy as np
from numpy.lib.arraysetops import isin
from pandas.core.indexes.numeric import Int64Index
from scipy.integrate import odeint
from scipy.integrate import quad
import pandas as pd
import geopandas as gpd  
import os 
import fiona as fiona 

import matplotlib.pyplot as plt
from scipy.optimize import fsolve
import warnings

def imports_csv():
    
    '''
  
    '''
    
    global df_1
    global df_2
    
    df_1 = pd.read_csv('/Users/rehnan/Box/bridge_scr_collect_data/soil monitoring data/drev1_csv.csv', sep= ';')
    df_2 = pd.read_csv('/Users/rehnan/Box/bridge_scr_collect_data/soil monitoring data/drev2_csv.csv', sep= ';')
    
    columns1 = [ 'lan','kokkun','fingerprint', 'drev','s','n',]
    columns2 = ['y','lan','kokkun','fingerprint','mjal','drev','soc_clay_2','c/n','orgmant', 'sand','mo','silt','ler','finsand','silt','jordart', 'drev','s','n',]
    df_1.drop(columns1, inplace=True, axis=1)
    df_2.drop(columns2, inplace=True, axis=1)
    
    
    
    print(df_1)
    print(df_2)
#imports_csv()
def merging_datasets ():
    
    # looking at the data for best possible merge
    
    # Unique values in sample ID 
    # Unique values in x - koordinate 
    # unique values in mvn ID 
    
    # counting unique values
    
    n = len(pd.unique(df_2['id']))
    n2 = len(pd.unique(df_2['mvn_id']))
    n3 = len(pd.unique(df_2['x']))
    print("No.of.unique values :",n)
    print("No.of.unique values :",n2)
    print("No.of.unique values :",n3)

    
    df_merged = pd.merge(left=df_1 , right = df_2, how= 'left', left_on = 'x', right_on= 'x')
    
    #df_merged.to_csv('/Users/rehnan/Box/bridge_scr_collect_data/soil monitoring data/drev_both_koord_.csv', sep= ';')
    
    print(df_merged)
#merging_datasets()

def data_calc():
    
    carbondata = pd.read_csv('/Users/rehnan/Box/bridge_scr_collect_data/soil monitoring data/clean_data/full_soildata_both.csv', sep= ';')
    
    print(carbondata.head())
    
    columns = [ 'sand','mo', 'mjal','silt','ler','finsand','jordart']
    carbondata.drop(columns, inplace=True, axis=1)
    
    carbondata['c_change'] = carbondata['c_2'] - carbondata['c_1']
    
    print(carbondata)
    
    carbondata.to_csv('/Users/rehnan/Box/bridge_scr_collect_data/soil monitoring data/carbondata_national.csv', sep= ';')
    

data_calc()
