#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 31 13:03:31 2020

@author: nathanbalson
"""


import pandas as pd
from pandas import DataFrame as df
import numpy as np
import os.path
from datetime import datetime




def readin_historical(last_season_to_include):
    print("readin_historical")
    years = [i for i in range(1999,(last_season_to_include +1))]
    data = pd.DataFrame()
    
    for i in years:  
        print(i)
        #low_memory=False eliminates a warning
        i_data = pd.read_csv('https://github.com/guga31bb/nflfastR-data/blob/master/data/' \
                             'play_by_play_' + str(i) + '.csv.gz?raw=True',
                             compression='gzip', low_memory=False)
        data = data.append(i_data)
    data.set_index(['game_id','play_id'], inplace =True)
    return data


def readin_current_year(year):
    print("running readin_current_year")
    data = pd.DataFrame()
    # year = create_years()[-1]
    #low_memory=False eliminates a warning
    data = pd.read_csv('https://github.com/guga31bb/nflfastR-data/blob/master/data/' \
                      'play_by_play_' + str(year) + '.csv.gz?raw=True',
                      compression='gzip', low_memory=False)
    data.set_index(['game_id','play_id'], inplace =True)
    return data
    
def readin_full_data(last_season_to_include,Include_2020):
    print("running readin_full_data")
    data = pd.DataFrame()
    years = [i for i in range(1999,(last_season_to_include +1))]
    for i in years:
        if (('y' in Include_2020) | ('Y' in Include_2020)) :
            print(i)
            #low_memory=False eliminates a warning
            i_data = pd.read_csv('https://github.com/guga31bb/nflfastR-data/blob/master/data/' \
                              'play_by_play_' + str(i) + '.csv.gz?raw=True',
                              compression='gzip', low_memory=False)
            data = data.append(i_data)
        else:
            if i != 2020:
                print(i)
                #low_memory=False eliminates a warning
                i_data = pd.read_csv('https://github.com/guga31bb/nflfastR-data/blob/master/data/' \
                                  'play_by_play_' + str(i) + '.csv.gz?raw=True',
                                  compression='gzip', low_memory=False)
                data = data.append(i_data)
    data.set_index(['game_id','play_id'], inplace =True)
    return data
       

def export_historical_to_csv(last_season_to_include):
    print("running export_historical_to_csv")
    clean_data_folder = '/Users/nathanbalson/Desktop/Data Science Projects/datasets/NFL/Clean/'
    data = readin_historical(last_season_to_include)
    data.to_csv(clean_data_folder + "NFl Plays 1999-" + str(last_season_to_include) + ".csv")
    return 

def export_current_to_csv(year):
    print("running export_current_to_csv")
    clean_data_folder = '/Users/nathanbalson/Desktop/Data Science Projects/datasets/NFL/Clean/'
    data = readin_current_year(year)
    data.to_csv(clean_data_folder + "NFl Plays " + str(year) + ".csv")
    return
    

