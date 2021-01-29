#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Oct 18 16:49:18 2020

@author: nathanbalson
"""
############################
 # to do:
 #    - fix "drive ended with score" in df_drive which is currently a 1 or 0 for the wrong possesion team


# to add:
#   - avg yac should be per play -- currently either a sum or per game
#   - cntrl f for " to do: here "  to find where to create added stats by drive
#   - pct of plays pass and pct of plays run when in the lead and when behind
#   - add time spent in the lead and time spent trailing


import pandas as pd
from pandas import DataFrame as df
import numpy as np
import os.path
from datetime import datetime 
from time import strptime, strftime



clean_data_folder = str("C:\\Users\\nateb\\OneDrive\\Desktop\\Data Science Projects\\datasets\\NFL\\Clean")

def remove_unused_vars(df):
    remove = []
    for i in range(0,len(df.columns)):
        if (( 160 <= i <= 245 ) or ('xyac' in df.columns[i]) or ('wpa' in df.columns[i])):
            remove.append(df.columns[i])    
    df2 = df.drop(remove,axis=1)
    return df2


# there are still plays classified as a pass or run when there is a penalty - remove and calculate separately
def create_penalties_df(df):
    print("running create_penalties_df")
    df_penalties = df[df['penalty'] == 1].copy()  
    df_penalties['home_penalty'] = df_penalties.apply(lambda x: 1  if x['penalty_team'] == x['home_team'] else 0, axis = 1)
    df_penalties['away_penalty'] = df_penalties.apply(lambda x: 1  if x['penalty_team'] != x['home_team'] else 0, axis = 1)   
    #sum up penalties by game
    df_penalties = df_penalties.groupby(['game_id','home_team', 'away_team', ]).agg({'home_penalty':'sum','away_penalty':'sum'})
    return  df_penalties

def create_drive_df(df):
    print("running create_drive_df")
    df = df[df['play_type'] != 'no play' ] 
    # looks like inside 20 is also for inside own 20 - create redzone variable that is only for opposing team side of field   
    df['home_redzone'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' and x['drive_inside20'] == 1 and x['side_of_field'] != x['posteam'] else 0, axis = 1 )
    df['away_redzone'] = df.apply(lambda x: 1 if x['posteam_type'] == 'away' and x['drive_inside20'] == 1 and x['side_of_field'] != x['posteam'] else 0, axis = 1 )    
    df['home_td_redzone'] = df.apply(lambda x: 1 if x['home_redzone'] == 1 and x['touchdown'] == 1 and x['td_team'] == x['posteam']  else (0 if x['home_redzone'] == 1 else np.nan), axis = 1 )
    df['away_td_redzone'] = df.apply(lambda x: 1 if x['away_redzone'] == 1 and x['touchdown'] == 1 and x['td_team'] == x['posteam']  else (0 if x['away_redzone'] == 1 else np.nan), axis = 1 )
      
    # get time of possession by drive - then sum up
    df_drive = df.copy()
    #  time of possession is a string - need to make it a time variable before summing up
    df_drive_new = df_drive.reset_index().set_index(['game_id','play_id','drive'])
    df_drive_new2 = df_drive_new[~df_drive_new.index.duplicated(keep='first')]
    df_drive_new3 = df_drive_new2.copy()
    df_drive_new3['home_top'] = df_drive_new2.apply(lambda x: x['drive_time_of_possession'] if x['posteam_type'] == 'home' else np.nan,axis = 1)

    df_drive_new3['home_top_sec'] = df_drive_new3.home_top.apply(lambda x: int(x[:x.index(':')])*60 + int(x[x.index(':')+1:]) if type(x) == str else 0)
    df_drive_new3['away_top'] = df_drive_new3.apply(lambda x: x['drive_time_of_possession'] if x['posteam_type'] == 'away' else np.nan,axis = 1)
    df_drive_new3['away_top_sec'] = df_drive_new3.away_top.apply(lambda x: int(x[:x.index(':')])*60 + int(x[x.index(':')+1:]) if type(x) == str else 0)
    
    df_drive_new3['home_drive_ended_with_score'] = df_drive_new3.apply( lambda x: 1 if ((x['posteam_type'] == 'home') & (x['drive_ended_with_score'] == 1)) else (0 if x['posteam_type'] == 'home' else np.nan) , axis = 1)
    df_drive_new3['away_drive_ended_with_score'] = df_drive_new3.apply( lambda x: 1 if ((x['posteam_type'] == 'away') & (x['drive_ended_with_score'] == 1)) else (0 if x['posteam_type'] == 'away' else np.nan) , axis = 1)
    
    df_drive_new3['home_drive_ended_with_fg'] = df_drive_new3.apply( lambda x: 1 if ((x['field_goal_result'] == 'made') & ( x['posteam_type'] == 'home')) else (0 if  x['posteam_type'] == 'home' else np.nan), axis = 1)
    df_drive_new3['away_drive_ended_with_fg'] = df_drive_new3.apply( lambda x: 1 if ((x['field_goal_result'] == 'made') & ( x['posteam_type'] == 'away')) else (0 if  x['posteam_type'] == 'away' else np.nan), axis = 1)
                                                                    
    df_drive_new3['home_drive_ended_with_td'] = df_drive_new3.apply(lambda x: 1 if ((x['posteam_type'] == 'home') & (x['touchdown'] == 1) & (x['td_team'] == x['posteam']))  else (0 if  x['posteam_type'] == 'home' else np.nan), axis = 1 )
    df_drive_new3['away_drive_ended_with_td'] = df_drive_new3.apply(lambda x: 1 if ((x['posteam_type'] == 'away') & (x['touchdown'] == 1) & (x['td_team'] == x['posteam']))  else (0 if  x['posteam_type'] == 'away' else np.nan), axis = 1 )
    

    df_drive2 = df_drive_new3.groupby(['game_id','fixed_drive','posteam_type']).agg({
        'home_top_sec':'max',
        'away_top_sec':'max',
        'home_redzone': 'max',
        'away_redzone': 'max',
        'home_td_redzone':'max',
        'away_td_redzone':'max',
        'home_drive_ended_with_score':'max',
        'away_drive_ended_with_score':'max',
        'home_drive_ended_with_fg':'max',
        'away_drive_ended_with_fg':'max',
        'home_drive_ended_with_td':'max',
        'away_drive_ended_with_td':'max',
        
        })
    return df_drive2




def create_game_df(df):
    print("running create_game_df")
    #turn drive stats into game stats
    # reset df after removing variables not being used
    df = remove_unused_vars(df)
    df_drive =  create_drive_df(df)
    df_drive3 = df_drive.groupby('game_id').agg({
        'home_top_sec':'sum',
        'away_top_sec':'sum',
        'home_redzone': 'sum',
        'away_redzone': 'sum',
        'home_td_redzone':'mean',
        'away_td_redzone':'mean',
        
        'home_drive_ended_with_score':'mean',
        'away_drive_ended_with_score':'mean',
        'home_drive_ended_with_fg':'mean',
        'away_drive_ended_with_fg':'mean',
        'home_drive_ended_with_td':'mean',
        'away_drive_ended_with_td':'mean'
        
        
        })
    
   

    # to get weather - scan for Temp: and after is the degrees, same for Humidity: and Wind
    # note: we are missing some weather data listed as nan -- so, only grabbing temp, humidity, and wind if weather is a string
    df['weather_temp'] = df.apply(lambda x: int((x['weather'][(x['weather'].index('Temp:') + len('Temp:')):((x['weather'].index('Temp:')) + len('Temp:')+3)]).replace('°','')) if  ((x['roof'] == 'outdoors') and (type(x['weather']) == str)) else np.nan, axis = 1)
    df['weather_humidity'] = df.apply(lambda x: (x['weather'][(x['weather'].index('Humidity:') + len('Humidity:')):(x['weather'].index('Humidity:') + len('Humidity:')+3)]).replace('%','') if (('Humidity' in str(x['weather'])) & (x['roof'] == 'outdoors') & (type(x['weather']) == str)) else np.nan, axis = 1).astype(float)
    df['weather_wind'] = (df.apply(lambda x: x['weather'][(x['weather'].index('Wind:') + len('Wind:')):(x['weather'].index('mph'))] if ((x['roof'] == 'outdoors') and (type(x['weather']) == str)) else 0, axis = 1).str.extract('(\d+)', expand=False)).astype(float)       
    df['weather_wind'].value_counts(normalize = True, dropna = False)
    
    df['weather_wind'][df['roof'] == 'outdoors'].value_counts(normalize = True, dropna = False)
    df['weather_humidity'].value_counts(normalize = True, dropna = False)
    df['weather_temp'][df['roof'] == 'outdoors'].value_counts(normalize = True, dropna = False)
    
    df['home_3down_converted'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' and x['third_down_converted'] == 1 else 0, axis = 1)
    
    df['home_3down_total'] = df.apply(lambda x: 1 if x['down'] == 3 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_3down_converted'] = df.apply(lambda x: 1 if x['third_down_converted'] == 1 and x['posteam_type'] != 'home' else 0 , axis = 1)
    df['away_3down_total'] = df.apply(lambda x: 1 if x['down'] == 3 and x['posteam_type'] != 'home' else 0 , axis = 1)
    
    df['home_4down_converted'] = df.apply(lambda x: 1 if x['fourth_down_converted'] == 1 and x['posteam_type'] == 'home' else 0 , axis = 1)
    df['home_4down_total'] = df.apply(lambda x: 1 if x['down'] == 4 and x['special'] == 0 and x['posteam_type'] == 'home' else 0, axis = 1) 
    df['away_4down_converted'] = df.apply(lambda x: 1 if x['fourth_down_converted'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1) 
    df['away_4down_total'] = df.apply(lambda x: 1 if x['down'] == 4 and x['special'] == 0 and x['posteam_type'] != 'home' else 0 , axis = 1)
    
    df['home_offensive_touchdown'] = df.apply(lambda x: 1 if x['touchdown'] == 1 and x['td_team'] == x['home_team'] and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_offensive_touchdown'] = df.apply(lambda x: 1 if x['touchdown'] == 1 and x['td_team'] == x['away_team'] and x['posteam_type'] == 'away' else 0, axis = 1)
    df['home_defensive_touchdown'] = df.apply(lambda x: 1 if x['touchdown'] == 1 and x['td_team'] == x['home_team'] and x['posteam_type'] == 'away' else 0, axis = 1)
    df['away_defensive_touchdown'] = df.apply(lambda x: 1 if x['touchdown'] == 1 and x['td_team'] == x['away_team'] and x['posteam_type'] == 'home' else 0, axis = 1)
    df['home_rush_attempt'] = df.apply(lambda x: 1 if x['rush_attempt'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_rush_attempt'] = df.apply(lambda x: 1 if x['rush_attempt'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    df['home_pass_attempt'] = df.apply(lambda x: 1 if x['pass_attempt'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_pass_attempt'] = df.apply(lambda x: 1 if x['pass_attempt'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    
    df['home_pass_complete'] = df.apply(lambda x: 1 if x['complete_pass'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_pass_complete'] = df.apply(lambda x: 1 if x['complete_pass'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    
    df['home_pass_incomplete'] = df.apply(lambda x: 1 if x['incomplete_pass'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_pass_incomplete'] = df.apply(lambda x: 1 if x['incomplete_pass'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)

    df['home_pass_touchdown'] = df.apply(lambda x: 1 if x['pass_touchdown'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_pass_touchdown'] = df.apply(lambda x: 1 if x['pass_touchdown'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    df['home_rush_touchdown'] = df.apply(lambda x: 1 if x['rush_touchdown'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_rush_touchdown'] = df.apply(lambda x: 1 if x['rush_touchdown'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    df['home_return_touchdown'] = df.apply(lambda x: 1 if x['return_touchdown'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_return_touchdown'] = df.apply(lambda x: 1 if x['return_touchdown'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    df['home_sack'] = df.apply(lambda x: 1 if x['sack'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_sack'] = df.apply(lambda x: 1 if x['sack'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)

    
    df['home_field_goal_pct'] = df.apply(lambda x: 1 if x['field_goal_result'] == 'made' and x['posteam_type'] == 'home' else (0 if  x['field_goal_attempt'] == 1 else np.nan), axis = 1)
    df['away_field_goal_pct'] = df.apply(lambda x: 1 if x['field_goal_result'] == 'made' and x['posteam_type'] != 'home' else (0 if  x['field_goal_attempt'] == 1 else np.nan), axis = 1)
    df['home_longest_made_field_goal'] = df.apply(lambda x: x['kick_distance'] if x['field_goal_result'] == 'made' and x['posteam_type'] == 'home' else np.nan, axis = 1)
    df['away_longest_made_field_goal'] = df.apply(lambda x: x['kick_distance'] if x['field_goal_result'] == 'made' and x['posteam_type'] != 'home' else np.nan, axis = 1)
    
    df['home_total_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_total_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] != 'home' else 0, axis = 1)
    df['home_run_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] == 'home' and x['rush'] == 1 else 0, axis = 1)
    df['away_run_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] != 'home' and x['rush'] == 1 else 0, axis = 1)
    df['home_pass_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] == 'home' and x['pass'] == 1 else 0, axis = 1)
    df['away_pass_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] != 'home' and x['pass'] == 1 else 0, axis = 1)
    
    # mean and median yards gained will be the same as original until group by function 
    df['home_mean_run_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] == 'home' and x['rush'] == 1 else np.nan, axis = 1)
    df['away_mean_run_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] != 'home' and x['rush'] == 1 else np.nan, axis = 1)
    df['home_mean_pass_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] == 'home' and x['pass'] == 1 else np.nan, axis = 1)
    df['away_mean_pass_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] != 'home' and x['pass'] == 1 else np.nan, axis = 1)
    #median
    df['home_median_run_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] == 'home' and x['rush'] == 1 else np.nan, axis = 1)
    df['away_median_run_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] != 'home' and x['rush'] == 1 else np.nan, axis = 1)
    df['home_median_pass_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] == 'home' and x['pass'] == 1 else np.nan, axis = 1)
    df['away_median_pass_yards_gained'] = df.apply(lambda x: x['yards_gained'] if x['posteam_type'] != 'home' and x['pass'] == 1 else np.nan, axis = 1)
    
    
    df['home_total_air_yards'] = df.apply(lambda x: x['air_yards'] if x['posteam_type'] == 'home' else np.nan, axis = 1)
    df['away_total_air_yards'] = df.apply(lambda x: x['air_yards'] if x['posteam_type'] != 'home' else np.nan, axis = 1)
    # mean and median yards gained will be the same as original until group by function 
    df['home_mean_air_yards'] = df.apply(lambda x: x['air_yards'] if x['posteam_type'] == 'home' else np.nan, axis = 1)
    df['away_mean_air_yards'] = df.apply(lambda x: x['air_yards'] if x['posteam_type'] != 'home' else np.nan, axis = 1)
    #median
    df['home_median_air_yards'] = df.apply(lambda x: x['air_yards'] if x['posteam_type'] == 'home' else np.nan, axis = 1)
    df['away_median_air_yards'] = df.apply(lambda x: x['air_yards'] if x['posteam_type'] != 'home' else np.nan, axis = 1)

    df['home_yac'] = df.apply(lambda x: x['yards_after_catch'] if x['posteam_type'] == 'home' and x['pass'] == 1 else np.nan, axis = 1)
    df['away_yac'] = df.apply(lambda x: x['yards_after_catch'] if x['posteam_type'] == 'home' and x['pass'] == 1 else np.nan, axis = 1)
    df['home_threw_int'] = df.apply(lambda x: 1 if x['interception'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_threw_int'] = df.apply(lambda x: 1 if x['interception'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)

    df['home_caused_fumble'] = df.apply(lambda x: 1 if x['fumble_forced'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    df['away_caused_fumble'] = df.apply(lambda x: 1 if x['fumble_forced'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    
    
    df['home_fumble'] = df.apply(lambda x: 1 if x['fumble'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_fumble'] = df.apply(lambda x: 1 if x['fumble'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    
    df['home_tackled_for_loss'] = df.apply(lambda x: 1 if x['tackled_for_loss'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_tackled_for_loss'] = df.apply(lambda x: 1 if x['tackled_for_loss'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
 
    df['home_successful_play'] = df.apply(lambda x: 1 if x['success'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_successful_play'] = df.apply(lambda x: 1 if x['success'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    
    df['home_successful_runs'] = df.apply(lambda x: 1 if x['success'] == 1 and x['rush'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_successful_runs'] = df.apply(lambda x: 1 if x['success'] == 1 and x['rush'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    
    df['home_successful_pass'] = df.apply(lambda x: 1 if x['success'] == 1 and x['pass'] == 1 and x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_successful_pass'] = df.apply(lambda x: 1 if x['success'] == 1 and x['pass'] == 1 and x['posteam_type'] != 'home' else 0, axis = 1)
    
    #epa's
    df['home_mean_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] == 'home' else np.nan, axis = 1)
    df['away_mean_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] != 'home' else np.nan, axis = 1)
    
    df['home_median_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] == 'home' else np.nan, axis = 1)
    df['away_median_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] != 'home' else np.nan, axis = 1)
    
    #run
    df['home_mean_run_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] == 'home' and x['rush'] == 1 else np.nan, axis = 1)
    df['away_mean_run_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] != 'home' and x['rush'] == 1 else np.nan, axis = 1)
    
    df['home_median_run_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] == 'home' and x['rush'] == 1  else np.nan, axis = 1)
    df['away_median_run_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] != 'home' and x['rush'] == 1   else np.nan, axis = 1)
    
    #pass
    df['home_mean_pass_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] == 'home' and x['pass'] == 1  else np.nan, axis = 1)
    df['away_mean_pass_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] != 'home' and x['pass'] == 1   else np.nan, axis = 1)
    
    df['home_median_pass_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] == 'home' and x['pass'] == 1  else np.nan, axis = 1)
    df['away_median_pass_epa'] = df.apply(lambda x: x['epa'] if x['posteam_type'] != 'home' and x['pass'] == 1  else np.nan, axis = 1)
     
    #using both hits that became sacks and hits that did not to compare afterwards for which is better to use
    df['home_qb_hit'] = df.apply(lambda x: 1 if x['posteam_type'] != 'home' and x['qb_hit'] == 1  else np.nan, axis = 1)
    df['away_qb_hit'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' and x['qb_hit'] == 1  else np.nan, axis = 1)
    
    df['home_qb_hit_allowed'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' and x['qb_hit'] == 1 and x['sack'] != 1  else np.nan, axis = 1)
    df['away_qb_hit_allowed'] = df.apply(lambda x: 1 if x['posteam_type'] != 'home' and x['qb_hit'] == 1 and x['sack'] != 1  else np.nan, axis = 1)
    
    df['home_qb_hit_nosack'] = df.apply(lambda x: 1 if x['posteam_type'] != 'home' and x['qb_hit'] == 1 and x['sack'] != 1  else np.nan, axis = 1)
    df['away_qb_hit_nosack'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' and x['qb_hit'] == 1 and x['sack'] != 1  else np.nan, axis = 1)
    
    df['home_qb_hit_nosack_allowed'] = df.apply(lambda x: 1 if x['posteam_type'] != 'home' and x['qb_hit'] == 1 and x['sack'] != 1  else np.nan, axis = 1)
    df['away_qb_hit_nosack_allowed'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' and x['qb_hit'] == 1 and x['sack'] != 1  else np.nan, axis = 1)
    
    df['home_qb_scramble'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' and x['qb_scramble'] == 1  else np.nan, axis = 1)
    df['away_qb_scramble'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' and x['qb_scramble'] == 1  else np.nan, axis = 1)
    
    df['home_off_play'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_off_play'] = df.apply(lambda x: 1 if x['posteam_type'] == 'away' else 0, axis = 1)
    df.reset_index(inplace=True)
    df['season'] = df.game_id.apply(lambda x: int(str(x[0:4])))
    df['home_play'] = df.apply(lambda x: 1 if x['posteam_type'] == 'home' else 0, axis = 1)
    df['away_play'] = df.apply(lambda x: 1 if x['posteam_type'] == 'away' else 0, axis = 1)
    df['indoors'] = df.apply(lambda x: 1 if x['roof'] == 'dome' else 0,axis = 1)
    
    
    df['home_first_down_yards'] = df.apply(lambda x:  x['yards_gained'] if ((x['down'] == 1) & ((x['pass'] == 1) | (x['rush'] ==1))) else np.nan, axis = 1)
    df['away_first_down_yards'] = df.apply(lambda x:  x['yards_gained'] if ((x['down'] == 1) & ((x['pass'] == 1) | (x['rush'] ==1))) else np.nan, axis = 1)
    
    df['home_second_down_yards'] = df.apply(lambda x:  x['yards_gained'] if ((x['down'] == 2) & ((x['pass'] == 1) | (x['rush'] ==1))) else np.nan, axis = 1)
    df['away_second_down_yards'] = df.apply(lambda x:  x['yards_gained'] if ((x['down'] == 2) & ((x['pass'] == 1) | (x['rush'] ==1))) else np.nan, axis = 1)
    df['home_caused_turnover'] = df.apply(lambda x: 1 if ((x['home_caused_fumble']==1) | (x['away_threw_int']==1)) else 0, axis = 1)
    df['away_caused_turnover'] = df.apply(lambda x: 1 if ((x['away_caused_fumble']==1) | (x['home_threw_int']==1)) else 0, axis = 1)
    
    game_df = df.groupby(['game_id','old_game_id', 'game_date', 'home_team', 'away_team', 'season_type','season']).agg({
        'week':'max',
        'div_game':'max',
        'weather_temp': 'max',
        'weather_humidity':'max',
        'weather_wind':'max',
        'indoors':'max',
        'home_first_down_yards':'mean',
        'home_second_down_yards':'mean',
        'home_3down_converted':'sum',
        'home_3down_total':'sum',
        'home_4down_converted':'sum',
        'home_4down_total':'sum',
        'home_offensive_touchdown': 'sum',
        'home_defensive_touchdown': 'sum',
        'home_rush_attempt': 'sum',
        'home_rush_touchdown':'sum',
        'home_pass_attempt': 'sum',
        'home_pass_complete':'sum',
        'home_pass_incomplete':'sum',
        'home_pass_touchdown':'sum',
        'home_return_touchdown':'sum',
        'home_field_goal_pct':'mean',
        'home_total_yards_gained':'sum',
        'home_run_yards_gained':'sum',
        'home_pass_yards_gained':'sum',
        'home_mean_run_yards_gained': 'mean',
        'home_median_air_yards':'median',
        'home_yac':'sum',
        'home_threw_int':'sum',
        'home_fumble':'sum',
        'home_successful_play':'sum',
        'home_successful_runs':'sum',
        'home_successful_pass':'sum',
        'home_longest_made_field_goal':'max',
        'home_mean_epa':'mean',
        'home_median_epa':'median',
        'home_mean_run_epa':'mean',
        'home_median_run_epa':'median',
        'home_mean_pass_epa':'mean',
        'home_median_pass_epa':'median',
        'home_off_play':'sum',
        'away_off_play':'sum',
        'home_qb_hit_nosack_allowed':'sum',
        'home_qb_scramble':'sum',
        'home_qb_hit_nosack':'sum',
        'home_qb_hit':'sum',
        'home_caused_fumble':'sum',
        'home_tackled_for_loss':'sum',
        'home_sack':'sum',
        'home_caused_turnover':'sum',
        'total_home_score': 'max',
        'total_away_score': 'max',
        'away_first_down_yards':'mean',
        'away_second_down_yards':'mean',
        'away_3down_converted':'sum',
        'away_3down_total':'sum',
        'away_4down_converted':'sum',
        'away_4down_total':'sum',
        'away_offensive_touchdown': 'sum',
        'away_defensive_touchdown': 'sum',
        'away_rush_attempt': 'sum',
        'away_rush_touchdown':'sum',
        'away_pass_attempt': 'sum',
        'away_pass_complete':'sum',
        'away_pass_incomplete':'sum',
        'away_pass_touchdown':'sum',
        'away_return_touchdown':'sum',
        'away_field_goal_pct':'mean',
        'away_total_yards_gained':'sum',
        'away_run_yards_gained':'sum',
        'away_pass_yards_gained':'sum',
        'away_mean_run_yards_gained': 'mean',
        'away_median_air_yards':'median',
        'away_yac':'sum',
        'away_threw_int':'sum',
        'away_fumble':'sum',
        'away_successful_play':'sum',
        'away_successful_runs':'sum',
        'away_successful_pass':'sum',
        'away_longest_made_field_goal':'max',
        'away_mean_epa':'mean',
        'away_median_epa':'median',
        'away_mean_run_epa':'mean',
        'away_median_run_epa':'median',
        'away_mean_pass_epa':'mean',
        'away_median_pass_epa':'median',
        'away_qb_hit_nosack_allowed':'sum',
        'away_qb_scramble':'sum',
        'away_qb_hit_nosack':'sum',
        'away_qb_hit':'sum',
        'away_caused_fumble':'sum',
        'away_tackled_for_loss':'sum',
        'away_sack':'sum',
        'away_caused_turnover':'sum',
        'home_score':'max',
        'away_score':'max',
        'home_play':'sum',
        'away_play':'sum'
     })
    

######################################################

#       Merge on penalties, DRIVE_DF, Odds

######################################################


    
    df_penalties= create_penalties_df(df).reset_index()
    game_df2 = game_df.reset_index()

    
    game_df2 = pd.merge(game_df2, df_penalties, on=['game_id','home_team','away_team'],validate="one_to_one")
    game_df2 = pd.merge(game_df2.reset_index(), df_drive3.reset_index(), on='game_id')
    df_odds1 = df.copy().reset_index()
    columns = ['game_id','home_team','away_team','away_score','home_score','result','total','spread_line','total_line']
    df_odds = df_odds1[columns]
    df_odds = df_odds.copy()
    df_odds['home_spread'] = df_odds['spread_line']*-1
    df_odds['away_spread'] = df_odds['spread_line']

    
    #MAKE ODDS BY GAME
    df_odds_game = df_odds.groupby(['game_id','spread_line','home_spread','away_spread']).agg({
        'total':'max',
        'total_line':'max'
        })  
    game_df2.drop(columns=['index'],inplace=True)
    game_df3=pd.merge(game_df2.reset_index(), df_odds_game.reset_index(), on='game_id',validate='one_to_one').set_index('game_id')
    game_df3.drop(columns=['index'],inplace=True)
    
    game_df3['home_pace'] = game_df3['home_top_sec']/game_df3['home_off_play']
    game_df3['away_pace'] = game_df3['away_top_sec']/game_df3['away_off_play']
    game_df3['home_pct_plays_pass'] = game_df3['home_pass_attempt']/(game_df3['home_pass_attempt'] + game_df3['home_rush_attempt'])
    game_df3['home_pct_plays_run'] = game_df3['home_rush_attempt']/(game_df3['home_pass_attempt'] + game_df3['home_rush_attempt'])
    game_df3['home_pct_yards_pass'] = game_df3['home_pass_yards_gained']/game_df3['home_total_yards_gained']
    game_df3['home_pct_yards_run'] = game_df3['home_run_yards_gained']/game_df3['home_total_yards_gained']
    
    
    game_df3['away_pct_plays_pass'] = game_df3['home_pass_attempt']/(game_df3['home_pass_attempt'] + game_df3['home_rush_attempt'])
    game_df3['away_pct_plays_run'] = game_df3['home_rush_attempt']/(game_df3['home_pass_attempt'] + game_df3['home_rush_attempt'])
    game_df3['away_pct_yards_pass'] = game_df3['away_pass_yards_gained']/game_df3['away_total_yards_gained']
    game_df3['away_pct_yards_run'] = game_df3['away_run_yards_gained']/game_df3['away_total_yards_gained']
    game_df3['home_completion_pct'] = game_df3['home_pass_complete']/game_df3['home_pass_attempt']
    game_df3['away_completion_pct'] = game_df3['away_pass_complete']/game_df3['away_pass_attempt']
    game_df3['home_3down_conversion_pct'] = game_df3['home_3down_converted']/game_df3['home_3down_total']
    game_df3['away_3down_conversion_pct'] = game_df3['away_3down_converted']/game_df3['away_3down_total']
    
    
    game_df3['home_pct_successful_play'] = game_df3['home_successful_play']/game_df3['home_play']
    game_df3['away_pct_successful_play'] = game_df3['away_successful_play']/game_df3['away_play']

    game_df3['home_pct_successful_runs'] = game_df3['home_successful_runs']/game_df3['home_rush_attempt']
    game_df3['away_pct_successful_runs'] = game_df3['away_successful_runs']/game_df3['away_rush_attempt']

    
    game_df3['home_pct_successful_pass'] = game_df3['home_successful_pass']/game_df3['home_pass_attempt']
    game_df3['away_pct_successful_pass'] = game_df3['away_successful_pass']/game_df3['away_pass_attempt']
    
    game_df3['away_run_yards_per_play'] = game_df3['away_run_yards_gained']/game_df3['away_rush_attempt']
    game_df3['home_run_yards_per_play'] = game_df3['home_run_yards_gained']/game_df3['home_rush_attempt']
   
    game_df3['away_pass_yards_per_play'] = game_df3['away_pass_yards_gained']/game_df3['away_pass_attempt']
    game_df3['home_pass_yards_per_play'] = game_df3['home_pass_yards_gained']/game_df3['home_pass_attempt']
    
    game_df3['home_winner'] = game_df3.apply(lambda x: 1 if (x['home_score'] > x['away_score']) else 0, axis = 1  )
    game_df3['away_winner'] = game_df3.apply(lambda x: 1 if (x['home_score'] < x['away_score']) else 0, axis = 1 )
    
    game_df3['home_win_gap'] = game_df3.apply(lambda x: (x['home_score'] - x['away_score']) if x['home_winner'] == 1 else (0 if x['home_winner'] == 1 else np.nan ), axis = 1)
    game_df3['away_win_gap'] = game_df3.apply(lambda x: (x['away_score'] - x['home_score']) if x['away_winner'] == 1 else (0 if x['away_winner'] == 1 else np.nan ), axis = 1)
    
    # get points won by when a team won
    
    return game_df3


def prep_game_df_for_avgs(df, Add_new_week_sched):
    print("running prep_game_df_for_avgs")
        
    game_df3 = df         
    #check = game_df3[['home_qb_hit_nosack','home_qb_hit','home_sack']]
    
    game_df4 = game_df3.drop(['season_type','old_game_id',
                              'home_3down_total',
                              'home_3down_converted',
                              'away_3down_converted',
                              'home_4down_total',
                              'home_rush_attempt',
                              'home_pass_attempt',
                              'home_pass_complete',
                              'home_pass_incomplete',
                              'home_rush_attempt',
                              'home_pass_attempt',
                              'home_longest_made_field_goal',
                              'home_redzone',
                              'away_3down_total',
                              'away_4down_total',
                              'away_rush_attempt',
                              'away_pass_attempt',
                              'away_pass_complete',
                              'away_pass_incomplete',
                              'away_rush_attempt',
                              'away_pass_attempt',
                              'away_longest_made_field_goal',
                              'away_redzone',
                              'away_off_play',
                              'home_off_play',
                              'spread_line',
                              'home_spread',
                              'away_spread',
                              'total',
                              'total_line',
                              'indoors',
                              'home_successful_play',
                              'away_successful_play',
                              'home_successful_runs',
                              'away_successful_runs',
                              'home_successful_pass',
                              'away_successful_pass'],axis=1)
    
    game_df4['home_wins'] = game_df4.apply(lambda x: 1 if x['total_home_score'] > x['total_away_score']  else 0 ,axis = 1)
    game_df4['home_losses'] = game_df4.apply(lambda x: 0 if x['total_home_score'] > x['total_away_score']  else 1 ,axis = 1)
    game_df4['home_ties'] = game_df4.apply(lambda x: 1 if x['total_home_score'] == x['total_away_score']  else 0 ,axis = 1)
    game_df4['away_wins'] = game_df4.apply(lambda x: 1 if x['total_home_score'] < x['total_away_score']  else 0 ,axis = 1)
    game_df4['away_losses'] = game_df4.apply(lambda x: 0 if x['total_home_score'] < x['total_away_score']  else 1 ,axis = 1)
    game_df4['away_ties'] = game_df4.apply(lambda x: 1 if x['total_home_score'] == x['total_away_score']  else 0 ,axis = 1)

     







    
    #merge on next weeks schedule to game df
    if Add_new_week_sched in ['yes','Yes','YES','y','Y']:

        new_week_sched = pd.read_excel(os.path.join(clean_data_folder, 'Upcoming Week Schedule.xlsx'))
        
        new_week_sched['game_id'] = new_week_sched.apply(lambda x: str(str(x['season']) + '_' + str(x['week']) + '_' + x['away_team'] + '_' + x['home_team']),axis=1)
        new_week_sched['game_date'] =new_week_sched.apply(lambda x: str(x['game_date'].date()),axis = 1)
        new_week_sched['season'] =  new_week_sched['season'].astype(int)                   
                     
        new_week_sched.reset_index(inplace=True)
        new_week_sched.set_index(['game_id','season','week','home_team','away_team'],inplace=True)
        game_df4.reset_index(inplace=True)
        game_df4.set_index(['game_id','season','week','home_team','away_team'],inplace=True)
        
        game_df5 = pd.concat([game_df4,new_week_sched])
        game_df5 = game_df5.drop(['index','home_spread','away_spread'],axis = 1)

    
        return game_df5
    else:
        game_df4.reset_index(inplace=True)
        game_df4.set_index(['game_id','season','week','home_team','away_team'],inplace=True)
        return game_df4
        


    
def rename_cols(home_away,df):

    temp_dict = {}
    sums = ['home_wins','home_losses','home_ties','away_wins','away_losses','away_ties']
    if home_away == 'home':
        for x in df.columns:
            if 'home' in x:
                if x in sums:
                    new_col = x.replace('home_','total_')
                    temp_dict[x] = new_col
                else:                
                    new_col = x.replace('home_','avg_')
                    temp_dict[x] = new_col
            if 'away' in x:
                if x not in sums:
                    new_col = x.replace('away_','against_avg_')
                    temp_dict[x] = new_col
                
    else:
        for x in df.columns:
            if 'home' in x:
                if x not in sums:
                    new_col = x.replace('home_','against_avg_')
                    temp_dict[x] = new_col
            if 'away' in x:
                if x in sums:
                    new_col = x.replace('away_','total_')
                    temp_dict[x] = new_col
                else:
                    new_col = x.replace('away_','avg_')
                    temp_dict[x] = new_col

    return temp_dict
                

def cols_for_groupby(df):
    sumamry_dic = {}
    max_dic = {}
    overall_dic = {}
    exclude = ['week','season']
    sum_lst = ['total_wins','total_losses','total_ties']# wins and losses for both home and away
    max_lst = ['indoor','weather_wind','weather_humidity','weather_temp','div_game']#indoor, weather vars,div game
    for var in df.columns:
        if var in sum_lst:
            if ((type(list(df[var])[0]) != str) & (var not in exclude)):
                sumamry_dic[var] = 'sum'
        elif var in max_lst:
            if ((type(list(df[var])[0]) != str) & (var not in exclude)):
                max_dic[var] = 'max'
            
        else:
            if ((type(list(df[var])[0]) != str) & (var not in exclude)):
                sumamry_dic[var] = 'mean'
            
    overall_dic['mean_sum'] = sumamry_dic
    overall_dic['max'] = max_dic
    return overall_dic


def get_season_avgs_by_game(game_df4,include_upcoming_week):
    print("running get_season_avgs_by_game")
    game_df_season = pd.DataFrame()
    game_df4.reset_index(inplace=True)
    if ('N' in include_upcoming_week) or ('n' in include_upcoming_week):
        for season in game_df4.season.unique():
            for team in game_df4[game_df4['season'] == season].home_team.unique():
                for week in game_df4[((game_df4['season'] == season) & ((game_df4['home_team'] == team) | (game_df4['away_team'] == team))) ].week.unique():
                    if week > 1:
                        temp = game_df4.loc[((game_df4['season'] == season) & (game_df4['week'] < week)) & ((game_df4['home_team'] == team) | (game_df4['away_team'] == team))]  
                        if not temp.empty:
                            temp3 = pd.DataFrame()
                            for game in temp['game_id'].unique():
                                game_team = temp[temp['game_id'] == game]
                                team_check = list(game_team['home_team'])[0]
                                if team_check == team: #if team is home team
                                    temp2 = game_team.copy()
                                    temp2 = temp2.drop(['away_wins','away_losses','away_ties','home_winner','away_winner'],axis=1)
                                    dic_home = rename_cols('home',temp2)
                                    temp2.rename(columns=dic_home,inplace = True)
                                    temp2['team'] = team
                                else:
                                    temp2 = game_team.copy()
                                    temp2 = temp2.drop(['home_wins','home_losses','home_ties','home_winner','away_winner'],axis=1)
                                    dic_away = rename_cols('away',temp2)
                                    temp2.rename(columns=dic_away,inplace = True)
                                    temp2['team'] = team
                                if temp3.empty:
                                    temp3 = temp2
                                else:
                                    temp3 = temp3.append(temp2)    
                            temp3 = temp3.drop('week',axis=1)
                            temp3['week'] = week
                            temp4 = temp3.groupby(['team','season','week']).agg(cols_for_groupby(temp3)['mean_sum'])
                            game_df_season = game_df_season.append(temp4)
    else:
        for season in game_df4.season.unique():
            for team in game_df4[game_df4['season'] == season].home_team.unique():
                for week in game_df4[((game_df4['season'] == season) & ((game_df4['home_team'] == team) | (game_df4['away_team'] == team))) ].week.unique():
                        temp = game_df4.loc[((game_df4['season'] == season) & (game_df4['week'] <= week)) & ((game_df4['home_team'] == team) | (game_df4['away_team'] == team))]  
                        if not temp.empty:
                            temp3 = pd.DataFrame()
                            for game in temp['game_id'].unique():
                                game_team = temp[temp['game_id'] == game]
                                team_check = list(game_team['home_team'])[0]
                                if team_check == team: #if team is home team
                                    temp2 = game_team.copy()
                                    temp2 = temp2.drop(['away_wins','away_losses','away_ties','home_winner','away_winner'],axis=1)
                                    dic_home = rename_cols('home',temp2)
                                    temp2.rename(columns=dic_home,inplace = True)
                                    temp2['team'] = team
                                else:
                                    temp2 = game_team.copy()
                                    temp2 = temp2.drop(['home_wins','home_losses','home_ties','home_winner','away_winner'],axis=1)
                                    dic_away = rename_cols('away',temp2)
                                    temp2.rename(columns=dic_away,inplace = True)
                                    temp2['team'] = team
                                if temp3.empty:
                                    temp3 = temp2
                                else:
                                    temp3 = temp3.append(temp2)    
                            temp3 = temp3.drop('week',axis=1)
                            temp3['week'] = week
                            temp4 = temp3.groupby(['team','season','week']).agg(cols_for_groupby(temp3)['mean_sum'])
                            game_df_season = game_df_season.append(temp4)


    return game_df_season
   
def rename_cols_game(df,home_away):
    dic = {}
    
    for var in df.columns:
        new_var = str(home_away)+'_'+var
        dic[var]=new_var
    return dic
        
#bring on game id and teams         

def days_rest(df):
 #   df.reset_index(inplace=True)
    all_teams = pd.DataFrame()  
    seasons = pd.Series(df['season'].unique())
    seasons.sort_values(inplace=True)
    for season in seasons:
        for team in df['home_team'].unique():
            temp = df[((df['season'] == season) & ((df['home_team'] == team) | (df['away_team'] == team )))]
            if not temp.empty:
                temp2 = temp[['game_id','season','home_team','away_team']].copy()
                temp2['team'] = team
                temp2['game_dates'] = temp.apply(lambda x: datetime.strptime(x['game_date'], '%Y-%m-%d').date(),axis=1)
                game_dates_datetime = list(temp2['game_dates'])
                game_dates_datetime.sort()
                days_series = list()
                for i in range(0,len(game_dates_datetime)):
                    if i == 0:
                        days = 100
                    else:
                        days = (game_dates_datetime[i] - game_dates_datetime[(i-1)]).days
                    days_series.append(days)
                temp2['days_rest'] = days_series
                temp2 = temp2[['game_id','season','home_team','away_team','team','days_rest']].copy()
                temp2['home_days_rest'] = temp2.apply(lambda x: x['days_rest'] if x['home_team'] == x['team'] else 0, axis = 1)
                temp2['away_days_rest'] = temp2.apply(lambda x: x['days_rest'] if x['home_team'] != x['team'] else 0, axis = 1)
                all_teams= pd.concat([all_teams,temp2])  
    home = all_teams[['game_id','season','home_team','home_days_rest']].loc[all_teams['home_days_rest'] > 0]
    home.set_index(['game_id','season'],inplace=True)
    away = all_teams[['game_id','season','away_team','away_days_rest']].loc[all_teams['away_days_rest'] > 0]
    away.set_index(['game_id','season'],inplace=True)
    all_teams2 = pd.merge(home,away,how='inner',left_index=True,right_index=True)
    
    return all_teams2



def create_final_game_avgs_df(df,Add_new_week_sched,include_upcoming_week):
    print("running create_final_game_avgs_df")
    full_game_df = create_game_df(df)
    game_df4 = prep_game_df_for_avgs(full_game_df,Add_new_week_sched)
    
        

    game_teams_freq = game_df4.reset_index()[['game_id','home_team','away_team','week','season']]
    
    games_home = game_teams_freq.copy()
    games_home['team'] = games_home['home_team']
    games_away = game_teams_freq.copy()
    games_away['team'] = games_away['away_team']
    
    game_df_season = get_season_avgs_by_game(game_df4,'No')
    game_df_season.reset_index(inplace=True)
    
    game_df_season_home = game_df_season.merge(games_home,how = 'inner',left_on=['team','season','week'],right_on=['team','season','week']).set_index(['game_id','season','week'])
    game_df_season_home.reset_index()['game_id'].value_counts(dropna = False)
    game_df_season_away = game_df_season.merge(games_away,how = 'inner',left_on=['team','season','week'],right_on=['team','season','week']).set_index(['game_id','season','week'])    
    #game_df_season_full = game_df_season_home.merge(game_df_season_away,how='left',left_index=True,right_index=True)
    
    game_df_season_home2 = game_df_season_home.rename(columns=rename_cols_game(game_df_season_home,'home'))
    game_df_season_away2 = game_df_season_away.rename(columns=rename_cols_game(game_df_season_away,'away'))
    
    
    game_df_season_full2 = game_df_season_home2.merge(game_df_season_away2,how='inner',left_index=True,right_index=True)
    #game_df_season_full3 = game_df_season_full2.merge(full_game_df.reset_index()[['game_id','indoors','weather_wind','weather_humidity','weather_temp','div_game','home_winner', 'away_winner','total_line','total','home_score','away_score','home_spread','away_spread']].set_index('game_id'),how='left',left_index=True,right_index=True)
    game_df_season_full3 = game_df_season_full2.merge(full_game_df.reset_index()[['game_id','indoors','weather_wind','weather_humidity','weather_temp','div_game','home_winner', 'away_winner','total_line','total','home_score','away_score','home_spread','away_spread']].set_index('game_id'),how='left',left_index=True,right_index=True)
    
    
    
    
    #merge on spread
    new_week_sched = pd.read_excel(os.path.join(clean_data_folder, 'Upcoming Week Schedule.xlsx'))
    
    new_week_sched['game_id'] = new_week_sched.apply(lambda x: str(str(x['season']) + '_' + str(x['week']) + '_' + x['away_team'] + '_' + x['home_team']),axis=1)
    new_week_sched.drop(['game_date'],axis=1,inplace=True)
    new_week_sched['season'] =  new_week_sched['season'].astype(int)                   
    new_week_sched.set_index(['game_id','season','week','home_team','away_team'],inplace=True)
    
    game_df_season_full3.reset_index(inplace=True)
    game_df_season_full3.set_index(['game_id','season','week','home_team','away_team'],inplace=True)
            
    
    
    game_df_season_full3b = game_df_season_full3.combine_first(new_week_sched)
    
    
    
    #merge on days rest
    rest_days = days_rest(game_df4)
    rest_days = rest_days.reset_index().set_index(['game_id','season','home_team','away_team'])
    game_df_season_full4 = game_df_season_full3b.reset_index().set_index(['game_id','season','home_team','away_team'])
    game_season_final = game_df_season_full4.merge(rest_days, how='left', left_index=True,right_index=True)
    game_season_final2 = game_season_final.drop(['home_home_team','home_away_team','away_home_team','away_away_team'],axis=1)


    
    return game_season_final2




