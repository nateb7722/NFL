#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 24 11:57:49 2020

@author: nathanbalson
"""
import pandas as pd
from pandas import DataFrame as df
import numpy as np
import os.path
from datetime import datetime 
from time import strptime, strftime

from Read_in import readin_historical, readin_current_year, readin_full_data, export_historical_to_csv, export_current_to_csv
from Gather_and_Clean import create_penalties_df, create_drive_df, create_game_df, prep_game_df_for_avgs, rename_cols, cols_for_groupby, get_season_avgs_by_game, rename_cols_game, days_rest, create_final_game_avgs_df 

Tableau_filepath = "C:\\Users\\nateb\\OneDrive\\Desktop\\Data Science Projects\Projects\\New Project EDA\\Tableau"
week_18_matchups_folder =  "C:\\Users\\nateb\\OneDrive\\Desktop\\Data Science Projects\Projects\\New Project EDA\\Tableau\\Matchups\\Week 18"
clean_data_folder = str("C:\\Users\\nateb\\OneDrive\\Desktop\\Data Science Projects\\datasets\\NFL\\Clean")


#To do:
    # search "Here"


def Update_season_charts(year):
    current_data = readin_current_year(year)
    game_df_current = create_game_df(current_data)
    game_df_avgs_current = prep_game_df_for_avgs(game_df_current,'no')
    season_avgs = get_season_avgs_by_game(game_df_avgs_current,include_upcoming_week='Yes')
    season_avgs_last_week = get_season_avgs_by_game(game_df_avgs_current,include_upcoming_week='No')
    
    #create total EPA variable for charts
    season_avgs['total_mean_epa'] = season_avgs['avg_mean_epa'] + (season_avgs['against_avg_mean_epa']*-1)
    season_avgs2 = season_avgs.copy()
    season_avgs_last_week2 = season_avgs_last_week.copy()
    season_avgs2.reset_index(inplace=True)
    season_avgs_last_week2.reset_index(inplace=True)
    last_week = season_avgs_last_week2[season_avgs_last_week2['week'] == max(season_avgs_last_week2['week'].unique())]
    #merge
    game_df4 = game_df_avgs_current.copy()
    
        
    game_teams_freq = game_df4.reset_index()[['game_id','home_team','away_team','week','season']]
    
    games_home = game_teams_freq.copy()
    games_home['team'] = games_home['home_team']
    games_home['opponent'] = games_home['away_team']
    games_away = game_teams_freq.copy()
    games_away['team'] = games_away['away_team']
    games_away['opponent'] = games_away['home_team']
    
    
    season_avgs2_home = season_avgs2.merge(games_home,how = 'inner',left_on=['team','season','week'],right_on=['team','season','week']).set_index(['game_id','season','week'])
    # game_df_season_home.reset_index()['game_id'].value_counts(dropna = False)
    season_avgs2_away = season_avgs2.merge(games_away,how = 'inner',left_on=['team','season','week'],right_on=['team','season','week']).set_index(['game_id','season','week'])    
    season_avgs2_full = pd.concat([season_avgs2_home,season_avgs2_away])
    season_avgs2_full.sort_index(inplace=True)
    season_avgs2_full.reset_index(inplace=True)
    with pd.ExcelWriter(os.path.join(Tableau_filepath,'Season Averages.xlsx')) as writer:  
        season_avgs2_full.to_excel(writer, sheet_name='Every_Week')
        last_week.to_excel(writer, sheet_name='last_week_only')
        game_df_current.to_excel(writer, sheet_name='Game DF')
    return "Season Avgs File Successfully Exported"

Update_season_charts(2020)

# current_data = readin_current_year(2020)
# game_df_current = create_game_df(current_data)
# game_df_avgs_current = prep_game_df_for_avgs(game_df_current,'no')
# season_avgs_last_week = get_season_avgs_by_game(game_df_avgs_current,include_upcoming_week='No')





#####################################################################
# Output matchup stats - final df current did not merge correctly # Here
#####################################################################

current_final_game_avgs_matchups= create_final_game_avgs_df(readin_current_year(2020),Add_new_week_sched='No',include_upcoming_week='No')


current_final_game_avgs_matchups['home_against_avg_field_goal_pct']

current_final_game_avgs = create_final_game_avgs_df(readin_current_year(2020),Add_new_week_sched='No',include_upcoming_week='No')
current_final_game_avgs.to_excel(os.path.join(clean_data_folder,'2020 Season Game DF averages .xlsx'))


def output_matchups(away_team,home_team):
    final_df_current = current_final_game_avgs_matchups.copy()
    final_df_current.reset_index(inplace=True)
    temp = final_df_current[final_df_current['game_id'] == '2020_18_'+away_team+'_'+home_team]
    # with pd.ExcelWriter(os.path.join(Tableau_filepath,'WEEK 18 Matchups.xlsx')) as writer:  
    temp.to_excel(os.path.join(week_18_matchups_folder,str('Week 18 -'+away_team+'_AT_'+home_team+'.xlsx')), sheet_name=str(away_team+'_AT_'+home_team ))
    return str(away_team +' @ '+home_team + ' Exported')
output_matchups('IND','BUF')
output_matchups('BAL','TEN')
output_matchups('LA','SEA')
output_matchups('TB','WAS')
output_matchups('CHI','NO')
output_matchups('CLE','PIT')


    
##############################################################
#       Output historical data for training models
##############################################################






#output game df for historical data
historical_data = readin_historical(2019)
game_df_historical = create_game_df(historical_data)
historical_final_game_avgs_df = create_final_game_avgs_df(historical_data,Add_new_week_sched='No',include_upcoming_week='No')
game_df_avgs_historical = prep_game_df_for_avgs(game_df_historical,'No')
season_avgs_historical = get_season_avgs_by_game(game_df_avgs_historical, include_upcoming_week='No') 



#####################################################################
# Output current data to get this weeks predictions
#####################################################################




#save as excel
# check = season_avgs_historical.head(10000)
# game_df_avgs_historical2 = game_df_avgs_historical[game_df_avgs_historical['season'] < 2020]
# season_avgs_historical2 = season_avgs_historical.copy()
# season_avgs_historical2.reset_index(inplace = True)
# season_avgs_historical2 = season_avgs_historical2[season_avgs_historical2['season'] < 2020]
#game_season_final2.to_excel(os.path.join(clean_data_folder,'Historical Game DF averages 1999-2019.xlsx'))
#game_df_historical.to_excel(os.path.join(clean_data_folder,'Historical Game DF 1999-2019.xlsx'))
# season_avgs_historical2.to_excel(os.path.join(clean_data_folder,'Historical Season Avgs by Game 1999-2019.xlsx'))



