# NFL

This purpose of this project is to use play by play data from the nflfastR package to predict winners, scores, and total points scored for NFL games. If accurate, these predictions could recommend moneyline, spread, and over-under wagers. Currently,  the models file is still a work in progress and  is currently only for predicting the winner. The project is split in to multiple files: Read_In, Gather_and_Clean, Output_Chart_Data, Upcoming Week Schedule and Models. 

Upcoming Week Schedule;
- This is an excel file where you should enter the information for upcoming games. By doing so and setting Add_new_week_sched to "Yes", we can feed the season averages to the models and gather predictions for those games

Read_In:
- This file contains functions to pull data from the nflfastR github page, during a defined time period, and create one file.
    - Use readin_historical() to read in every season up to and including the input year.
    - Use readin_current_year() to read in the current season

Gather and Clean:
- This file contains functions that clean the play by play data and create averages for many statistics by game, which is used to calculate season averages for each team going in to each week.
    - Use create_game_df() to create dataframe with averages by game for each team
    - Use prep_game_df_for_avgs(df, Add_new_week_sched) as an input for the functions that create season averages(below)
        - df should be output from create_game_df(), Add_new_week_sched() should be a "Yes" or "No" for whether you would like to merge on the upcoming week schedule to predict upcoming games
    - Use get_season_avgs_by_game(game_df4,include_upcoming_week) to create a dataframe with season averages going in to each week for each team
        - game_df4 should be output from prep_game_df_for_avgs()
        - include_upcoming_week should be a "Yes" or "No" for whether you would like the averages to be inclusive of the current week. EX: week 2 stats will be averages from week1 and week 2
    - Use create_final_game_avgs_df(df,Add_new_week_sched,include_upcoming_week) to run all required functions to output season averages by week for each matcup for ML models
        - df should be play by play data
        - Add_new_week_sched is for prep_game_df_for_avgs()
        - include_upcoming_week is for get_season_avgs_by_game()
        
Output Chart Data
 - This file is used to output statistics for charts
    - change file paths and use to Update_season_charts(year) to update file
    - Vizualizations can be seen here: https://public.tableau.com/profile/nate.balson#!/
