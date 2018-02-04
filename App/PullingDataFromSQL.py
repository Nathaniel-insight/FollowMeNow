#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 16:45:01 2018

@author: nathaniel

data base exists now pull data from it
"""


from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import psycopg2
import pandas as pd
from App.get_all_followers import datetime_to_day
import numpy as np


con = None

con = psycopg2.connect(dbname = 'twitchdb', user = 'postgres', password = 'kazenoaijo6', host = 'localhost', port = '5432' )

#sql_qry = """ SELECT * FROM broadcasters_table """

#broadcasters_table = pd.read_sql_query(sql_qry, con)

#followers_table = pd.read_sql_query(""" SELECT * FROM followers_table """, con)

#all_followers = pd.read_sql_query(""" SELECT * FROM "All_followers_table" """, con)

#return followers from user id
def get_followers_sql(user_id = 56744242):
    
    qry = """ SELECT * FROM "All_followers_table" WHERE to_id = '{}' """.format(user_id)

    followers = pd.read_sql_query(qry,con)
    total_followers = len(followers.drop_duplicates(subset= ['from_id','to_id']))
    
    return followers, total_followers

#return follower_totals table from twitchdb
def get_follower_table():
    
    qry = """ SELECT * FROM followers_totals_table """
    
    totals = pd.read_sql_query(qry,con)
    
    return totals

#get broadcaster ids within a certain range of follower_totals
def get_broadcaster_in_range(total_followers_range, follower_totals_df):
    
    min_range = min(total_followers_range)
    max_range = max(total_followers_range)
    
    broadcaster_id = follower_totals_df[(follower_totals_df['follower_totals'] < max_range) &(follower_totals_df['follower_totals'] > min_range) ]
    
    return broadcaster_id

#create dataframe with broadcaster_ids as rows and hourly follows as features
def create_broadcaster_df(stream_ids, time_axis = 'hour', normed = True):
    
    hours_df = pd.DataFrame()
    
    for stream in stream_ids:
        
        followers, total_followers = get_followers_sql(stream)
        
        followers_df = datetime_to_day(followers)
        
        count, division = np.histogram(followers_df[time_axis], bins = 24, range = [0,24], normed = normed)
        
        data = count.tolist() + [total_followers]
        
        hours_df[stream] = data
           
    hours_df = hours_df.transpose()
    
    hours_df = hours_df.rename(index=str, columns = {24:'total_followers'})
        
    return hours_df
        
#get broadcaster ids who stream game x

def get_broadcaster_in_game(game_id = 488552):
    
    qry = """SELECT user_id, game_id FROM broadcasters_table WHERE game_id = '{}'""".format(game_id)
     
    broadcasters = pd.read_sql_query(qry, con)
    
    unique_broadcasters = broadcasters.drop_duplicates(subset = 'user_id')
    
    return unique_broadcasters
    

    
    
