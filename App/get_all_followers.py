#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 17:51:11 2018

@author: nathaniel
"""

import requests
import time
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

URL = 'https://api.twitch.tv/helix'

HEADERS = {'Client-ID' : '33tq1s5qdisvrqjdqe201logvxw7ys'}

#get all followers for one channel
def get_all_followers(user_id = '148519305'):
    
    #initialize requests parameters
    follower_url = URL + '/users/follows'
    cursor = ''
    follower_payload = {'to_id': user_id, 'first': 100, 'after': cursor}
    
    #use requests to get follower totals    
    followers = requests.get(follower_url, headers = HEADERS, params = follower_payload)
    follower_total = followers.json().get('total')
    cursor = followers.json().get('pagination')['cursor']
    time.sleep(2)
    print('this will take approximately {}s'.format(2*follower_total/100))
    
    
    all_followers = followers.json().get('data')
    
    #collect all followers
    for follow_iter in range(0, follower_total, 100):
        follower_payload = {'to_id': user_id, 'first': 100, 'after': cursor}
        print('\r you have collected {} followers out of {}'.format(follow_iter, follower_total), end = '\r')
        time.sleep(2.1) #limits total requests to 30/minuite
        temp_followers = requests.get(follower_url, headers = HEADERS, params = follower_payload)
        if temp_followers is None:
            print('\rYoure gonna have to wait a bit\r')
            time.sleep(90)
            temp_followers = requests.get(follower_url, headers = HEADERS, params = follower_payload)
        all_followers += temp_followers.json().get('data')
        cursor = temp_followers.json().get('pagination')['cursor']
        
    followers_df = pd.DataFrame(all_followers)
        
    return followers_df

#get top 100 streamers playing game x
def get_streams(game_id = 2083):
    
    streams_url = URL + '/streams'
    stream_payload = {'game_id' : str(game_id), 'first': 100, 'type': 'live'}
    
    streams_response = requests.get(streams_url, headers = HEADERS, params = stream_payload)
    streams_data = streams_response.json().get('data')
    
    streams_df = pd.DataFrame(streams_data)
    
    return streams_df
    
def save_streams(streams_df = pd.DataFrame()):
    
    username = 'postgres'
    password = 'kazenoaijo6'     # change this
    host     = 'localhost'
    port     = '5432'            # default port that postgres listens on
    db_name  = 'twitchdb'

    engine = create_engine( 'postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, db_name) )
    
    if not database_exists(engine.url):
        create_database(engine.url)
    
    print('\rSaving {} streams to {}'.format(len(streams_df), db_name))
    
    streams_df.to_sql('broadcasters_table', engine, if_exists = 'append')


#take data with datetime index and create necessary columns for future filtering 
#uneeded with resampling
def datetime_to_day(data = pd.DataFrame()):
    
    temp_df = data.copy()
    
    datetime_index = data.index

    temp_df['hour'] = datetime_index.hour
    temp_df['day_of_week'] = datetime_index.weekday
    temp_df['day_of_month'] = datetime_index.day
    temp_df['day_of_year'] = datetime_index.dayofyear
    temp_df['week_of_year'] = datetime_index.week
    temp_df['month_of_year'] = datetime_index.month
    temp_df['year'] = datetime_index.year
    temp_df['date'] = datetime_index.date
    
    return temp_df
        
#Return series data of followers user resampling to get aggregates
def num_followers(data = pd.DataFrame()):
    
    date_time_index = pd.DatetimeIndex(data = data['followed_at'])
    series_df = pd.DataFrame(index = date_time_index.values)
    series_df['follower_added'] = 1
    
    return series_df

#get all unique broadcasters and their total followers   
def get_broadcaster_list():

    con = None

    con = psycopg2.connect(dbname = 'twitchdb', user = 'postgres', password = 'kazenoaijo6', host = 'localhost', port = '5432' )

    sql_qry = """ SELECT  broadcaster_ids, follower_totals FROM followers_totals_table """

    followers_totals = pd.read_sql_query(sql_qry, con)
    unique = followers_totals.drop_duplicates(subset = ['broadcaster_ids'], keep = 'last')
    
    print('{} Unique broadcasters out of {} stream pulls'.format(len(unique), len(followers_totals)))
    
    return unique

#must pass user list with duplicates dropped

def get_all_broadcast_followers(user_df = pd.DataFrame):
    
    all_followers_in_list = user_df['follower_totals'].sum()
    
    print('this will take approximately {}s or {}m'.format(2*all_followers_in_list/100, 2*all_followers_in_list/60/100))
    
    user_list = user_df['broadcaster_ids']
    
    for user in user_list:
        
        followers = get_all_followers(user_id = user)
        
        save_followers_to_sql(followers, user_id = user)
        
        

#save follower list to sql database
    
def save_followers_to_sql(followers = pd.DataFrame(), user_id = 'test'):
    
    username = 'postgres'
    password = 'kazenoaijo6'     # change this
    host     = 'localhost'
    port     = '5432'            # default port that postgres listens on
    db_name  = 'twitchdb'

    engine = create_engine( 'postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, db_name) )
    
    if not database_exists(engine.url):
        create_database(engine.url)
    
    print('\rSaving {} followers from {} broadcaster to {}'.format(len(followers),user_id, db_name))
    
    followers.to_sql('All_followers_table', engine, if_exists = 'append')
    
    
    
    