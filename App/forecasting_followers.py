#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 15:58:51 2018

@author: nathaniel

first attempt at using keras and RNN to forecast
"""



import pandas as pd
import numpy as np
from fbprophet import Prophet
from App.get_all_followers import num_followers
from App.PullingDataFromSQL import get_followers_sql
from datetime import datetime
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error
pd.options.mode.chained_assignment = None 
plt.switch_backend('agg')

#df = pd.read_csv('./Data/all_follows_sample.csv')
#
#df_series = num_followers(df[['followed_at']])
#
#df_series_week = df_series.resample('W').sum()
#
##need to specify ds and y
#
#to_forecast = pd.DataFrame(df_series_week['follower_added'].values, columns = ['y'])
#
#to_forecast['ds'] = df_series_week.index
#
#forecast = Prophet()
#forecast.fit(to_forecast)
#
#future = forecast.make_future_dataframe(periods = 47)
#
#print(future.tail())


# apply fb Prophet and return fb object with forecasts
def apply_fb_prophet(user_id = 148519305, freq = 'D'):
    
    # get followers and create datetime series
    followers, total_followers = get_followers_sql(user_id = user_id)
    
    print('this user has {} total_followers'.format(total_followers))
    
    followers_series = num_followers(followers)
    
    if freq == 'D':
        series_sum = followers_series.resample('D').sum()
    elif freq == 'W':
        series_sum = followers_series.resample('W').sum()
    elif freq == 'H':
        series_sum = followers_series.resample('H').sum()
        
    hour_sum = followers_series.resample('H').sum()
    
    #get 2017 data
    beg_year = datetime(2017,1,1)
    end_year = datetime(2017,12,31)
    
    series_year_2017 = series_sum[(series_sum.index >= beg_year) & (series_sum.index <= end_year)]
    
    # create fb prophet friendly dataFrame
    pre_processed_df = pd.DataFrame(series_year_2017['follower_added'].values, columns = ['y'])
    pre_processed_df['ds'] = series_year_2017.index.values
    
    #create fb prophet object
    m = Prophet(yearly_seasonality = True, weekly_seasonality = True, daily_seasonality = True)
    m.fit(pre_processed_df)
    
    #make future dataframe
    future = m.make_future_dataframe(periods = 7)
    
    forecast = m.predict(future)
    
#    to_plot = pd.DataFrame(forecast[['ds','yhat','yhat_upper','yhat_lower']], columns = ['ds', 'fit','uppwer','lower'])
    
    
    return m, forecast, hour_sum, total_followers

# analyze a streamers forecast and return dataframe for use in FollowMeNow app
def analyze_forecast(user_id = 148519305 ):
    
    m, forecast, hour_sum, total_followers = apply_fb_prophet(user_id = user_id)
    
    forecast_date_end = forecast['ds'].iloc[-1]
    forecast_date_start = forecast['ds'].iloc[-7]
    
    forecast_range = (forecast['ds'] >= forecast_date_start) & (forecast['ds'] <= forecast_date_end)
    predicted_df = forecast[forecast_range]
    actual_range = (hour_sum.index >= forecast_date_start) & (hour_sum.index <= forecast_date_end)
    actual_df = hour_sum[actual_range]
    actual_df_day = actual_df.resample('D').sum()
    
    actual_gained = actual_df['follower_added'].sum()
    pred_gained = predicted_df['yhat'].sum()
    
    hours_streamed = len(actual_df[actual_df['follower_added'] != 0])
    
    dummy_result = {'actual_total': actual_gained, 'pred_total': pred_gained,'hours_streamed':hours_streamed}
    the_result = pd.DataFrame(dummy_result, index = range(0,1))

    #Return MASE = MAE_pred / MAE_fit
    mae_pred = mean_absolute_error(actual_df_day['follower_added'].values,predicted_df['yhat'].values)
    
    beg_year = datetime(2017,1,1)
    end_year = datetime(2017,12,31)
    fit_data = hour_sum.resample('D').sum()
    fit_data = fit_data[(fit_data.index >= beg_year) & (fit_data.index <= end_year)]
    fit_fit = forecast.iloc[0:-7,:]

    mean_pred = fit_data['follower_added'].mean()
    predicted_df['ave'] = mean_pred
    mae_ave = mean_absolute_error(actual_df_day['follower_added'].values, predicted_df['ave'].values)

    
    mae_fit = mean_absolute_error(fit_data, fit_fit['yhat'])

    better_than_average = mae_pred/mae_ave
    
    the_result['better_than_average'] = better_than_average


    fig, ax = plt.subplots()
#    actual_df.plot(y = 'follower_added', ax = ax, label = 'Hourly Growth')
#    plt.legend()
    actual_df_day.plot(y = 'follower_added', ax = ax, label = 'Actual')
    plt.legend()
    predicted_df.plot(x = 'ds', y = 'yhat',ax = ax, label = 'Predicted')
    plt.legend()
    ax.set_ylabel('Followers Added')
    
    fig.savefig('./App/static/{}.png'.format(user_id))
    
    
    return the_result


















