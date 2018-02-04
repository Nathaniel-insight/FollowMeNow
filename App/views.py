from flask import render_template
from flask import request
from App import app
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
import pandas as pd
import psycopg2
from App.a_Model import ModelIt, get_image_url

user = 'postgres' #add your username here (same as previous postgreSQL)   
password = 'kazenoaijo6'
port = '5432'
host = 'localhost'                   
dbname = 'twitchdb'
engine = create_engine( 'postgresql://{}:{}@{}:{}/{}'.format(user, password, host, port, dbname) )
con = None
con = psycopg2.connect(database = dbname, user = user, host=host, password=password)

@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html",
       title = 'FollowMeNow', user = { 'nickname': 'Miguel' },
       )

@app.route('/db')
def birth_page():
    sql_query = """                                                                       
                SELECT * FROM followers_table WHERE to_id='148519305';          
                """
    query_results = pd.read_sql_query(sql_query,con)
    followed_at = ""
    for i in range(0,10):
        followed_at += query_results.iloc[i]['followed_at']
        followed_at += "<br>"
    return followed_at

@app.route('/db_fancy')
def cesareans_page_fancy():
    sql_query = """
               SELECT followed_at, from_id, to_id FROM followers_table WHERE to_id='148519305';
                """
    query_results=pd.read_sql_query(sql_query,con)
    followed = []
    for i in range(0,10):
        followed.append(dict(followed_at=query_results.iloc[i]['followed_at'], from_id=query_results.iloc[i]['from_id'], to_id=query_results.iloc[i]['to_id']))
    return render_template('cesareans.html',followed=followed)

@app.route('/input')
def cesareans_input():
    return render_template("input.html")

@app.route('/output')
def cesareans_output():
    #pull 'user_id' from input field and store it
    user_id = request.args.get('user_id')
    the_result = ModelIt(user_id = user_id)
    figure_url = get_image_url(user_id)	
    #just select the Cesareans  from the birth dtabase for the month that the user inputs
    #query = """SELECT followed_at, from_id, to_id FROM followers_table WHERE to_id='%s'""" % user_id
    #print(query)
    #query_results=pd.read_sql_query(query,con)
    #print(query_results)
    #followed = []
    #for i in range(0,10):
    #    followed.append(dict(followed_at=query_results.iloc[i]['followed_at'], from_id=query_results.iloc[i]['from_id'], to_id=query_results.iloc[i]['to_id']))
    return render_template("output.html", the_result = the_result, figure_url = figure_url)


