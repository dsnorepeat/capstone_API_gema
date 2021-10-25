from datetime import datetime
from flask import Flask, request 
import sqlite3
import requests
from tqdm import tqdm
import json
import numpy as np
import pandas as pd

app = Flask(__name__)

@app.route('/')
def home():
    return 'Hello World'

################## stations_route #####################

@app.route('/stations/')
def route_all_stations(): 
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')                                           #station's dynamic route#
def route_stations_id(station_id): 
    conn = make_connection()
    stations = get_station_id(station_id, conn)
    return stations.to_json()

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)

    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result


################### stations_functions ##############

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection 

def get_station_id(station_id, conn):                                         #station dynamic route's func#
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""   
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'



##################### trips_route ####################

@app.route('/trips/')
def route_all_trips(): 
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()



@app.route('/trips/attach', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result

@app.route('/trips/average_duration') 
def route_average_duration():
    conn = make_connection()
    avg_dur = get_average_duration(conn)
    return avg_dur.to_json()

@app.route('/trips/goaround') 
def route_goaround_trips():
    conn = make_connection()
    go_arnd = get_goaround_trips(conn)
    return go_arnd.to_json()



@app.route('/trips/goaround/<id>')                                          
def route_goaround_id(id): 
    conn = make_connection()
    go_arnd_id = get_goaround_id(id, conn)
    return go_arnd_id.to_json()

################### trips_functions #################

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection 

def insert_into_trips(data, conn):
    query = f"""INSERT INTO trips values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_average_duration(conn):
    query = f"""SELECT id, AVG (duration_minutes) as  avg_duration from trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_goaround_trips(conn):
    query = f"""SELECT * from Trips WHERE trips.start_station_id like trips.end_station_id"""
    result = pd.read_sql_query(query, conn)
    return result

def get_goaround_id(id, conn):                                         
    query = f"""SELECT * FROM trips WHERE id = {id} AND trips.start_station_id like trips.end_station_id"""   
    result = pd.read_sql_query(query, conn)
    return result 



################# POST ENDPOINTS #####################

@app.route('/json', methods = ['POST']) #aslinya method = GET kalo nggak di state
def json_example(): 

    req = request.get_json(force=True,) # Parse the incoming Json data as Dictionary 

    name = req['name']
    age = req['age']
    address = req['address']

    return (f'''Hello {name}, your age is {age}, and your adress is in {address}
            ''')

################# FINAL POST ENDPOINTS #################

@app.route('/trips/rent', methods=['POST']) 
def route_rent_from_trips():
    # parse and transform incoming data into a tuple as we need 
    input_data = request.get_json()
    specified_date = input_data['x']

    conn = make_connection()
    result = insert_rent_from_trips(specified_date, conn)
    return result

def insert_rent_from_trips(input_data, conn):
    conn = make_connection()
    query = f"SELECT * FROM trips WHERE start_time LIKE ('{input_data}%')"
    selected_data = pd.read_sql_query(query, conn)

    result = selected_data.groupby('start_station_id').agg({
    'bikeid' : 'count', 
    'duration_minutes' : 'mean'}) 
    return result.to_json()

if __name__ == '__main__':
    app.run(debug=True, port=5000)
