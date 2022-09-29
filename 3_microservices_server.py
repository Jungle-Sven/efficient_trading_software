import os
import sqlite3

from flask import Flask, request
from flask_restful import Resource, Api
from flask_jsonpify import jsonify

app = Flask(__name__)
api = Api(app)

#constants
#api url is localhost
API_URL = "0.0.0.0"
API_PORT = 8080

class Database:
    #this is a db connector
    #we will use sqlite in this example for simplicity
    def __init__(self):
        #filename and path to database are just hardcoded for simplicity
        self.connect_to = 'test.db'

    def create_table_orders(self):
        #a func to create our database
        conn = sqlite3.connect(self.connect_to)
        conn.execute('''CREATE TABLE if not exists Orders
                 (timestamp TEXT NOT NULL,
                  username TEXT NOT NULL,
                  market TEXT NOT NULL,
                  side TEXT NOT NULL,
                  size FLOAT NOT NULL,
                  price FLOAT NOT NULL
                  );''')
        conn.close()

    def add_data_orders(self, timestamp, username, market, side, size, price):
        #a func to save orders data
        conn = sqlite3.connect(self.connect_to)
        conn.execute("INSERT INTO Orders (timestamp, username, market, side, size, price) VALUES (?, ?, ?, ?, ?, ?)", (timestamp, username, market, side, size, price));
        conn.commit()
        conn.close()

db = Database()
#this func will create Orders table inside database if it does not exist
db.create_table_orders()

class API_SERVER(Resource):
    #this is basic API server
    def __init__(self):
        pass

    @app.post("/api/orders")
    def save_orders():
        if request.is_json:
            response = request.get_json()
            db.add_data_orders(response['timestamp'], response['username'], response['market'], response['side'], response['size'], response['price'])
            return response, 201
        return {"error": "Request must be JSON"}, 415

api.add_resource(API_SERVER, '/api/orders')

if __name__ == '__main__':
    #this runs API SERVER
    from waitress import serve
    serve(app, host=API_URL, port=API_PORT)
