import time
from abc import ABCMeta, abstractmethod
import pandas as pd
import os

from pymongo import MongoClient
from arctic import Arctic
from icecream import ic
import sqlite3
import matplotlib.pyplot as plt
import psycopg2
from configparser import ConfigParser
from sqlalchemy import create_engine

from synthetic_data_generator import DataGenerator

'''we will use synthetic_data_generator described in blog post 4
to create our dataset'''

class Connector:
    '''contains database connector info like filename, password, etc '''
    pass

class ReaderApp:
    __metaclass__ = ABCMeta

    @abstractmethod
    def read_all_data(self):
        '''reads all data from file/database'''
        raise NotImplementedError("Should implement read_all_data()")

    @abstractmethod
    def read_query(self):
        '''makes a specific query to file/database '''
        raise NotImplementedError("Should implement read_query()")

class WriterApp:
    __metaclass__ = ABCMeta

    @abstractmethod
    def append_data(self):
        '''saves data line by line, like in real app '''
        raise NotImplementedError("Should implement save_data()")

    @abstractmethod
    def bulk_save_data(self):
        '''save all data at one time '''
        raise NotImplementedError("Should implement bulk_save_data()")

class ServiceApp:
    __metaclass__ = ABCMeta

    @abstractmethod
    def clear_database(self):
        raise NotImplementedError("Should implement clear_database()")

    @abstractmethod
    def size_of_database(self):
        raise NotImplementedError("Should implement size_of_database()")

class ConnectorCSV:
    def __init__(self):
        self.filename='db/btcusd.csv'

class ReaderAppCSV(ReaderApp, ConnectorCSV):
    def read_all_data(self):
        df = pd.read_csv(self.filename)
        return df

    def read_query(self):
        df = pd.read_csv(self.filename)
        df.columns = ['timestamp', 'symbol', 'side', 'amount', 'price', 'exchange']
        result = df[df['price'] < 20155]
        return result

class WriterAppCSV(WriterApp, ConnectorCSV):
    def append_data(self, data):
        for i in range(len(data)):
            data.loc[[i]].to_csv(self.filename, index=False, header=False, mode='a')

    def bulk_save_data(self, data):
        data.to_csv(self.filename, index=False, header=False)

class ServiceAppCSV(ServiceApp, ConnectorCSV):
    def clear_database(self):
        df = pd.DataFrame([])
        df.to_csv(self.filename, index=False, header=False)

    def size_of_database(self):
        return os.path.getsize(self.filename)

class ConnectorSQLite:
    def __init__(self):
        self.filename = 'db/trades.db'

    def create_table_trades(self):
        conn = sqlite3.connect(self.filename)
        conn.execute('''CREATE TABLE if not exists BTCUSD
                 (timestamp FLOAT NOT NULL,
                  symbol TEXT NOT NULL,
                  side TEXT NOT NULL,
                  amount FLOAT NOT NULL,
                  price FLOAT NOT NULL,
                  exchange TEXT NOT NULL
                  );''')
        conn.close()

class ReaderAppSQLite(ConnectorSQLite, ReaderApp):
    def to_db(self, data):
        return pd.DataFrame(data, columns = ['timestamp', 'symbol', 'side', 'amount', 'price', 'exchange'])

    def read_all_data(self):
        conn = sqlite3.connect(self.filename)
        cursor = conn.execute("SELECT * FROM BTCUSD;")
        result = cursor.fetchall()
        return self.to_db(result)

    def read_query(self):
        conn = sqlite3.connect(self.filename)
        cursor = conn.execute("SELECT * FROM BTCUSD WHERE price < 20155;")
        result = cursor.fetchall()
        return result

class WriterAppSQLite(ConnectorSQLite, WriterApp):
    def append_data(self, data):
        '''appends data row by row '''
        data = data.to_dict('records')
        for i in data:
            conn = sqlite3.connect(self.filename)
            conn.execute("INSERT INTO BTCUSD (timestamp, symbol, side, amount, price, exchange) VALUES (?, ?, ?, ?, ?, ?)", (i['timestamp'], i['symbol'], i['side'], i['amount'], i['price'], i['exchange']));
            conn.commit()
            conn.close()

    def bulk_save_data(self, data):
        '''saves all data at 1 time '''
        conn = sqlite3.connect(self.filename)
        data.to_sql('BTCUSD', conn, if_exists='replace', index=False)

class ServiceAppSQLite(ConnectorSQLite, ServiceApp):
    def clear_database(self):
        os.remove(self.filename)

        self.create_table_trades()

    def size_of_database(self):
        return os.path.getsize(self.filename)

class ConnectorPostgres:
    def __init__(self):
        self.connection_parameters = {'database': 'ticks',
                                 'host': 'localhost',
                                 'password': 'Pb2RoVH11OQLWo56NiLy',
                                 'port': '5432',
                                 'user': 'postgres'}

    def execute(self, command:str, data=None, fetch=False, executemany=False):
        '''executes a database query '''
        conn = None
        result = None
        try:
            conn = psycopg2.connect(**self.connection_parameters)
            cur = conn.cursor()
            if fetch:
                cur.execute(command)
                result = cur.fetchall()
            else:
                if executemany:
                    asd = cur.executemany(command, data)
                else:
                    cur.execute(command, data)
            cur.close()
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()
            return result

    def create_table(self):
        q = """
        CREATE TABLE btcusd (
            id BIGSERIAL PRIMARY KEY,
            timestamp FLOAT(4) NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            amount FLOAT NOT NULL,
            price FLOAT NOT NULL,
            exchange TEXT NOT NULL
        )
        """
        self.execute(q, data=None, fetch=False, executemany=False)


class ReaderAppPostgres(ConnectorPostgres, ReaderApp):
    def read_all_data(self):
        q = "select * from btcusd"
        data = self.execute(q, data=None, fetch=True, executemany=False)
        return data

    def read_query(self):
        q = "select * from btcusd where price < 20155"
        data = self.execute(q, data=None, fetch=True, executemany=False)
        return data

class WriterAppPostgres(ConnectorPostgres, WriterApp):
    def bulk_save_data(self, data, fetch=False):
        engine = create_engine('postgresql+psycopg2://postgres:Pb2RoVH11OQLWo56NiLy@localhost:5432/ticks')
        data.to_sql('btcusd', engine, if_exists='replace', index=False)

    def append_data(self, data):
        data = data.values.tolist()
        q = "INSERT INTO btcusd (timestamp, symbol, side, amount, price, exchange) VALUES (%s, %s, %s, %s, %s, %s)"
        for line in data:
            self.execute(q, line, fetch=False, executemany=False)

class ServiceAppPostgres(ConnectorPostgres, ServiceApp):
    def clear_database(self):
        q = 'DROP table IF EXISTS "btcusd"'
        self.execute(q, data=[], fetch=False, executemany=False)

        self.create_table()

    def size_of_database(self):
        q = "SELECT pg_database_size('ticks');"
        size = self.execute(q, data=[], fetch=True, executemany=False)
        return size[0][0]

class ConnectorMongo:
    def __init__(self):
        self.client = MongoClient('localhost', 27017)
        self.db = self.client.mongo


class ReaderAppMongo(ConnectorMongo, ReaderApp):
    def read_all_data(self):
        list_of_results = []
        for i in self.db.btcusd.find({}):
            list_of_results.append(i)
        return pd.DataFrame(list_of_results)

    def read_query(self):
        list_of_results = []
        for i in self.db.btcusd.find({'price': {'$lt': 20155}}):
            list_of_results.append(i)
        return pd.DataFrame(list_of_results)

class WriterAppMongo(ConnectorMongo, WriterApp):
    def append_data(self, data):
        '''appends data row by row '''
        data = data.to_dict('records')
        collection = self.db.btcusd
        for i in data:
            collection.insert_one(i)

    def bulk_save_data(self, data):
        data = data.to_dict('records')
        '''saves all data at 1 time'''
        collection = self.db.btcusd
        collection.insert_many(data)

class ServiceAppMongo(ConnectorMongo, WriterApp):
    def clear_database(self):
        self.db.btcusd.remove({})

    def size_of_database(self):
        return self.db.command("collstats", "btcusd")['size']

class ConnectorArctic:
    def __init__(self):
        self.client = Arctic('localhost')
        self.db = self.client['btcusd']

class ReaderAppArctic(ConnectorArctic, ReaderApp):
    def read_all_data(self):
        item = self.db.read('btcusd')
        return pd.DataFrame(item.data)

    def read_query(self):
        item = self.db.read('btcusd')
        df = item.data
        result = df[df['price'] < 20155]
        return result

class WriterAppArctic(ConnectorArctic, WriterApp):
    def bulk_save_data(self, data):
        '''saves all data as 1 item to VersionStore '''
        self.db.write('btcusd', data, metadata={'source': 'synthetic_data_generator'})

    def append_data(self, data):
        '''appends data row by row to VersionStore'''
        for i in data:
            self.db.write(i, self.db.read('btcusd').data)

class ServiceAppArctic(ConnectorArctic, WriterApp):
    def clear_database(self):
        self.client.delete_library('btcusd')
        self.client.initialize_library('btcusd')

    def size_of_database(self):
        stats = self.db.stats()
        size = stats['totals']['size']
        ic(stats['totals'])
        return size

class MultiDatabaseApp:
    def __init__(self):
        self.generator = DataGenerator()

        self.csv_storage = WriterAppCSV()
        self.sqlite_storage = WriterAppSQLite()
        self.postgres_storage = WriterAppPostgres()
        self.mongo_storage = WriterAppMongo()
        self.arctic_storage = WriterAppArctic()

        self.csv_reader = ReaderAppCSV()
        self.sqlite_reader = ReaderAppSQLite()
        self.postgres_reader = ReaderAppPostgres()
        self.mongo_reader = ReaderAppMongo()
        self.arctic_reader = ReaderAppArctic()

        self.csv_service = ServiceAppCSV()
        self.sqlite_service = ServiceAppSQLite()
        self.postgres_service = ServiceAppPostgres()
        self.mongo_service = ServiceAppMongo()
        self.arctic_service = ServiceAppArctic()

    def get_data_ticks(self, dataset_size):
        return self.generator.run_ticks(dataset_size)

    def get_data_ohlcv(self, dataset_size):
        return self.generator.run_ohlcv(dataset_size)

    def execution_time(func):
        #measures a database operation execution time
        def measure_time(*args, **kwargs) -> dict:
            start_time = time.time()
            response = func(*args, **kwargs)
            execution_time = time.time() - start_time
            response['execution_time'] = execution_time
            return response
        return measure_time

    def plot_results(self, df):
        df.to_html('temp.html')
        df.plot(kind = 'bar', x='action', fontsize=20, \
            y=['csv', 'sqlite', 'postgres', 'mongo', 'arctic'], rot=0, title='Execution time')
        plt.show()

    def plot_hist(self, list_of_res):
        df = pd.DataFrame([list_of_res], index=[''])
        df.plot(kind = 'bar', fontsize=20, title=list_of_res['action'], \
            y=['csv', 'sqlite', 'postgres', 'mongo', 'arctic'])
        plt.show()

    @execution_time
    def csv_storage_append(self, data:pd.DataFrame) -> dict:
        response = self.csv_storage.append_data(data)
        return {'name': 'csv_storage_append', 'result': response}

    @execution_time
    def sqlite_storage_append(self, data:pd.DataFrame) -> dict:
        response = self.sqlite_storage.append_data(data)
        return {'name': 'sqlite_storage_append', 'result': response}

    @execution_time
    def postgres_storage_append(self, data:pd.DataFrame) -> dict:
        response = self.postgres_storage.append_data(data)
        return {'name': 'postgres_storage_append', 'result': response}

    @execution_time
    def mongo_storage_append(self, data:pd.DataFrame) -> dict:
        response = self.mongo_storage.append_data(data)
        return {'name': 'mongo_storage_append', 'result': response}

    @execution_time
    def arctic_storage_append(self, data:pd.DataFrame) -> dict:
        response = self.arctic_storage.append_data(data)
        return {'name': 'arctic_storage_append', 'result': response}

    def append_data(self, dataset):
        action = 'append_data'
        '''choose ticks or ohlcv '''
        data = self.get_data_ticks(dataset)
        #data = self.get_data_ohlcv(dataset)

        csv = self.csv_storage_append(data)
        sqlite = self.sqlite_storage_append(data)
        postgres = self.postgres_storage_append(data)
        mongo = self.mongo_storage_append(data)
        arctic = self.arctic_storage_append(data)

        return {'action': action, 'dataset': dataset,
            'csv': csv['execution_time'],
            'sqlite': sqlite['execution_time'],
            'postgres': postgres['execution_time'],
            'mongo': mongo['execution_time'],
            'arctic': arctic['execution_time']}

    @execution_time
    def csv_storage_bulk(self, data:pd.DataFrame) -> dict:
        response = self.csv_storage.bulk_save_data(data)
        return {'name': 'csv_storage_bulk', 'result': response}

    @execution_time
    def sqlite_storage_bulk(self, data:pd.DataFrame) -> dict:
        response = self.sqlite_storage.bulk_save_data(data)
        return {'name': 'sqlite_storage_bulk', 'result': response}

    @execution_time
    def postgres_storage_bulk(self, data:pd.DataFrame) -> dict:
        response = self.postgres_storage.bulk_save_data(data)
        return {'name': 'postgres_storage_bulk', 'result': response}

    @execution_time
    def mongo_storage_bulk(self, data:pd.DataFrame) -> dict:
        response = self.mongo_storage.bulk_save_data(data)
        return {'name': 'mongo_storage_bulk', 'result': response}

    @execution_time
    def arctic_storage_bulk(self, data:pd.DataFrame) -> dict:
        response = self.arctic_storage.bulk_save_data(data)
        return {'name': 'arctic_storage_bulk', 'result': response}

    def bulk_save_data(self, dataset):
        action = 'bulk_save_data'
        '''choose ticks or ohlcv '''
        data = self.get_data_ticks(dataset)
        #data = self.get_data_ohlcv(dataset)

        csv = self.csv_storage_bulk(data)
        sqlite = self.sqlite_storage_bulk(data)
        postgres = self.postgres_storage_bulk(data)
        mongo = self.mongo_storage_bulk(data)
        arctic = self.arctic_storage_bulk(data)

        return {'action': action, 'dataset': dataset,
            'csv': csv['execution_time'],
            'sqlite': sqlite['execution_time'],
            'postgres': postgres['execution_time'],
            'mongo': mongo['execution_time'],
            'arctic': arctic['execution_time']}

    @execution_time
    def csv_reader_read_all_data(self) -> dict:
        response = self.csv_reader.read_all_data()
        return {'name': 'csv_read_all_data', 'result': response}

    @execution_time
    def sqlite_reader_read_all_data(self) -> dict:
        response = self.sqlite_reader.read_all_data()
        return {'name': 'sqlite_storage_read_all_data', 'result': response}

    @execution_time
    def postgres_reader_read_all_data(self) -> dict:
        response = self.postgres_reader.read_all_data()
        return {'name': 'postgres_reader_read_all_data', 'result': response}

    @execution_time
    def mongo_reader_read_all_data(self) -> dict:
        response = self.mongo_reader.read_all_data()
        return {'name': 'mongo_storage_read_all_data', 'result': response}

    @execution_time
    def arctic_reader_read_all_data(self) -> dict:
        response = self.arctic_reader.read_all_data()
        return {'name': 'arctic_storage_read_all_data', 'result': response}

    def read_all_data(self, dataset):
        action = 'read_all_data'

        csv = self.csv_reader_read_all_data()
        sqlite = self.sqlite_reader_read_all_data()
        postgres = self.postgres_reader_read_all_data()
        mongo = self.mongo_reader_read_all_data()
        arctic = self.arctic_reader_read_all_data()

        return {'action': action, 'dataset': dataset,
            'csv': csv['execution_time'],
            'sqlite': sqlite['execution_time'],
            'postgres': postgres['execution_time'],
            'mongo': mongo['execution_time'],
            'arctic': arctic['execution_time']}

    @execution_time
    def csv_reader_read_query(self) -> dict:
        response = self.csv_reader.read_query()
        return {'name': 'csv_reader_read_query', 'result': response}

    @execution_time
    def sqlite_reader_read_query(self) -> dict:
        response = self.sqlite_reader.read_query()
        return {'name': 'sqlite_storage_read_query', 'result': response}

    @execution_time
    def postgres_reader_read_query(self) -> dict:
        response = self.postgres_reader.read_query()
        return {'name': 'postgres_reader_read_query', 'result': response}

    @execution_time
    def mongo_reader_read_query(self) -> dict:
        response = self.mongo_reader.read_query()
        return {'name': 'mongo_storage_read_query', 'result': response}

    @execution_time
    def arctic_reader_read_query(self) -> dict:
        response = self.arctic_reader.read_query()
        return {'name': 'arctic_storage_read_query', 'result': response}

    def read_query(self, dataset):
        action = 'read_query'

        csv = self.csv_reader_read_query()
        sqlite = self.sqlite_reader_read_query()
        postgres = self.postgres_reader_read_query()
        mongo = self.mongo_reader_read_query()
        arctic = self.arctic_reader_read_query()

        return {'action': action, 'dataset': dataset,
            'csv': csv['execution_time'],
            'sqlite': sqlite['execution_time'],
            'postgres': postgres['execution_time'],
            'mongo': mongo['execution_time'],
            'arctic': arctic['execution_time']}

    def clear_databases(self):
        self.csv_service.clear_database()
        self.sqlite_service.clear_database()
        self.postgres_service.clear_database()
        self.mongo_service.clear_database()
        self.arctic_service.clear_database()

        ic('all databases cleared')

    def size_of_databases(self):
        action = 'size_of_databases'
        dataset = 1000000

        csv = self.csv_service.size_of_database()
        sqlite = self.sqlite_service.size_of_database()
        postgres = self.postgres_service.size_of_database()
        mongo = self.mongo_service.size_of_database()
        arctic = self.arctic_service.size_of_database()

        #to find size in MB
        in_mb = 1000000

        return {'action': action,
            'dataset': dataset,
            'csv': csv / in_mb,
            'sqlite': sqlite / in_mb,
            'postgres': postgres / in_mb,
            'mongo': mongo / in_mb,
            'arctic': arctic / in_mb}

    def run_all_tests(self):
        list_of_results = []
        datasets = (1000, 1000000)

        self.clear_databases()

        res = self.bulk_save_data(dataset = 1000000)
        list_of_results.append(res)
        #self.plot_hist(res)

        res = self.append_data(dataset = 1000)
        list_of_results.append(res)
        #self.plot_hist(res)

        res = self.read_all_data(dataset = 1000000)
        list_of_results.append(res)
        #self.plot_hist(res)

        res = self.read_query(dataset = 1000000)
        list_of_results.append(res)
        #self.plot_hist(res)

        res = self.size_of_databases()
        list_of_results.append(res)
        #self.plot_hist(res)

        df = pd.DataFrame(list_of_results)
        ic(df)

        self.plot_results(df)

if __name__ == '__main__':
    app = MultiDatabaseApp()
    app.run_all_tests()
