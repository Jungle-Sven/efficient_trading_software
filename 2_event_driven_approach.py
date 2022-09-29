from datetime import datetime, timedelta
from queue import Queue
import time

class Event(object):
    #base class
    pass

class AccountUpdateEvent(Event):
    def __init__(self, timestamp, username, total):
        self.type = 'ACCOUNT_UPDATE'
        self.timestamp = timestamp
        self.username = username
        self.total = float(total)

class GetAccountData:
    #this is an example data input class
    #it can make an api call, read response and create an event
    def __init__(self, events):
        self.events = events

    def run(self):
        self.api_call()
        self.read_data()
        self.create_event()

    def api_call(self):
        #makes an api call and returns response
        pass
    def read_data(self):
        #reads response and returns total account value
        pass
    def create_event(self, timestamp, username, total):
        #this func adds events to events queue
        self.events.put(AccountUpdateEvent(timestamp=timestamp, username=username, total=total))

    def test(self):
        #we will randomly create some events here
        self.create_event(datetime.utcnow(), 'user1', 12345)
        time.sleep(1)
        self.create_event(datetime.utcnow(), 'user2', 346776)
        time.sleep(1)
        self.create_event(datetime.utcnow(), 'user3', 1989.1)
        pass

class ProcessAccountData:
    #this class processes account update events
    #in this example it will have only one func to read and save data from event
    def __init__(self):
        pass

    def update_account_data(self, event):
        print('Account update recieved at: ', event.timestamp, 'Username: ', event.username, 'Total balance: ', event.total)
        #save data here

class App:
    #this is our app
    #this app reacts to events
    def __init__(self):
        self.events = Queue()
        self.get_data_account = GetAccountData(self.events)
        self.process_data_account = ProcessAccountData()

    def run_updates(self):
        self.get_data_account.test()

    def event_loop(self):
        #this is infinitive loop of events
        #in real app this func will be async,
        #but now for educational purposes we will use synchronous programming
        while True:
            try:
                event = self.events.get(False)
            except Exception as e:
                time.sleep(1)
                break

            else:
                try:
                    if event is not None:
                        if event.type == 'ACCOUNT_UPDATE':
                            self.process_data_account.update_account_data(event)

                except Exception as e:
                    print('event_loop error:', e)

    def run(self):
        #in real app this func will be async,
        #but now for educational purposes we will use synchronous programming
        while True:
            self.run_updates()
            self.event_loop()

if __name__ == '__main__':
    app= App()
    app.run()
