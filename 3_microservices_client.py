from datetime import datetime
import requests

#constants
#api url is localhost
API_URL = "http://localhost:8080/api/orders"

class API_CLIENT:
    def __init__(self, api_url):
        self.api_url = api_url

    def generate_order(self):
        #this func generates an order for testing purposes
        order = {'timestamp': str(datetime.utcnow()),
                'username': 'username',
                'market': 'BTC/USD',
                'side': 'BUY',
                'size': 0.345,
                'price': 18950
                }
        return order

    def generate_request(self, order):
        try:
            response = requests.post(self.api_url, json=order)
            print(response)
        except Exception as e:
            print('generate_request - Exception', e)

    def run(self):
        order = self.generate_order()
        self.generate_request(order)


if __name__ == '__main__':
    #this runs API CLIENT and makes 1 API call to our SERVER
    #order data will be saved in database
    client = API_CLIENT(API_URL)
    client.run()
