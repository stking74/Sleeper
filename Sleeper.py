# -*- coding: utf-8 -*-

import esipy as api
import datetime
import pickle
import os
import yaml

print('Initiailizing app...')
esi_app = api.EsiApp()
print('Updating swagger...')
app = esi_app.get_latest_swagger
print('Initializing client...')
client = api.EsiClient(
        retry_requests=True,
        headers={'User-Agent':'Sleeper \\ <0.0.1>'},
        raw_body_only=False)

class Sleeper(object):
    
    def __init__(self, app, client):
        self.app = app
        self.client = client
        return
    
    def _update_region_list(self):
        
        print('Refreshing region metadata...')
        operation = self.app.op['get_universe_regions']()
        response = self.client.request(operation)
        region_ids = response.data
        self.region_list = {}
        i = 1
        for region_id in region_ids:
            print(f'{i}/{len(region_ids)}')
            operation = self.app.op['get_universe_regions_region_id'](
                region_id=region_id)
            response = self.client.request(operation)
            region_name = response.data['name']
            self.region_list[region_name] = dict(response.data)
            for name, data in self.region_list.items():
                self.region_list[name]['constellations'] = list(self.region_list[name]['constellations'])
            i += 1
        return
    
    def market_dump(self):
        orders = {}
        print('Scraping market data')
        print('Region:')
        for region in self.region_list.items():
            name, region_data = region
            print(name)
            region_id = region_data['region_id']
            unpacked = []
            response = self._request_region_market_orders(
                    region_id,
                    order_type='all')
            for entry in response.data:
                unpacked.append(dict(entry))
            orders[name] = unpacked
        refresh_time = datetime.datetime.now()
        filename_timestamp = refresh_time.strftime('%c')
        os.chdir('data_dumps')
        pik_filename = 'market_dump-'+str(refresh_time)[:10]+'.pik'
        pik_file = open(pik_filename, 'wb')
        pickle.dump(orders, pik_file)
        pik_file.close()
        os.chdir('..')
        
        return
    
    def _request_region_market_orders(self,
            region_id=10000002,
            type_id=34,
            order_type='all',
            page=1):
        operation = self.app.op['get_markets_region_id_orders'](
                region_id=region_id,
                order_type=order_type)
        response = client.request(operation)
        return response
    
    def aggregate_data(self, data_directory):
        print('Aggregating order data...')
        data_directory_contents = os.listdir(data_directory)
        split_contents = [item.split('.') for item in data_directory_contents]
        target_file_list = []
        for idx, item in enumerate(split_contents):
            if item[1] == 'pik':
                target_file_list.append(data_directory_contents[idx])
        aggregated_raw_data = {}
        for file in target_file_list:
            pull_date = file[12:22]
            file = os.path.join(data_directory, file)
            with open(file, 'rb') as pik_file:
                pickle_data = pickle.load(pik_file)
            aggregated_raw_data[pull_date] = pickle_data
        
        print('Extracting order IDs...')
        order_catalog = {}
        for date in aggregated_raw_data.values():
            for region in date.values():
                for order in region:
                    order_id = str(order['order_id'])
                    if order_id in order_catalog.keys():
                        order_catalog[order_id].append(order)
                    else:
                        order_catalog[order_id] = [order]
        '''
        order_ids = list(set(order_ids))

        orders = {}
        for order_id in order_ids:
            new_key = str(order_id)
            orders[new_key] = {}
            
        print('Cataloging orders...')
        for idx, order_id in enumerate(orders.keys()):
            for date in aggregated_raw_data.values():
                for region in date.values():
                    for order in region:
                        #!!! This if condition below is not working as intended
                        if order['order_id'] == order_id:
                            orders[order_id].append(order)
            if idx % 1000 == 0:
                print(int(idx),'/',len(orders))
        '''
        return order_catalog
    
MarketDump = Sleeper(app, client)
MarketDump._update_region_list()
MarketDump.market_dump()
orders = MarketDump.aggregate_data('data_dumps')