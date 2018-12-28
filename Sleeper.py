# -*- coding: utf-8 -*-

import esipy as api
import datetime
import time
import numpy as np
import pickle
import os

def crawldir(topdir=[], ext='sxm'):
    '''
    Crawls through given directory topdir, returns list of all files with 
    extension matching ext
    '''
    import re
    fn = dict()
    for root, dirs, files in os.walk(topdir):
              for name in files:
              
                if len(re.findall('\.'+ext,name)):
                    addname = os.path.join(root,name)

                    if root in fn.keys():
                        fn[root].append(addname)

                    else:
                        fn[root] = [addname]    
    return fn

class Sleeper():
    
    def __init__(self):
        print('Initiailizing app...')
        self.esi_app = api.EsiApp()
        print('Updating swagger...')
        self.app = self.esi_app.get_latest_swagger
        print('Initializing client...')
        self.client = api.EsiClient(retry_requests=True, 
                                    headers={'User-Agent':'Sleeper \\ <0.3.0>'},
                                    raw_body_only=False)
        self.root_dir = os.getcwd()
        self.store_dir = os.path.join(self.root_dir, 'data_dumps')
        self.settings_fname = 'sleeper_settings.sl'
        return
    
    def _update_region_list(self):
        
        print('Refreshing region metadata...')
        operation = self.app.op['get_universe_regions']()
        response = self.client.request(operation)
        region_ids = response.data
        self.region_list = {}
        i = 1
        for region_id in region_ids:
#            print(f'{i}/{len(region_ids)}')
            operation = self.app.op['get_universe_regions_region_id'](
                region_id=region_id)
            response = self.client.request(operation)
            region_name = response.data['name']
            self.region_list[region_name] = dict(response.data)
            for name, data in self.region_list.items():
                self.region_list[name]['constellations'] = list(self.region_list[name]['constellations'])
            i += 1
        return self.region_list
    
    def market_dump(self):
        
        def _request_region_market_orders(region_id=10000002, type_id=34, order_type='all'):
            page = 1
            while True:
                operation = self.app.op['get_markets_region_id_orders'](region_id=region_id, order_type=order_type, page=page)
                response = self.client.request(operation)
                pull_time = datetime.datetime.now()
                time.sleep(0.25)
                if page == 1:
                    orders = [dict(entry) for entry in response.data]
                    for order in orders:
                        order['timestamps'] = pull_time
                    if len(orders)==0:
                        break
                    page += 1
                    continue
                else:
                    new_orders = [dict(entry) for entry in response.data]
                    for order in new_orders:
                        order['timestamps'] = pull_time
                    if len(new_orders) == 0:
                        break
                    for entry in new_orders:
                        orders.append(entry)
                page += 1
            return orders
        
        orders = {}
        print('Scraping market data')
        print('Region:')
        for region in self.region_list.items():
            
            name, region_data = region
            print(name)
            region_id = region_data['region_id']
            orders[name] = _request_region_market_orders(region_id, order_type='all')
        refresh_time = datetime.datetime.now()
        filename_timestamp = refresh_time.strftime('%c')
        os.chdir(self.store_dir)
        pik_filename = 'market_dump-'+str(refresh_time)[:10]+'.pik'
        pik_file = open(pik_filename, 'wb')
        pickle.dump(orders, pik_file)
        pik_file.close()
        os.chdir(self.root_dir)
        
        return
    
    
    
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
    
    def _load_settings_file_(self):
        dir_dump = crawldir(self.root_dir, 'sl')
        if len(dir_dump) == 0:
            self._new_settings_file_()
        return
    
    def _new_settings_file_():
        return
    
    def _aggregate_weekly_(self):
        week_ago = datetime.datetime.now() - datetime.timedelta(days=7)
        weekly_dumps = []
        os.chdir(self.store_dir)
        contents = os.listdir(self.store_dir)
        files = []
        for item in contents:
            if os.path.isfile(item):
                fname, ext = item.split('.')
                if ext == 'pik':
                    weekly_dumps.append(item)
        region_list = self._update_region_list()
        catalog = {key:[] for key in region_list.keys()}
        order_ids = set()
        order_id_list = {key:[] for key in region_list.keys()}
        previous_length = 0
        for file in weekly_dumps:
            f_timestamp = file[12:22]
            timestamp = datetime.datetime.strptime(f_timestamp, '%Y-%m-%d')
            if timestamp > week_ago:
                print('loading file:', file)
                with open(file, 'rb') as f:
                    data_dump = pickle.load(f)
                for key, region in data_dump.items():
                    region_catalog = catalog[key]
                    ti = time.time()
                    oids = [order['order_id'] for order in region]
                    for order in region:
                        oid = order['order_id']
                        if oid not in order_ids:
                            order_ids.add(oid)
                            order_id_list[key].append(oid)
                            order['volume_remain'] = [order['volume_remain']]
                            try:
                                order['timestamps'] = [order['timestamps']]
                            except KeyError:
                                order['timestamps'] = [timestamp]
                            order['price'] = [order['price']]
                            region_catalog.append(order)
                        else:
                            i = order_id_list[key].index(oid)
                            check = region_catalog[i]
                            cid = check['order_id']
                            if cid == oid:
                                check['volume_remain'].append(order['volume_remain'])
                                check['price'].append(order['price'])
                                try:
                                    check['timestamps'].append(order['timestamps'])
                                except KeyError:
                                    check['timestamps'].append(timestamp)
                                region_catalog[i] = check
#                               print(oid, len(order_ids), te)
                                continue
                    te = time.time() - ti
                    print('%s: %i %f'%(key, len(region), te))
#                        print(oid, len(order_ids), te)
                running_length = sum(len(region) for region in catalog)
                print('Current catalog length:', running_length)
                new_orders = running_length - previous_length
                previous_length = running_length
                print('Added',new_orders,'new orders.')
        return catalog

class Order:
    '''
    Object for handling and organization of order data. Attributes correspond
    to values available from Swagger through regional market order requests. 
    '''
    def __init__(self, duration, is_buy_order, issued, location_id, min_volume,
                 order_id, price, _range, system_id, timestamps, type_id,
                 volume_remain, volume_total):
        '''
        Initialize Order, assign attributes from individual order entries in 
        raw data dump.
        
        Input:
        --------
            duration : int
                Duration of order, in hours or days
            is_buy_order : bool
                True if buy order, False if sell order
            issued : datetime.datetime
                Timestamp for order creation
            location_id : int
                Unique location identifier for order
            min_volume : int
                Minimum volume allowed per transaction
            order_id : int
                Unique order identification number
            price : list of floats
                Order unit price, in the order in which they were captured in a market dump
            _range : str
                Distance from location within which transaction may occur
            system_id : int
                Unique identifier of system in which order was placed, redundant information also provided indirectly by location_id
            timestamps : list of datetime.datetime objects
                Timestamps for each market dump in which order was located
            type_id : int
                Unique identifier for type of item being sold/bought in order
            volume_remain : int
                Total number of units still available/desired in order
            volume_total : int
                Total number of units available/desired at order creation
        '''
        self.duration = duration
        self.is_buy_order = is_buy_order
        self.issued = issued
        self.location_id = location_id
        self.min_volume = min_volume
        self.order_id = order_id
        self.price = price
        self._range = _range
        self.system_id = system_id
        self.timestamps = timestamps
        self.type_id = type_id
        self.volume_remain = volume_remain
        self.volume_total = volume_total
        return
    
    def mean_price(self):
        '''
        
        Calculates the average of all prices identified for this order.
        '''
        prices = [price for price in self.price]
        average_price = np.mean(prices)
        return average_price
    
    def delta_price(self):
        '''
        Calculates total change in listed price since order creation.
        '''
        prices = [price for price in self.price]
        if len(prices) < 2:
            price_change = None
        else:
            price_change = prices[-1] - prices[0]
        return price_change
    
    def age(self):
        '''
        Calculates total age of order since issuance.
        '''
        now = datetime.datetime.now()
        order_age = now - self.issued
        return order_age
    
class Report:
    '''
    Object for organization and handling of Sleeper data for the purposes
    of compilation of aggregate data into a meaningful format. 
    '''
    
    def __init__(self):
        self.data = {}
        self.metrics = {}
        self.figures = {}
        return