# -*- coding: utf-8 -*-

import esipy as api
import datetime
import time
import h5py
import numpy as np
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
        headers={'User-Agent':'Sleeper \\ <0.1.0>'},
        raw_body_only=False)

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
    
    def __init__(self, app, client):
        self.app = app
        self.client = client
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
        return
    
    def market_dump(self):
        orders = {}
        print('Scraping market data')
        print('Region:')
        for region in self.region_list.items():
            pull_time = datetime.datetime.now()
            name, region_data = region
            print(name)
            region_id = region_data['region_id']
            unpacked = []
            orders[name] = self._request_region_market_orders(region_id, order_type='all')
            for order in orders[name]:
                order['timestamps'] = pull_time
            time.sleep(0.5)
        refresh_time = datetime.datetime.now()
        filename_timestamp = refresh_time.strftime('%c')
        os.chdir(self.store_dir)
        pik_filename = 'market_dump-'+str(refresh_time)[:10]+'.pik'
        pik_file = open(pik_filename, 'wb')
        pickle.dump(orders, pik_file)
        pik_file.close()
        os.chdir(self.root_dir)
        
        return
    
    def _request_region_market_orders(self, region_id=10000002, type_id=34, order_type='all'):
        page = 1
        while True:
            operation = self.app.op['get_markets_region_id_orders'](region_id=region_id, order_type=order_type, page=page)
            response = client.request(operation)
            if page == 1:
                orders = [dict(entry) for entry in response.data]
                if len(orders)==0:
                    break
                page += 1
                continue
            else:
                new_orders = [dict(entry) for entry in response.data]
                if len(new_orders) == 0:
                    break
                for entry in new_orders:
                    orders.append(entry)
            page += 1
        return orders
    
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
        catalog = []
        order_ids = set()
        order_id_list = []
        for file in weekly_dumps:
            f_timestamp = file[12:22]
            timestamp = datetime.datetime.strptime(f_timestamp, '%Y-%m-%d')
            if timestamp > week_ago:
                print('processing file:', file)
                with open(file, 'rb') as f:
                    data_dump = pickle.load(f)
                for key, region in data_dump.items():
                    print('%s: %i'%(key, len(region)))
                    oids = [order['order_id'] for order in region]
                    new_oids = []
                    old_oids = []
                    for idx, oid in enumerate(oids):
                        if oid in order_ids:
                            old_oids.append(idx)
                        else:
                            new_oids.append(idx)
                    for order in region:
                        ti = time.time()
                        oid = order['order_id']
                        if oid not in order_ids:
                            order_ids.add(oid)
                            order_id_list.append(oid)
                            order['volume_remain'] = [order['volume_remain']]
                            try:
                                order['timestamps'] = [order['timestamps']]
                            except KeyError:
                                order['timestamps'] = [timestamp]
                            order['price'] = [order['price']]
                            catalog.append(order)
                        else:
                            i = order_id_list.index(oid)
                            check = catalog[i]
                            cid = check['order_id']
                            if cid == oid:
                                check['volume_remain'].append(order['volume_remain'])
                                check['price'].append(order['price'])
                                try:
                                    check['timestamps'].append(order['timestamps'])
                                except KeyError:
                                    check['timestamps'].append(timestamp)
                                catalog[i] = check
                                te = time.time() - ti
#                                print(oid, len(order_ids), te)
                                continue
                        te = time.time() - ti
#                        print(oid, len(order_ids), te)
                        
        return catalog
        
    
MarketDump = Sleeper(app, client)
#MarketDump._load_settings_file_()
#MarketDump._update_region_list()
#MarketDump.market_dump()
catalog = MarketDump._aggregate_weekly_()