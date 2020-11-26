# -*- coding: utf-8 -*-

import datetime
import time
import json
import os
import requests

import esipy as api
import bz2
import zlib
import numpy as np


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

def soft_append(container, addendum):
    '''
    Appends addendum item to container only if addendum is not already member
    of container.

    Input:
    --------
    container : list or set
        container object to be appended
    addendum : any
        value to be soft-appended to container
    '''
    if addendum not in container:
        if type(container) is list:
            container.append(addendum)
        elif type(container) is set:
            container.add(addendum)
        return container
    return container

class Sleeper:

    def __init__(self):
        '''
        Initialize Sleeper class. Initializes instance of ESI Swagger interfacial
        app, updates relevant metadata, declares
        '''

        def _load_resources_():

            def update_region_list(payload):
                payload = payload['mapRegions.csv.bz2']
                payload = payload.split('\r\n')
                payload = [line.split(',') for line in payload]
                header = payload[0]
                body = payload[1:-1]
                self.regions = {}
                for line in body:
                    rid = int(line[0])
                    region_data = {}
                    for i, cat in enumerate(header):
                        try:
                            region_data[cat] = line[i]
                        except:
                            region_data[cat] = None
                    self.regions[rid] = region_data
                return

            # update_region_list(raw_resources)
            # update_typeID_list(raw_resources)
            return

        self.root_dir = os.getcwd()
        self.store_dir = os.path.join(self.root_dir, 'data_dumps')
        self.resource_dir = os.path.join(self.root_dir, 'bin')
        self.settings_fname = os.path.join(self.resource_dir, 'sleeper_settings.sl')
        self.static_data = Static(self.resource_dir)
        self.esi_app = None
        self.client = None
        self.app = None
        self._init_interface_()
        return

    def _init_interface_(self):
        print('Initiailizing API interface...')
        self.esi_app = api.EsiApp()
        self.app = self.esi_app.get_latest_swagger
        self.client = api.EsiClient(retry_requests=True,
                                    headers={'User-Agent':'Sleeper \\ <0.4.0>'},
                                    raw_body_only=False)
        return

    def _populate_resources_(self):
        #DEPRECATED!!!

        import bz2

        def grab_SDE_files(filenames):
            print('Updating EVE static data')
            target_urls = {fname:url_base+fname for fname in self.resource_files}
            responses = {}
            for fname, url in target_urls.items():
                while True:
                    response = requests.get(url)
                    if response.status_code == 200:
                        new_fname = os.path.join(self.resource_dir,fname)[:-4]
                        payload = response.content
                        responses[fname] = payload
                        break
            return responses

        def write_file(fname, payload):
            fname = os.path.join(self.resource_dir, fname)
            with open(fname, 'wb') as f:
                f.write(payload)
            return

        url_base = r"http://www.fuzzwork.co.uk/dump/latest/"

        responses = grab_SDE_files(self.resource_files)
        for fname, payload in responses.items():
            write_file(fname, payload)

        return

    def _request_region_market_orders(self, region_id=10000002, type_id=34, order_type='all'):

        while True:
            op = self.app.op['get_markets_region_id_orders'](
                region_id = region_id,
                page=1,
                order_type=order_type)
            res = self.client.head(op)
            if res.status == 200:
                n_pages = res.header['X-Pages'][0]
                operations = []
                for page in range(1, n_pages+1):
                    operations.append(
                            self.app.op['get_markets_region_id_orders'](
                                    region_id=region_id,
                                    page=page,
                                    order_type=order_type)
                            )
                while True:
                    response = self.client.multi_request(operations)
                    pull_time = datetime.datetime.now()
                    r_codes = [packet[1].status for packet in response]
                    success = [code == 200 for code in r_codes]
                    if all(success):
                        break
                orders = Catalog._rawdump2catalog_(response, pull_time)
                break
        return orders

    # def _update_market_metadata_(self):
    #     topdown={}
    #     bottomup={}
    #     ignore_categories = [0,1,3,10,11,14,24,26,29,350001,53,54,59,49]
    #
    #     operation = self.app.op['get_universe_categories']()
    #     response = self.client.request(operation)
    #     cat_ids = [d for d in response.data]
    #     for cid in ignore_categories:
    #         cat_ids.remove(cid)
    #     cat_names = []
    #     grp_ids = []
    #     grp_names = []
    #     type_ids = []
    #     type_names = []
    #     for cid in cat_ids:
    #         operation = self.app.op['get_universe_categories_category_id'](
    #                 category_id=cid)
    #         while True:
    #             response = self.client.request(operation)
    #             if response.status == 200:
    #                 break
    #         cat_names.append(response.data['name'])
    #         categorical_groups = response.data['groups']
    #         categorized = {}
    #         for gid in categorical_groups:
    #             grp_ids.append(gid)
    #             operation = self.app.op['get_universe_groups_group_id'](
    #                 group_id=gid)
    #             while True:
    #                 response = self.client.request(operation)
    #                 if response.status == 200:
    #                     break
    #             grp_names.append(response.data['name'])
    #             grouped_types = response.data['types']
    #             grouped = []
    #             for tid in grouped_types:
    #                 type_ids.append(tid)
    #                 operation = self.app.op['get_universe_types_type_id'](
    #                         type_id=tid)
    #                 while True:
    #                     response = self.client.request(operation)
    #                     if response.status == 200:
    #                         break
    #                 type_names.append(response.data['name'])
    #                 grouped.append(tid)
    #                 bottomup[tid]={'category_id':cid, 'group_id':gid}
    #             categorized[gid] = grouped
    #         topdown[cid] = categorized
    #         print('Pulled category: %s'%cat_names[-1])
    #
    #     category_ids_fname = os.path.join(self.resource_dir, 'category_ids.json')
    #     group_ids_fname = os.path.join(self.resource_dir, 'group_ids.json')
    #     type_ids_fname = os.path.join(self.resource_dir, 'type_ids.json')
    #     topdown_fname = os.path.join(self.resource_dir, 'heirarchy_topdown.json')
    #     bottomup_fname = os.path.join(self.resource_dir, 'heirarchy_bottomup.json')
    #
    #     print('Writing metadata...')
    #
    #     with open(category_ids_fname, 'w') as f:
    #         json.dump((cat_ids, cat_names), f)
    #     with open(group_ids_fname, 'w') as f:
    #         json.dump((grp_ids, grp_names), f)
    #     with open(type_ids_fname, 'w') as f:
    #         json.dump((type_ids, type_names), f)
    #     with open(topdown_fname, 'w') as f:
    #         json.dump(topdown, f)
    #     with open(bottomup_fname, 'w') as f:
    #         json.dump(bottomup, f)
    #
    #     return
    #
    # def _import_market_metadata_(self):
    #
    #     category_ids_fname = os.path.join(self.resource_dir, 'category_ids.json')
    #     group_ids_fname = os.path.join(self.resource_dir, 'group_ids.json')
    #     type_ids_fname = os.path.join(self.resource_dir, 'type_ids.json')
    #     topdown_fname = os.path.join(self.resource_dir, 'heirarchy_topdown.json')
    #     bottomup_fname = os.path.join(self.resource_dir, 'heirarchy_bottomup.json')
    #
    #     with open(category_ids_fname, 'r') as f:
    #         cat_ids, cat_names = tuple(json.load(f))
    #     with open(group_ids_fname, 'r') as f:
    #         grp_ids, grp_names = tuple(json.load(f))
    #     with open(type_ids_fname, 'r') as f:
    #         type_ids, type_names = tuple(json.load(f))
    #     with open(topdown_fname, 'r') as f:
    #         self.topdown = json.load(f)
    #     with open(bottomup_fname, 'r') as f:
    #         self.bottomup = json.load(f)
    #
    #     self.categories = {}
    #     self.categories_reverse = {}
    #     for idx, cid in enumerate(cat_ids):
    #         self.categories[cid] = cat_names[idx]
    #         self.categories_reverse[cat_names[idx]] = cid
    #     self.groups = {}
    #     self.groups_reverse = {}
    #     for idx, gid in enumerate(grp_ids):
    #         self.groups[gid] = grp_names[idx]
    #         self.groups_reverse[grp_names[idx]] = gid
    #     self.types = {}
    #     self.types_reverse = {}
    #     for idx, tid in enumerate(type_ids):
    #         self.types[tid] = type_names[idx]
    #         self.types_reverse[type_names[idx]] = tid
    #
    #     return

    def _make_request_(self, op, maxtries=5):
        try_count = 0
        while try_count < maxtries:
            response = self.client.request(op)
            try_count += 1
            if resonse.status == 200:
                return response
        return None

    @staticmethod
    def _rawentry2order_(rawentry, timestamp):
        '''
        Converts raw returned data entry from SwaggerAPI to Order object
        '''
        strptime_template = '%Y-%m-%dT%H:%M:%S+00:00'
        duration = rawentry['duration']
        is_buy_order = rawentry['is_buy_order']
        issued = datetime.datetime.strptime(str(rawentry['issued']), strptime_template)
        location_id = rawentry['location_id']
        min_volume = rawentry['min_volume']
        order_id = rawentry['order_id']
        price = rawentry['price']
        _range = rawentry['range']
        system_id = rawentry['system_id']
        timestamps = [timestamp]
        type_id = rawentry['type_id']
        volume_remain = int(rawentry['volume_remain'])
        volume_total = rawentry['volume_total']
        order = Order(duration, is_buy_order, issued, location_id, min_volume,
                      order_id, price, _range, system_id, timestamps, type_id,
                      volume_remain, volume_total)
        return order

    def get_regional_history(self, region_id=10000002, type_id=34):

        def _request_region_market_history(region_id=10000002, type_id=34):
            while True:
                op = self.app.op['get_markets_region_id_history'](
                    region_id=region_id,
                    type_id=type_id)
                res = self.client.request(op)
                if res.status == 200:
                    break
            return res

        try:
            for tid in type_id:
                pass
        except:
            type_id = [type_id]

        results = {}
        for tid in type_id:
            results[tid] = _request_region_market_history(region_id, tid)
        return results

    def _scrape_by_type_id(self, type_id, region_id=None, order_type='all', verbose=False):
        '''
        Scrapes regions or full market for all orders of type "order_type" matching
        a "type_id"
        '''
        if region_id is not None:
            try:
                region_id = [r for r in region_id]
            except TypeError:
                region_id = [region_id]
        else:
            region_id = [k for k in self.regions.keys()]

        master_catalog = Catalog()
        for rid, region in self.regions.items():
            name = region['regionName']
            region_catalog = self._request_region_market_orders(rid, type_id, order_type)
            master_catalog += region_catalog
            if verbose: print(name,': #Orders :',len(region_catalog),': TotalOrders :', len(master_catalog))
        return master_catalog

#    def get_lowest_sell(self, type_id, region_id=None, n_results=10):
#
#        def find_n_lowest(arr, n):
#            arr = list(arr)
#            indices = []
#            values = []
#            i = 0
#            while i < n:
#                try:
#                    minimum = min(arr)
#                except ValueError:
#                    break
#                mindex = arr.index(minimum)
#                indices.append(mindex)
#                values.append(arr.pop(mindex))
#                i+=1
#            return values, indices
#
#
#        return master_catalog



    @staticmethod
    def dict2order(dictionary):
        strptime_template = '%Y-%m-%d %H:%M:%S.%f'
        duration = dictionary['duration']
        is_buy_order = dictionary['is_buy_order']
        issued = datetime.datetime.strptime(dictionary['issued'], strptime_template)
        location_id = dictionary['location_id']
        min_volume = dictionary['min_volume']
        order_id = dictionary['order_id']
        price = dictionary['price']
        _range = dictionary['range']
        system_id = dictionary['system_id']
        timestamps = [datetime.datetime.strptime(ts, strptime_template) for ts in dictionary['timestamps']]
        type_id = dictionary['type_id']
        volume_remain = dictionary['volume_remain']
        volume_total = dictionary['volume_total']
        order = Order(duration, is_buy_order, issued, location_id, min_volume,
                      order_id, price, _range, system_id, timestamps, type_id,
                      volume_remain, volume_total)
        return order

    def list_categories(self):
        print([i for i in zip(self.categories.keys(), self.categories.values())])
        return

    def list_groups(self):
        print([i for i in zip(self.groups.keys(), self.groups.values())])
        return

    def list_types(self):
        print([i for i in zip(self.types.keys(), self.types.values())])
        return

    def market_dump(self, save=False):
        '''
        Scrapes full current market data, saves to .sld archive file. File is
        formatted as a tuple containing a decomposed catalog and a dump timestamp
        '''

        if self.client is None:
            self._init_interface_()

        master_catalog = Catalog()
        print('Scraping market data')
        print('Region:')
        region_ids = self.static_data.IDs['region']
        region_names = self.static_data.Names['region']
        if len(region_ids) != len(region_names):
            raise ValueError('List of region names and IDs have different lengths')
        for i, name in enumerate(region_names):
            rid = region_ids[i]
            print(name)
            region_dump = self._request_region_market_orders(rid, order_type='all')
            master_catalog += region_dump

        if save:
            refresh_time = time.time()
            fname = 'market_dump-'+str(refresh_time)+'.cat'
            fname = os.path.join(self.store_dir, fname)
            master_catalog.save(fname)

        print('DONE')

        return master_catalog

    @staticmethod
    def _save_dumpfile_(payload, filename):
        with open(filename, 'wb') as f:
            p_string = json.dumps(payload)
            p_bytes = zlib.compress(p_string.encode())
            f.write(p_bytes)
        return

    @staticmethod
    def _load_dumpfile_(fname):
        with open(fname, 'rb') as f:
            p_bytes = f.read()
            p_string = zlib.decompress(p_bytes).decode()
            decomposed_catalog, dump_timestamp = json.loads(p_string)
        raw_catalog = Catalog._recompose_(decomposed_catalog)
        return raw_catalog, dump_timestamp

    def strip_duplicates(catalog):
        stripped_catalog = []
        order_ids = set()
        for order in catalog:
            if order.order_id not in order_ids:
                stripped_catalog.append(order)
        return stripped_catalog

    def tid2name(self, tid):
        """Convert integer TypeID to string"""
        with open(os.path.join(self.resource_dir, 'type_ids.json'),'r') as f:
            ids, names = json.load(f)
        idx = ids.index(tid)
        return names[idx]

    def gid2name(self, gid):
        """Look up item group name by GroupID"""
        with open(os.path.join(self.resource_dir, 'group_ids.json'),'r') as f:
            ids, names = json.load(f)
        idx = ids.index(gid)
        return names[idx]

    def cid2name(self, cid):
        """Look up item category name by CategoryID"""
        with open(os.path.join(self.resource_dir, 'category_ids.json'),'r') as f:
            ids, names = json.load(f)
        idx = ids.index(cid)
        return names[idx]

    def get_route(self, origin, destination, avoid=None, security='shortest'):

        if type(origin) is str:
            pass #Convert string to integer system id
        if type(destination) is str:
            pass #Convert string to integer system id

        op = self.app.op['get_route'](
            origin=origin, destination=destination, flag=security, avoid=avoid
        )
        response = self._make_request_(op)


class Order(dict):
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
            timestamps : (list of) datetime.datetime object(s)
                Timestamps for each market dump in which order was located
            type_id : int
                Unique identifier for type of item being sold/bought in order
            volume_remain : int
                Total number of units still available/desired in order
            volume_total : int
                Total number of units available/desired at order creation
        '''
        self['duration'] = duration
        self['is_buy_order'] = is_buy_order
        self['issued'] = issued
        self['location_id'] = location_id
        self['system_id'] = system_id
        self['min_volume'] = min_volume
        self.id = order_id
        self['order_id'] = order_id
        if type(price) is list:
            self['price'] = price
        else:
            self['price'] = [price]
        self['range'] = _range
#        self['system_id'] = system_id
        if type(timestamps) is list:
            self['timestamps'] = timestamps
        else:
            self['timestamps'] = [timestamps]
        self['type_id'] = type_id
        if type(volume_remain) is list:
            self['volume_remain'] = volume_remain
        else:
            self['volume_remain'] = [volume_remain]
        self['volume_total'] = volume_total
        return

    @staticmethod
    def __add__(self, b):
        return Order._append_order_(self, b)

    @staticmethod
    def _append_order_(old_order, new_order):
        '''
        Adds the contents of old_order to new_order. Non-container attributes values
        are pulled from old_order. Container attributes are appended using list.__add__().
        This means that data in returned order will not be sorted in any way, only treated
        as a back-to-front chaining of containers.
        '''
        if old_order.id == new_order.id:

            new_order = Order(
                    old_order['duration'],
                    old_order['is_buy_order'],
                    old_order['issued'],
                    old_order['location_id'],
                    old_order['min_volume'],
                    old_order['order_id'],
                    old_order['price'] + new_order['price'],
                    old_order['range'],
                    old_order['system_id'],
                    old_order['timestamps'] + new_order['timestamps'],
                    old_order['type_id'],
                    old_order['volume_remain'] + new_order['volume_remain'],
                    old_order['volume_total']
                    )

            return new_order
        else:
            raise ValueError('Order IDs do not match!')
            return None

    def _decompose_(self):
        '''
        Decompose Order into dictionary which contains all data. Intended as
        intermediate step for saving order to file individually or as part of
        catalog dump.
        '''

        strftime_template = '%Y-%m-%d %H:%M:%S.%f'
        decomposed_order = {
                'duration' : self['duration'],
                'is_buy_order' : self['is_buy_order'],
                'issued' : datetime.datetime.strftime(self['issued'], strftime_template),
                'location_id' : self['location_id'],
                'min_volume' : self['min_volume'],
                'order_id' : self['order_id'],
                'price' : self['price'],
                'range' : self['range'],
                'system_id' : self['system_id'],
                'timestamps' : [datetime.datetime.strftime(timestamp, strftime_template) for timestamp in self['timestamps']],
                'type_id' : self['type_id'],
                'volume_remain' : self['volume_remain'],
                'volume_total' : self['volume_total']
                }

        return decomposed_order

    def mean_price(self):
        '''

        Calculates the average of all prices identified for this order.
        '''
        prices = [price for price in self['price']]
        average_price = np.mean(prices)
        return average_price

    def delta_price(self):
        '''
        Calculates total change in listed price since order creation.
        '''
        prices = [price for price in self['price']]
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
        order_age = now - self['issued']
        return order_age

class Catalog(dict):

    def __init__(self):
        self.filters = {}
        self.orders = self.values
        return

    def __add__(self, b):
        return self._merge_catalogs_(self, b)

    def _decompose_(self):
        '''
        Primitivize catalog to list of dictionaries, for saving as .slc file.
        Sequentially steps through catalog, decomposing each order to a JSON-
        compatible dictionary object using the order's _decompose_() method.
        '''
        return [order._decompose_() for order in self.orders()]

    @staticmethod
    def _recompose_(decomposed_catalog):
        rebuilt = Catalog()
        for order in decomposed_catalog:
            new_order = Sleeper.dict2order(order)
            rebuilt[new_order.id] = new_order
        rebuilt.strip_duplicates()
        return rebuilt

    def save(self, fname):
        payload = self._decompose_()
        payload = json.dumps(payload).encode('utf-8')
        payload = bz2.compress(payload)
        with open(fname, 'wb') as f:
            f.write(payload)
        return

    @staticmethod
    def load(fname):
        with open(fname, 'rb') as f:
            payload = f.read()
        payload = bz2.decompress(payload).decode('utf-8')
        payload = json.loads(payload)
        return Catalog._recompose_(payload)

    def filter(self, criteria, value):
        '''
        Input:
        --------
            catalog : list of Sleeper.Order objects

            criteria : str

            value : str
        '''
        matching_orders = []
        criteria = criteria.upper()
        if criteria=='DURATION':
            critcheck = [order['duration'] for order in self]
            value = int(value)
        elif criteria=='IS_BUY_ORDER':
            critcheck = [order.is_buy_order for order in self]
            value = bool(value)
        elif criteria=='ISSUED':
            critcheck = [order.issued for order in self]
            value = datetime.datetime.strptime(value, '%Y-%m-%d')
        elif criteria=='LOCATION_ID':
            critcheck = [order.location_id for order in self]
            value = int(value)
        elif criteria=='MIN_VOLUME':
            critcheck = [order.min_volume for order in self]
            value = int(value)
        elif criteria=='ORDER_ID':
            critcheck = [order.order_id for order in self]
            value = int(value)
        elif criteria=='PRICE':
            critcheck = [order.price[-1] for order in self]
            value = float(value)
        elif criteria=='RANGE':
            critcheck = [order._range for order in self]
        elif criteria=='SYSTEM_ID':
            critcheck = [order.system_id for order in self]
            value = int(value)
        elif criteria=='TIMESTAMPS':
            critcheck = [order.timestamps[-1] for order in self]
            value = datetime.datetime.strptime(value, '%Y-%m-%d')
        elif criteria=='TYPE_ID':
            critcheck = [order.type_id for order in self]
            value = int(value)
        elif criteria=='VOLUME_REMAIN':
            critcheck = [order.volume_remain for order in self]
            value = int(value)
        elif criteria=='VOLUME_TOTAL':
            critcheck = [order.volume_total for order in self]
            value = int(value)
        else:
            raise KeyError('Criteria not recognized')

        match_indices = []
        for idx, val in enumerate(critcheck):
            if val == value:
                matching_orders.append(self[idx])

        return filtered_catalog

    # def _agregate_range_(self, low, high=None):
    #
    #     # If no high is given, set high equal to low. Equivalence is handled later
    #     strptime_template = '%Y-%m-%d %H:%M:%S'
    #
    #     if high == '' or high is None:
    #         high = low
    #
    #     if type(low)==str:
    #         low = datetime.datetime.strptime(low, strptime_template)
    #     if type(high)==str:
    #         high = datetime.datetime.strptime(high, strptime_template)
    #     dir_dump = os.listdir(self.store_dir)
    #
    #     files = []
    #     for item in dir_dump:
    #         item = os.path.join(self.store_dir, item)
    #         if os.path.isfile(item):
    #             if item.split('.')[1] == 'sld':
    #                 files.append(item)
    #     file_timestamps = [datetime.datetime.strptime(os.path.split(fname)[1][12:31],'%Y-%m-%d %H-%M-%S') for fname in files]
    #
    #     range_ts = []
    #
    #     #If high == low, load only file with timestamp == low
    #     if high == low:
    #         range_ts.append(file_timestamps.index(low))
    #     # Else load all files with timestamps between from low to high, inclusive
    #     else:
    #         for idx, ts in enumerate(file_timestamps):
    #             if ts >= low and ts <= high:
    #                 range_ts.append(idx)
    #
    #     range_files = [files[i] for i in range_ts]
    #
    #     ti = time.time()
    #
    #     master_catalog = Catalog()
    #     loaded_timestamps = []
    #
    #     for fname in range_files:
    #         raw_cat, ts = self._load_dumpfile_(fname)
    #         master_catalog = Catalog._merge_catalogs_(master_catalog, raw_cat)
    #         loaded_timestamps.append(ts)
    #
    #     dt = time.time() - ti
    #     print('loaded %i files, %i orders in %f seconds'%(len(range_files), np.sum([len(catalog) for catalog in master_catalog.values()]), dt))
    #
    #     return master_catalog

    def strip_duplicates(self):
        '''
        Removes duplicate orders
        '''
        stripped_catalog = Catalog()
        order_ids = set()
        for order_id, order in self.items():
            if order_id not in order_ids:
                stripped_catalog[order_id] = order
        return stripped_catalog

    @staticmethod
    def _rawdump2catalog_(rawdump, timestamp):
        catalog = Catalog()
        for packet in rawdump:
            packet_data = packet[1].data
            for entry in packet_data:
                new_order = Sleeper._rawentry2order_(entry, timestamp)
                if new_order.id not in catalog:
                    catalog[new_order.id] = new_order
                else:
                    old_order = catalog[new_order.id]
                    catalog[new_order.id] = Order._append_order_(old_order, new_order)
        return catalog

    @staticmethod
    def _merge_catalogs_(catalog_1, catalog_2):
        '''
        Method called by Catalog._add__()
        '''
        merged_catalog = catalog_1
        for order_id, new_order in catalog_2.items():
            if order_id in merged_catalog:
                old_order = merged_catalog[order_id]
                merged_catalog[order_id] = Order._append_order_(old_order, new_order)
            else:
                merged_catalog[order_id] = new_order
        return merged_catalog

class Static:
    '''
    Reference container for accessing and parsing EVE static data, ex. for
    converting from numerical Type IDs to item names and vice versa
    '''

    def __init__(self, resource_dir):

        def parse_resource_file(fname):
            fname = os.path.join(self.resource_dir, fname)
            with bz2.open(fname) as f:
                lines = [line.decode().rstrip().split(',') for line in f.readlines()]
            while '' in lines:
                lines.remove('')
            return lines

        def populate_types():
            lines = parse_resource_file('invTypes.csv.bz2')
            typeIDs = []
            typeNames = []
            for line in lines[1]:
                try:
                    typeIDs.append(int(line[0]))
                    typeNames.append(line[2])
                except ValueError: pass
                except IndexError: pass
            self.IDs['type'] = typeIDs
            self.Names['type'] = typeNames
            return

        def populate_regions():
            lines = parse_resource_file('mapRegions.csv.bz2')
            regionIDs = []
            regionNames = []
            for line in lines[1:]:
                regionIDs.append(int(line[0]))
                regionNames.append(line[1])
            self.IDs['region'] = regionIDs
            self.Names['region'] = regionNames
            return

        def populate_groups():
            lines = parse_resource_file('invGroups.csv.bz2')
            groupIDs = []
            groupNames = []
            groupCategoryIDs = []
            for line in lines[1:]:
                groupIDs.append(int(line[0]))
                groupNames.append(line[2])
            self.IDs['group'] = groupIDs
            self.Names['group'] = groupNames
            return
        #Initialize empty container for numerical IDs
        self.IDs={'type': None, 'group': None, 'category': None,
                  'location': None, 'region': None, 'system': None}

        #Initialize empty container for plaintext names (as duplicate of empty ID dict)
        self.Names = dict(self.IDs)

        self.resource_dir = resource_dir
        populate_types()
        populate_regions()
