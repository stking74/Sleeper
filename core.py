# -*- coding: utf-8 -*-

import esipy as api
import datetime
import time
import numpy as np
import pickle
import json
import os
import zlib

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

class Sleeper():

    def __init__(self):
        '''
        Initialize Sleeper class. Initializes instance of ESI Swagger interfacial
        app, updates relevant metadata, declares
        '''
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
        self.resource_dir = os.path.join(self.root_dir, 'bin')
        try:
            resource_files = os.listdir(self.resource_dir)
        except:
            os.mkdir(self.resource_dir)
            resource_files = os.listdir(self.resource_dir)
        resource_filenames = [
            'category_ids.json',
            'group_ids.json',
            'heirarchy_topdown.json',
            'heirarchy_bottomup.json',
            'type_ids.json'
        ]
        for fname in resource_filenames:
            if fname not in resource_files:
                print('Performing first-time metadata update. This will take a few minutes.')
                self._update_market_metadata_()
                break
        self._import_market_metadata_()
        self.settings_fname = os.path.join(self.resource_dir, 'sleeper_settings.sl')
        self._update_region_list()
        return

    @staticmethod
    def _save_catalog_(filename, catalog):
        with open(filename, 'w') as f:
            json.dump(catalog._decompose_(), f)
        return

    def _update_market_metadata_(self):
        topdown={}
        bottomup={}
        ignore_categories = [0,1,3,10,11,14,24,26,29,350001,53,54,59,49]

        operation = self.app.op['get_universe_categories']()
        response = self.client.request(operation)
        cat_ids = [d for d in response.data]
        for cid in ignore_categories:
            cat_ids.remove(cid)
        cat_names = []
        grp_ids = []
        grp_names = []
        type_ids = []
        type_names = []
        for cid in cat_ids:
            operation = self.app.op['get_universe_categories_category_id'](
                    category_id=cid)
            while True:
                response = self.client.request(operation)
                if response.status == 200:
                    break
            cat_names.append(response.data['name'])
            categorical_groups = response.data['groups']
            categorized = {}
            for gid in categorical_groups:
                grp_ids.append(gid)
                operation = self.app.op['get_universe_groups_group_id'](
                    group_id=gid)
                while True:
                    response = self.client.request(operation)
                    if response.status == 200:
                        break
                grp_names.append(response.data['name'])
                grouped_types = response.data['types']
                grouped = []
                for tid in grouped_types:
                    type_ids.append(tid)
                    operation = self.app.op['get_universe_types_type_id'](
                            type_id=tid)
                    while True:
                        response = self.client.request(operation)
                        if response.status == 200:
                            break
                    type_names.append(response.data['name'])
                    grouped.append(tid)
                    bottomup[tid]={'category_id':cid, 'group_id':gid}
                categorized[gid] = grouped
            topdown[cid] = categorized
            print('Pulled category: %s'%cat_names[-1])

        category_ids_fname = os.path.join(self.resource_dir, 'category_ids.json')
        group_ids_fname = os.path.join(self.resource_dir, 'group_ids.json')
        type_ids_fname = os.path.join(self.resource_dir, 'type_ids.json')
        topdown_fname = os.path.join(self.resource_dir, 'heirarchy_topdown.json')
        bottomup_fname = os.path.join(self.resource_dir, 'heirarchy_bottomup.json')

        print('Writing metadata...')

        with open(category_ids_fname, 'w') as f:
            json.dump((cat_ids, cat_names), f)
        with open(group_ids_fname, 'w') as f:
            json.dump((grp_ids, grp_names), f)
        with open(type_ids_fname, 'w') as f:
            json.dump((type_ids, type_names), f)
        with open(topdown_fname, 'w') as f:
            json.dump(topdown, f)
        with open(bottomup_fname, 'w') as f:
            json.dump(bottomup, f)

        return

    def _update_region_list(self):

        print('Refreshing region metadata...')
        operation = self.app.op['get_universe_regions']()
        response = self.client.request(operation)
        region_ids = response.data
        response_header = response.header
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

    def _import_market_metadata_(self):

        category_ids_fname = os.path.join(self.resource_dir, 'category_ids.json')
        group_ids_fname = os.path.join(self.resource_dir, 'group_ids.json')
        type_ids_fname = os.path.join(self.resource_dir, 'type_ids.json')
        topdown_fname = os.path.join(self.resource_dir, 'heirarchy_topdown.json')
        bottomup_fname = os.path.join(self.resource_dir, 'heirarchy_bottomup.json')

        with open(category_ids_fname, 'r') as f:
            cat_ids, cat_names = tuple(json.load(f))
        with open(group_ids_fname, 'r') as f:
            grp_ids, grp_names = tuple(json.load(f))
        with open(type_ids_fname, 'r') as f:
            type_ids, type_names = tuple(json.load(f))
        with open(topdown_fname, 'r') as f:
            self.topdown = json.load(f)
        with open(bottomup_fname, 'r') as f:
            self.bottomup = json.load(f)

        self.categories = {}
        self.categories_reverse = {}
        for idx, cid in enumerate(cat_ids):
            self.categories[cid] = cat_names[idx]
            self.categories_reverse[cat_names[idx]] = cid
        self.groups = {}
        self.groups_reverse = {}
        for idx, gid in enumerate(grp_ids):
            self.groups[gid] = grp_names[idx]
            self.groups_reverse[grp_names[idx]] = gid
        self.types = {}
        self.types_reverse = {}
        for idx, tid in enumerate(type_ids):
            self.types[tid] = type_names[idx]
            self.types_reverse[type_names[idx]] = tid

        return

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

        def _request_region_market_orders(region_id=10000002, type_id=34, order_type='all'):

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

        master_catalog = Catalog()
        print('Scraping market data')
        print('Region:')
        for region in self.region_list.items():

            name, region_data = region
            print(name)
            region_id = region_data['region_id']
            region_dump = _request_region_market_orders(region_id, order_type='all')
            master_catalog += region_dump

        if save:
            decomposed_catalog = master_catalog._decompose_()
            refresh_time = datetime.datetime.now()
            pik_filename = 'market_dump-'+str(refresh_time)[:-7]+'.sld'
            pik_filename = [c for c in pik_filename]
            pik_filename[25] = '-'
            pik_filename[28] = '-'
            pik_filename = ''.join(pik_filename)
            pik_filename = os.path.join(self.store_dir, pik_filename)
            dump_contents = (decomposed_catalog, str(refresh_time))
            Sleeper._save_dumpfile_(dump_contents, pik_filename)

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
        raw_catalog = Sleeper.recompose(decomposed_catalog)
        return raw_catalog, dump_timestamp

    @staticmethod
    def recompose(decomposed_catalog):
        rebuilt = Catalog()
        for order in decomposed_catalog:
            new_order = Sleeper.dict2order(order)
            rebuilt[new_order.id] = new_order
        return rebuilt

    def _agregate_range_(self, low, high=None):

        # If no high is given, set high equal to low. Equivalence is handled later
        strptime_template = '%Y-%m-%d %H:%M:%S'

        if high == '' or high is None:
            high = low

        if type(low)==str:
            low = datetime.datetime.strptime(low, strptime_template)
        if type(high)==str:
            high = datetime.datetime.strptime(high, strptime_template)
        dir_dump = os.listdir(self.store_dir)

        files = []
        for item in dir_dump:
            item = os.path.join(self.store_dir, item)
            if os.path.isfile(item):
                if item.split('.')[1] == 'sld':
                    files.append(item)
        file_timestamps = [datetime.datetime.strptime(os.path.split(fname)[1][12:31],'%Y-%m-%d %H-%M-%S') for fname in files]

        range_ts = []

        #If high == low, load only file with timestamp == low
        if high == low:
            range_ts.append(file_timestamps.index(low))
        # Else load all files with timestamps between from low to high, inclusive
        else:
            for idx, ts in enumerate(file_timestamps):
                if ts >= low and ts <= high:
                    range_ts.append(idx)

        range_files = [files[i] for i in range_ts]

        ti = time.time()

        master_catalog = Catalog()
        loaded_timestamps = []

        for fname in range_files:
            raw_cat, ts = self._load_dumpfile_(fname)
            master_catalog = Catalog._merge_catalogs_(master_catalog, raw_cat)
            loaded_timestamps.append(ts)

        dt = time.time() - ti
        print('loaded %i files, %i orders in %f seconds'%(len(range_files), np.sum([len(catalog) for catalog in master_catalog.values()]), dt))

        return master_catalog

    def filter_catalog(self, catalog, criteria, value):
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
            critcheck = [order['duration'] for order in catalog]
            value = int(value)
        elif criteria=='IS_BUY_ORDER':
            critcheck = [order.is_buy_order for order in catalog]
            value = bool(value)
        elif criteria=='ISSUED':
            critcheck = [order.issued for order in catalog]
            value = datetime.datetime.strptime(value, '%Y-%m-%d')
        elif criteria=='LOCATION_ID':
            critcheck = [order.location_id for order in catalog]
            value = int(value)
        elif criteria=='MIN_VOLUME':
            critcheck = [order.min_volume for order in catalog]
            value = int(value)
        elif criteria=='ORDER_ID':
            critcheck = [order.order_id for order in catalog]
            value = int(value)
        elif criteria=='PRICE':
            critcheck = [order.price[-1] for order in catalog]
            value = float(value)
        elif criteria=='RANGE':
            critcheck = [order._range for order in catalog]
        elif criteria=='SYSTEM_ID':
            critcheck = [order.system_id for order in catalog]
            value = int(value)
        elif criteria=='TIMESTAMPS':
            critcheck = [order.timestamps[-1] for order in catalog]
            value = datetime.datetime.strptime(value, '%Y-%m-%d')
        elif criteria=='TYPE_ID':
            critcheck = [order.type_id for order in catalog]
            value = int(value)
        elif criteria=='VOLUME_REMAIN':
            critcheck = [order.volume_remain for order in catalog]
            value = int(value)
        elif criteria=='VOLUME_TOTAL':
            critcheck = [order.volume_total for order in catalog]
            value = int(value)
        else:
            raise KeyError('Criteria not recognized')

        match_indices = []
        for idx, val in enumerate(critcheck):
            if val == value:
                matching_orders.append(catalog[idx])

        return filtered_catalog

    def strip_duplicates(catalog):
        stripped_catalog = []
        order_ids = set()
        for order in catalog:
            if order.order_id not in order_ids:
                stripped_catalog.append(order)
        return stripped_catalog

    def tid2name(self, tid):
        with open(os.path.join(self.resource_dir, 'type_ids.json'),'r') as f:
            ids, names = json.load(f)
        idx = ids.index(tid)
        return names[idx]

    def gid2name(self, gid):
        with open(os.path.join(self.resource_dir, 'group_ids.json'),'r') as f:
            ids, names = json.load(f)
        idx = ids.index(gid)
        return names[idx]

    def cid2name(self, cid):
        with open(os.path.join(self.resource_dir, 'category_ids.json'),'r') as f:
            ids, names = json.load(f)
        idx = ids.index(cid)
        return names[idx]


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

class Reporter:
    '''
    Object for organization and handling of Sleeper data for the purposes
    of compilation of aggregate data into a meaningful format.
    '''

    def __init__(self, data_dir, report_dir):
        self.data_dir = data_dir
        self.report_dir = report_dir
        return

    def list2sheet(self, list_data, worksheet):
        '''
        Converts a list of lists into a formatted openpyxl worksheet, with each nested
        list representing a separate row of the final worksheet
        '''
        for row, data in enumerate(list_data):
            row += 1
            for column, value in enumerate(data):
                column += 1
                worksheet.cell(row=row, column=column, value=value)
        return worksheet

    @staticmethod
    def generate_report(catalog, *args, **kwds):

        return
