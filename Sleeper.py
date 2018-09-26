# -*- coding: utf-8 -*-

import esipy as api

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
            
        return orders
    
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
    
MarketDump = Sleeper(app, client)

MarketDump._update_region_list()

MarketDump.market_dump()