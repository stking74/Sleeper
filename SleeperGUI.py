# -*- coding: utf-8 -*-
"""
Created on Sat Jan 12 13:52:35 2019

@author: tyler
"""

import os
import tkinter
import datetime
import matplotlib.pyplot as plt
from .core import Sleeper, Catalog, Order

class GUI:

    def __init__(self):

        def open_query_gui():
            query_gui = QueryGUI(self.master, self.sleeper)
            return

        def open_catalog_browser():
            catalog_browser = CatalogBrowser(self.master, self.sleeper)
            return

        def static_refresh():
            self.sleeper._populate_resources_()
            return

        #Headers and text
        self.master = tkinter.Tk()
        self.readout_txt = tkinter.StringVar()
        self.readout_txt.set('Initializing GUI...')
        self.readout = tkinter.Label(self.master, textvariable=self.readout_txt)
        self.readout.grid(row=20, column=0)
        #Sleeper app startup
        self.readout_txt.set('Initializing Sleeper app...')
        self.sleeper = Sleeper()
        self.readout_txt.set('Sleeper app initialized')
        #Buttons
        self.open_dir_button = tkinter.Button(self.master, text='Browse data', command=open_catalog_browser)
        self.scrape_button = tkinter.Button(self.master, text='Query data', command=open_query_gui)
        static_refresh_button = tkinter.Button(self.master, text='Refresh static resources', command=static_refresh)
        #Layout
        self.open_dir_button.grid(row=1, column=0)
        self.scrape_button.grid(row=2, column=0)
        static_refresh_button.grid(row=3,column=0)
        self.master.mainloop()

        return

class CatalogBrowser:

    def __init__(self, master, app):
        self.window = tkinter.Toplevel(master)
        self.sleeper = app
        self.active_catalog = None
        dir_entry = tkinter.StringVar()
        #Slave window widgets
        dir_entry_field = tkinter.Entry(self.window, textvariable=dir_entry)
        dir_open_button = tkinter.Button(self.window, text='Open', command=lambda:open_catalog(dir_entry.get()))
        #Slave window layout
        dir_entry_field.grid(row=0, column=0)
        dir_open_button.grid(row=0,column=1)

    def open_catalog(self,fname):
        self.active_catalog = Catalog.load(fname)
        return

class QueryGUI:

    def __init__(self, master, app):
        self.window = tkinter.Toplevel(master)
        self.sleeper = app
        self.readout_txt = tkinter.StringVar()
        self.readout_txt.set('Initializing Query Assistant...')
        self.readout = tkinter.Label(self.window, textvariable=self.readout_txt)
        self.readout.grid(row=20, column=0)
        #Buttons
        open_dir_button = tkinter.Button(self.window, text='Full Market Scrape', command=self.full_scrape)
        save_scrape_button = tkinter.Button(self.window, text='Save Scrape', command=lambda:self.save_active_catalog)
        # filtered_scrape_button = tkinter.Button(self.window, text='Filtered scrape', command=)
        #Filters
        #By typeID
        self.type_id_filters = tkinter.StringVar()
        typeId_filters_field = tkinter.Entry(self.window, textvariable=self.type_id_filters)
        self.region_id_filters = tkinter.StringVar()
        regionId_filters_field = tkinter.Entry(self.window, textvariable=self.region_id_filters)
        #Misc fields
        self.save_fname = tkinter.StringVar()
        save_fname_field = tkinter.Entry(self.window, textvariable=self.save_fname)
        #Layout
        open_dir_button.grid(row=1, column=0)
        typeId_filters_field.grid(row=2, column=0)
        regionId_filters_field.grid(row=3, column=0)
        save_fname_field.grid(row=4, column=1)
        save_scrape_button.grid(row=5, column=1)
        # self.master.mainloop()
        return

    def full_scrape(self):
        self.readout_txt.set('Performing full market scrape...')
        self.readout = tkinter.Label(self.window, textvariable=self.readout_txt)
        self.active_catalog = self.sleeper.market_dump()
        self.readout_txt.set('Market scrape finished!')
        self.readout = tkinter.Label(self.window, textvariable=self.readout_txt)
        return

    def save_active_catalog(self):
        fname = self.save_fname.get()
        fname = os.path.join(self.sleeper.store_dir, fname)
        self.active_catalog.save(fname)
        return
