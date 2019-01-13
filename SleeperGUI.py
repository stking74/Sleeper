# -*- coding: utf-8 -*-
"""
Created on Sat Jan 12 13:52:35 2019

@author: tyler
"""

import os
import tkinter
import datetime
import matplotlib.pyplot as plt
import pickle
from Sleeper import Sleeper

class SleeperGUI(tkinter.Toplevel):
    
    def __init__(self, master):
        
        
        #Headers and text
        self.master = master
        self.readout_txt = tkinter.StringVar()
        self.readout_txt.set('Initializing GUI...')
        self.readout = tkinter.Label(master, textvariable=self.readout_txt)
        self.readout.grid(row=20, column=0)
        #Sleeper app startup
        self.readout_txt.set('Initializing Sleeper app...')
        self.sleeper = Sleeper()
        self.sleeper._update_region_list()
        self.readout_txt.set('Sleeper app initialized')
        #Buttons
        self.open_dir_button = tkinter.Button(master, text='Open', command=self.open_dir_window)
        self.scrape_button = tkinter.Button(master, text='Scrape market', command=self.sleeper.market_dump)
        #Layout
        self.open_dir_button.grid(row=1, column=0)
        self.scrape_button.grid(row=2, column=0)
        
        
        return
    
    def open_dir_window(self):
        
        def open_directory():
            
            def pick_range(event):
                xpos = event.xdata
                ypos = event.ydata
                return
            #Check for dir exist
            target_dir = dir_entry.get()
            if os.path.exists(target_dir):
                #Walk target dir, build list of files
                dir_dump = os.listdir(target_dir)
                files = []
                for item in dir_dump:
                    item = os.path.join(target_dir, item)
                    if os.path.isfile(item):
                        if item.split('.')[1] == 'pik':
                            files.append(os.path.basename(item))
                file_timestamps = [datetime.datetime.strptime(fname[12:22],'%Y-%m-%d') for fname in files]
                earliest = file_timestamps[0]
                latest = file_timestamps[-1]
                for ts in file_timestamps:
                    if ts < earliest:
                        earliest = ts
                    if ts > latest:
                        latest = ts
                td = latest - earliest
                td = td.days
                str_timestamps = [str(ts.date()) for ts in file_timestamps]
                
                self.data_range_str_low = tkinter.StringVar(dir_select_window)
                self.data_range_str_high = tkinter.StringVar(dir_select_window)
                dir_range_select_lower = tkinter.OptionMenu(dir_select_window, self.data_range_str_low, *str_timestamps)
                dir_range_select_upper = tkinter.OptionMenu(dir_select_window, self.data_range_str_high, *str_timestamps)
                range_load_button = tkinter.Button(dir_select_window, text='Load data range', command=self.load_datarange)
                
                dir_range_select_lower.grid(row=1, column=0)
                dir_range_select_upper.grid(row=1, column=1)
                range_load_button.grid(row=2, column=1)
                
                #Init dir content window
                
                print('success')
                return
            else:
                print('Directory not found.')
            print(target_dir)
            return
        
        #Init slave window
        dir_select_window = tkinter.Toplevel(self.master)
        dir_entry = tkinter.StringVar()
        #Slave window widgets
        dir_entry_field = tkinter.Entry(dir_select_window, textvariable=dir_entry)
        dir_open_button = tkinter.Button(dir_select_window, text='Open', command=open_directory)
        #Slave window layout
        dir_entry_field.grid(row=0, column=0)
        dir_open_button.grid(row=0,column=1)
        
        return
    
    
    
    def load_datarange(self):
        self.active_catalog = self.sleeper._agregate_range_(self.data_range_str_low.get(), self.data_range_str_high.get())
        
        return
    
    
root = tkinter.Tk()
gui = SleeperGUI(root)
root.mainloop()