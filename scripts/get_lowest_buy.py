# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 15:55:45 2020

@author: tyler
"""

import Sleeper
import numpy as np
import matplotlib.pyplot as plt

target_tid = 2806

app = Sleeper.core.Sleeper()
history = app.get_regional_history(type_id=target_tid)
hits = app._scrape_by_type_id(target_tid, order_type='sell', verbose=True)
prices = [o['price'][0] for o in hits.orders()]
hist_volume = [entry['volume'] for entry in history[target_tid].data]

plt.figure()
plt.boxplot(prices, showfliers=False)

plt.figure()
plt.plot(hist_volume)
