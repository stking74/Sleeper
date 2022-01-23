

import Sleeper
import numpy as np
import matplotlib.pyplot as plt
from sklearn.decomposition import PCA
#from umap import UMAP

types = {'Tritanium':34,
         'Pyerite':35,
         'Mexallon':36,
         'Isogen':37,
         'Nocxium':38,
         'Zydrine':39,
         'Megacyte':40,
         'Morphite':18}

app = Sleeper.core.Sleeper()
history = app.get_regional_history(type_id=[t for t in types.values()])
averages = []
dates = []
for k, tid in types.items():
    averages.append(np.array([d['average'] for d in history[tid].data]))
    dates.append([str(d['date']) for d in history[tid].data])
    print(k, averages[-1][-1])

plt.figure()
for i, tid in enumerate(types):
    d = dates[i]
    a = averages[i]
    a = a / a.max()
    plt.plot(d, a)
plt.legend([])
plt.show()

types = {'Veldspar':1230,
         'Tritanium':34}

history = app.get_regional_history(type_id=[t for t in types.values()])
averages = []
dates = []
for k, tid in types.items():
    averages.append(np.array([d['average'] for d in history[tid].data]))
    dates.append([str(d['date']) for d in history[tid].data])
    print(k, averages[-1][-1])

plt.figure()
for i, tid in enumerate(types):
    d = dates[i]
    a = averages[i]
    a = a / a.max()
    plt.plot(d, a)
plt.legend([])
plt.show()



#averages = np.array(averages).T
#print(averages.shape)
#
#model = UMAP()
#reduced = model.fit_transform(averages)

#plt.figure()
#plt.scatter(reduced[:,0], reduced[:,1])
#plt.show()

#model = PCA(n_components = 3)
#reduced = model.fit_transform(averages)
#plt.figure()
#for comp in model.components_:
#    plt.bar(range(8), comp)
#plt.show()
