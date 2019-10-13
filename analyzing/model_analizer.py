"""
This file will plot how each input modifies the network prediction
"""

import sys
sys.path.append("..")

import warnings
warnings.simplefilter('ignore')

from utils import norm_input_arr, diff95
import matplotlib.pyplot as plt
import numpy as np
import tensorflow as tf

from tensorflow.python.keras.api._v2.keras.models import load_model, Model
import psycopg2

# Connect to the database
conn = psycopg2.connect(database='giswater3', user='postgres', password='postgres', host='localhost')
cursor = conn.cursor()

# Get the temperature data from the database
cursor.execute(f"select period, val from api_ws_sample.ext_timeseries")
temperature = cursor.fetchall()

# Load the model with its custom metrics (else it would give an error)
model = tf.keras.models.load_model('C:/Users/elies/Desktop/break_prediction/models/ndvi_7.h5', custom_objects={'diff95': diff95})
# model.summary()

# List to iterate on every type of material
mats = ['Fe', 'FO', 'FIB', 'PE', 'PVC', 'other']

# Set the range of each variable you want to analyze
plots = {
	'ndvi':		[np.arange(0, 1, 0.1), "index ndvi"],
	'expl_id': 	[np.arange(0, 23, 1), "id del municipi"],
	'age':  	[np.arange(0, 100, 0.1), "edat [anys]"],
	'material':	[mats, "material"],
	'dnom': 	[np.arange(20, 900, 0.1), "diàmetre [mm]"],
	'pnom': 	[np.array([6, 10, 16, 25]), "pressió nominal [kg]"],
	'slope': 	[np.arange(0, 15, 0.1), "pendent [%]"],
	'n_connec':	[np.arange(0, 30, 1), "número de connexions"],
	'consum':	[np.arange(0, 40000, 20), "consum [m3]"],
	'length': 	[np.arange(0.6, 1000, 1), "longitud [m]"]
}

# result = []
#
# for expl in np.arange(0, 23, 1):
#
#
# 	data = dict()
#
# 	data['expl_id'] = 	[0] * len(mats)
# 	data['age'] = 		[5] * len(mats)
# 	data['material'] = 	['FIB'] * len(mats)
# 	data['pnom'] = 		[10] * len(mats)
# 	data['dnom'] = 		[20] * len(mats)
# 	data['slope'] = 	[4] * len(mats)
# 	data['n_connec'] = 	[5] * len(mats)
# 	data['consum'] = 	[100] * len(mats)
# 	data['length'] =	[60] * len(mats)
# 	data['sum'] = 		[0.8] * len(mats)
#
# 	# Set the dynamic values
# 	data['expl_id'] = [expl] * len(mats)
# 	data['material'] = mats
#
# 	inputs = [list(map(lambda i: [i], x)) for x in data.values()]
# 	result.append(model.predict(norm_input_arr(inputs)))
#
# print(np.array(result))




# inputs = [list(map(list, x)) for x in data.values()]

# Set the output image to be 40000x10000 pixels
# plt.figure(figsize=(40, 10))

# For each dictionary key (@var) and value (@ran). Also store the item index
for n, (var, ran) in enumerate(plots.items()):

	rang = list(ran[0])
	
	print(n + 1)

	# Start a subplot in the window
	# plt.subplot(2, 4, n + 1)

	# For each material plot how the the input efects the output
	# for mat in mats:

	# Initialize the values
	data = dict()

	data['expl_id'] = 	[0] * len(rang)
	data['age'] = 		[30] * len(rang)
	data['material'] = 	['Fe'] * len(rang)
	data['pnom'] = 		[10] * len(rang)
	data['dnom'] = 		[200] * len(rang)
	data['slope'] = 	[4] * len(rang)
	data['n_connec'] = 	[2] * len(rang)
	data['consum'] = 	[1000] * len(rang)
	data['length'] =	[130] * len(rang)
	data['sum'] = 		[0.5] * len(rang)

	# Set the dynamic values
	data[var] = rang
	# data['material'] = [mat] * len(ran)

	# inputs = [list(map(list, x)) for x in data.values()]
	inputs = [list(map(lambda i: [i], x)) for x in data.values()]

	# # Get the temperature data (more info in gui_test_model.py)
	# year = 2009
	# month = 12
	# index = (year - 2007) * 12 + month
	#
	# stations = ['CL', 'U4', 'U4', 'WW', 'U4', 'U4', 'CL', 'VU', 'CL', 'WW', 'WW', 'U4', 'VP', 'VV', 'CL', 'CL', 'U4', 'VP', 'WE', 'CI', 'CI', 'V3', 'CG', 'V5']
	#
	# tmax = []
	# tmin = []
	#
	# for i in data['expl_id']:
	#
	# 	s = stations[i]
	# 	tmax.append([t[1][index - 6: index] for t in temperature if t[0]['id'] == s + 'max'][0])
	# 	tmin.append([t[1][index - 6: index] for t in temperature if t[0]['id'] == s + 'min'][0])
	#
	#
	# # Pack static inputs with the temperature
	# inputs += [tmax]
	# inputs += [tmin]

	# Predict the inputs
	y = model.predict(norm_input_arr(inputs))

	# y = [0.32835] * len(rang)

	print(y)
	
	
	
	# dat = sorted(zip(y, rang))
	
	# Plot the data
	# for yy, x in dat:
	# 	print(x, yy)
	# 	plt.bar(x, yy)
	plt.plot(rang, y)#, label=mat)

	# Set the tile of the subplot
	# plt.title(var)
	
	# plt.xticks(list(zip(*dat))[1])
	
	plt.xlabel(ran[1], size=15)
	plt.ylabel("probabilitat de trencament", size=15)
	
	# Plot the lengend and reduce its size
	# plt.legend(fontsize='x-small')

	plt.show()

# Save the image
plt.tight_layout()
plt.savefig('plots/plt.png')
