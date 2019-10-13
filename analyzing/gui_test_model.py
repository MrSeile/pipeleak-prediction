"""
This script creates an interactive application in which you can set all model variables througth sliders
"""

import sys
sys.path.append("..")

from utils import *

from functools import partial
from tensorflow.python.keras.api._v2.keras.models import load_model
from random import randint
import psycopg2
from typing import List

from PyQt5 import QtGui, QtCore
from PyQt5.QtCore import Qt, QEvent
from PyQt5.QtWidgets import QApplication, QLabel, QSlider, QHBoxLayout, QWidget, QGridLayout, QPushButton, QDialog


# Contains the data of one input to the neural network
class Param:
	def __init__(self, var, min_val, max_val, func=None):
		self.var = var
		self.min = min_val
		self.max = max_val
		self.func = func


# Contains all inputs variables
class Data:
	# @variables is a list of Param objects
	def __init__(self, variables: List[Param]):
		
		# Stores the variables as a dictionary
		self.data = {}
		
		# Stores how to transform (if at all) the raw data from the slider
		self.funcs = {}
		
		# Initialize the data
		for i in variables:
			self.funcs[i.var] = i.func
			self.set_var(i.var, i.min)

	# Returns a list with all the variables values
	def as_list(self):
		return list(map(lambda x: [[x]], [x for x in self.data.values()]))
	
	# Set variable @var to value @value and transform it if necessary
	def set_var(self, var, value):
		if self.funcs[var] is None:
			self.data[var] = value
		else:
			self.data[var] = self.funcs[var](value)


# Connect to postgres
conn = psycopg2.connect(database='giswater3', user='postgres', password='postgres', host='localhost')
cursor = conn.cursor()

# Window class
# Stucture: QWidget formed by two other widgets side by side. The left one has a grid layout to store all the buttons
# and the rigth one only has a label with the network prediction
class Window(QDialog):
	
	def __init__(self, parent=None):
		super().__init__(parent)
		
		# Load the model with its custom metrics (else it would give an error)
		self.model = load_model('C:/Users/User/Desktop/break_prediction/models/32.h5', custom_objects={'diff95': diff95})
		
		pnoms = [6, 10, 16, 25]
		materials = ['PE', 'FIB', 'FO', 'PVC', 'Fe', 'other']
		
		# Store all the sliders you want
		self.params = [
			Param('year',		2008, 	2018),
			Param('month', 		1, 		12),
			Param('expl_id',	0,  	23),
			Param('age', 		0,  	100),
			Param('material',	0,  	5,		lambda x: materials[x]),
			Param('pnom', 		0,  	3, 		lambda x: pnoms[x]),
			Param('dnom', 		20, 	900),
			Param('slope', 		0,  	40),
			Param('n_connec',	0,  	30),
			Param('consum',		0,  	100000),
			Param('length',		0,  	2000)
		]
		
		# Create the data acording to the parametern previously defined
		self.data = Data(self.params)
		
		# Create all the sliders into @input_widget
		input_widget = QWidget()
		input_layout = QGridLayout()
		for i, par in enumerate(self.params):
			
			# Label which displays the name of the variable which is modified by the slider
			name = QLabel(f'{par.var}:')
			
			# Create a slider
			slider = QSlider(Qt.Horizontal)
			slider.setObjectName(f's_{par.var}')
			slider.setRange(par.min, par.max)
			slider.setValue(par.min)
			slider.valueChanged.connect(partial(self.set_data, par.var))
			
			# Create a label which will display the current value of the slider
			value = QLabel(f'{self.data.data[par.var]}')
			value.setObjectName(f'v_{par.var}')
			
			# Add them into the widget
			input_layout.addWidget(name, i, 0)
			input_layout.addWidget(slider, i, 1)
			input_layout.addWidget(value, i, 2)
		
		# Add a push button to randomize the data
		button = QPushButton()
		button.setObjectName('rand_button')
		button.setText('Randomize')
		button.clicked.connect(self.randomize_data)
		input_layout.addWidget(button, len(self.params), 1)
		
		input_widget.setLayout(input_layout)
		
		# Labal which will display the prediction of the network
		result = QLabel()
		result.setObjectName('result')
		
		# Set all the widgets
		layout = QHBoxLayout()
		layout.addWidget(input_widget)
		layout.addWidget(result)
		
		self.setLayout(layout)
		
		# Claculate the output for the fist time
		self.update()
	
	# Set every slider to a random value
	def randomize_data(self):
		
		for par in self.params:
			
			slider = self.findChild(QSlider, f's_{par.var}')
			slider.setValue(randint(par.min, par.max))

	# Change @data variable value and updates the label
	def set_data(self, var, value):
		
		self.data.set_var(var, value)
		label = self.findChild(QLabel, f'v_{var}')
		label.setText(str(self.data.data[var]))
		
		self.update()
	
	# Recalculate neural network output, optional: update variable value
	def update(self):
		
		# The station code for each exploitation_id (ex: expl_id = 0 -> station_code = CL)
		stations = ['CL', 'U4', 'U4', 'WW', 'U4', 'U4', 'CL', 'VU', 'CL', 'WW', 'WW', 'U4', 'VP', 'VV', 'CL', 'CL', 'U4', 'VP', 'WE', 'CI', 'CI', 'V3', 'CG', 'V5']
		
		# Get the temperature table from the database
		cursor.execute(f"select period, val from api_ws_sample.ext_timeseries")
		temperature = cursor.fetchall()
		
		# Get all variables as a list
		data = self.data.as_list()
		
		# Extract the year and the month to calculate the index of the temperature series
		year = data[0][0][0]
		month = data[1][0][0]
		
		data.pop(1)
		data.pop(0)
		
		index = (year - 2007) * 12 + month
		
		# Get the station code for the current exploitation_id
		s = stations[self.data.data['expl_id']]
		
		# Extract six months of data
		tmax = [t[1][index - 6: index] for t in temperature if t[0]['id'] == s + 'max']
		tmin = [t[1][index - 6: index] for t in temperature if t[0]['id'] == s + 'min']
		
		# Append the temperature to the static data (pipe propieties)
		data.append(tmax)
		data.append(tmin)
		
		# Calculate the output of the network
		output = self.model.predict(norm_input_arr(data))
		
		# Set the result label text
		t_result = self.findChild(QLabel, 'result')
		t_result.setText(str(output[0][0]))


app = QApplication(sys.argv)

window = Window()
window.show()

sys.exit(app.exec_())

while True: pass
