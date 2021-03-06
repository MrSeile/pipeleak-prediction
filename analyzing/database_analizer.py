"""
This script analizes the database and
"""

import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns

# Connect to postgres dataset
try:
	conn = psycopg2.connect(database='giswater3', user='postgres', password='postgres', host='localhost')
except:
	raise

cursor = conn.cursor()


# Plots all the data from @table and plots its distribution
def plot_dist(var, table, condition='true'):
	
	# Get the data
	sql = f'SELECT {var} FROM {table} WHERE {condition}'
	cursor.execute(sql)
	data = cursor.fetchall()
	
	# Get it as an array of values
	data = list(list(zip(*data))[0])
	
	# Turn it into float
	data = list(map(float, data))
	
	print(f"max: {max(data)}")
	print(f"min: {min(data)}")
	
	# Plot the data and return de axis
	return sns.distplot(data, kde=False)


# condition = "mean is not null"
condition = "consum < 30000 and consum > 50"

table = "api_ws_sample.v_ai_pipeleak_main_leak a join ai_ndvi b on (a.id = b.arc_id)"
plot_dist('consum', table, condition)

# table = "api_ws_sample.v_ai_pipeleak_main_noleak a join ai_ndvi b on (a.id = b.arc_id)"
# plot_dist('mean', table, condition)

plt.tight_layout()
plt.show()