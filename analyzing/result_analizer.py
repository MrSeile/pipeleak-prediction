import psycopg2
import matplotlib.pyplot as plt
import seaborn as sns
import json

# Connect to the database
conn = psycopg2.connect(database='giswater3', user='postgres', password='postgres', host='localhost')
cursor = conn.cursor()

cursor.execute(f"SELECT log_message FROM api_ws_sample.audit_log_data where fprocesscat_id=48")

data = cursor.fetchall()
data = [json.loads(d[0])['value'] for d in data]

sns.distplot(data, kde=False)
plt.ylabel("número de canonades", size=15)
plt.xlabel("predicció de la xarxa neuronal", size=15)
plt.show()