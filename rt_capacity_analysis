import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import pandas.io.sql as psql


print('Attempting to connect to database')
conn = psycopg2.connect("dbname='fuser' user='fuser' password='fuser' host='taint.arc.nasa.gov' ")

q = '''SELECT 
*
FROM
scheduler_analysis
order by msg_time DESC
limit 3000
'''  
df = psql.read_sql(q, conn)