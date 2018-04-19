import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import pandas.io.sql as psql


bin_width = 1
debug = True

print('Attempting to connect to database')
conn = psycopg2.connect("dbname='fuser' user='fuser' password='fuser' host='taint.arc.nasa.gov' ")

runway_vector = ['18L' , '18C' , '18R' , '23' , '36L' , '36C' , '36R' , '5']

q = '''SELECT 
*
FROM
scheduler_analysis
order by msg_time DESC
limit 1
'''  
df = psql.read_sql(q, conn)

print(df)

start_time = df.loc[0,'msg_time']

bins = [start_time, pd.Timestamp(start_time) + pd.Timedelta(str(bin_width) + ' minutes')]

cols = ['start_bin','end_bin',]
for rwy in range(len(runway_vector)):
	cols.append('departures_' + runway_vector[rwy])
	cols.append('arrivals_' + runway_vector[rwy])
	cols.append('departures_residual_' + runway_vector[rwy])
	cols.append('arrivals_residual_' + runway_vector[rwy])
dfRealized = pd.DataFrame(np.empty((1,len(cols)), dtype=object),columns=cols)

cols = ['start_bin','end_bin',]
for rwy in range(len(runway_vector)):
	cols.append('departures_scheduled_' + runway_vector[rwy])
	cols.append('arrivals_scheduled_' + runway_vector[rwy])

dfScheduled = pd.DataFrame(np.empty((1,len(cols)), dtype=object),columns=cols)


getSchedules = True

while True:
	if getSchedules:
		q = '''SELECT 
		*
		FROM
		scheduler_analysis
		WHERE
		msg_time = '%s'
		'''%bins[-2]  
		df = psql.read_sql(q, conn)
		df.to_csv('scheduler_analysis_data.csv')

		for rwy in range(len(runway_vector)):
			df_sched_dept = df[ (df['general_stream'] == 'DEPARTURE')\
			& ( df['runway'] == runway_vector[rwy] )\
			& ( df['runway_sta'] > bins[-2] )\
			& ( df['runway_sta'] <= bins[-1] ) ]

			df_sched_arv = df[ (df['general_stream'] == 'ARRIVAL')\
			& ( df['runway'] == runway_vector[rwy] )\
			& ( df['runway_sta'] > bins[-2] )\
			& ( df['runway_sta'] <= bins[-1] ) ]

			dfScheduled.loc[len(bins)-2,'start_bin'] = bins[-2]
			dfScheduled.loc[len(bins)-2,'end_bin'] = bins[-1]
			dfScheduled.loc[len(bins)-2,'departures_scheduled_' + runway_vector[rwy]] = len(df_sched_dept['flight_key'])
			dfScheduled.loc[len(bins)-2,'arrivals_scheduled_' + runway_vector[rwy]] = len(df_sched_arv['flight_key'])
			getSchedules = False

	

	q = '''SELECT 
	msg_time
	FROM
	scheduler_analysis
	order by msg_time DESC
	limit 1
	'''  
	df_msg_time = psql.read_sql(q, conn)
	current_time = df_msg_time.loc[0,'msg_time']
	if  ( pd.Timedelta(pd.Timestamp(current_time) - pd.Timestamp(bins[-1])).total_seconds() / float(60) ) < 0:
		

		t0 = pd.Timestamp(start_time) - pd.Timedelta('30 minutes')
		q = '''SELECT 
		gufi,
		departure_runway_actual_time,
		departure_runway_position_derived,
		departure_aerodrome_iata_name,
		arrival_runway_actual_time,
		COALESCE( arrival_runway_assigned , arrival_runway_position_derived) as arrival_runway,
		arrival_aerodrome_iata_name
		FROM
		matm_flight_summary mfs
		WHERE
		mfs.timestamp > '%s'
		'''%(t0)
		dfMATM = psql.read_sql(q, conn)

		for bin_number in range(len(bins)-1):
			for rwy in range(len(runway_vector)):
				df_dept = dfMATM[ (dfMATM['departure_aerodrome_iata_name'] == 'CLT')\
				&( dfMATM['departure_runway_actual_time'] > bins[bin_number] )\
				&( dfMATM['departure_runway_actual_time'] <= bins[bin_number+1])\
				&( dfMATM['departure_runway_position_derived'] == runway_vector[rwy]  )]
			

				df_arv = dfMATM[ (dfMATM['arrival_aerodrome_iata_name'] == 'CLT')\
				&( dfMATM['arrival_runway_actual_time'] > bins[bin_number] )\
				&( dfMATM['arrival_runway_actual_time'] <= bins[bin_number+1])\
				&( dfMATM['arrival_runway'] == runway_vector[rwy]  )]

			
				dfRealized.loc[bin_number,'start_bin'] = bins[bin_number]
				dfRealized.loc[bin_number,'end_bin'] = bins[bin_number+1]
				dfRealized.loc[bin_number,'departures_' + runway_vector[rwy]] = len(df_dept['gufi'])
				dfRealized.loc[bin_number,'arrivals_' + runway_vector[rwy]] = len(df_arv['gufi'])
				dfRealized.loc[bin_number,'departures_residual_' + runway_vector[rwy]] = dfScheduled.loc[bin_number,'departures_scheduled_' + runway_vector[rwy]] - len(df_dept['gufi'])
				dfRealized.loc[bin_number,'arrivals_residual_' + runway_vector[rwy]] = dfScheduled.loc[bin_number,'arrivals_scheduled_' + runway_vector[rwy]] - len(df_arv['gufi'])

		print(current_time)
		print('SCHEDULED')
		print(dfScheduled)
		print('REALIZED')
		print(dfRealized)

		for rwy in range(len(runway_vector)):
			num = dfScheduled[ 'departures_scheduled_' + runway_vector[rwy]  ].sum() + dfScheduled[ 'arrivals_scheduled_' + runway_vector[rwy]  ].sum() \
			+ dfRealized[ 'departures_' + runway_vector[rwy]  ].sum() + dfRealized[ 'arrivals_' + runway_vector[rwy]  ].sum()
			if num > 0:
				plt.figure(rwy,figsize=(12,10))		
				plt.subplot(2,1,1)
				plt.cla()
				ax = plt.gca()
				print('PLOTTING ON')
				print(runway_vector[rwy])
				print(dfScheduled['departures_scheduled_' + runway_vector[rwy]])
				print(dfScheduled['arrivals_scheduled_' + runway_vector[rwy]])
				dfScheduled.plot(x='start_bin',y = 'departures_scheduled_' + runway_vector[rwy] , marker = 'o',ms=7, alpha = 0.6, color = 'green',ax=ax)
				dfScheduled.plot(x='start_bin',y = 'arrivals_scheduled_' + runway_vector[rwy] , marker = 'o',ms=7, alpha = 0.6, color = 'blue',ax=ax)
				dfRealized.plot(x='start_bin',y = 'departures_' + runway_vector[rwy] , style= '--', marker = 's',ms=10, alpha = 0.6, color = 'green',ax=ax)
				dfRealized.plot(x='start_bin',y = 'arrivals_' + runway_vector[rwy] , style= '--', marker = 's',ms=10, alpha = 0.6, color = 'blue',ax=ax)

				plt.subplot(2,1,2)
				plt.cla()
				ax = plt.gca()
				
				dfRealized.plot.bar(x='start_bin',y = 'departures_residual_' + runway_vector[rwy] ,alpha = 0.6, color = 'green',ax=ax)
				dfRealized.plot.bar(x='start_bin',y = 'arrivals_residual_' + runway_vector[rwy] , alpha = 0.6, color = 'blue',ax=ax)
				plt.xticks(rotation=0)
				plt.ylim([-bin_width,bin_width])

				plt.tight_layout()

		plt.show(block=False)
		plt.pause(0.1)
		if debug:
			dfScheduled.to_csv('scheduled.csv')
			dfRealized.to_csv('realized.csv')

	else:
		bins.append( pd.Timestamp(bins[-1]) + pd.Timedelta(str(bin_width) + ' minutes') )
		getSchedules = True
	



		

