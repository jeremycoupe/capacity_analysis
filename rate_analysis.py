import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import pandas.io.sql as psql


print('Attempting to connect to database')
conn = psycopg2.connect("dbname='fuserclt' user='fuserclt' password='fuserclt' host='localhost' ")

def plot_rate(bank_date, bank_start_time):
	date_st = bank_date + bank_start_time
	print(date_st)
	binsize = 5
	minutes = 150
	timestamp_0 = pd.Timestamp(date_st)
	timestamp_1 = timestamp_0 + pd.Timedelta( str(minutes) + ' minutes')

	line_width_var = 2

	q = '''SELECT 
	*
	FROM
	scheduler_analysis
	where msg_time > '%s'
	and msg_time < '%s'
	order by msg_time ASC
	''' %(timestamp_0,timestamp_1)  
	df_sched = psql.read_sql(q, conn)
	print('Got Scheduler Data')

	if len(df_sched) > 0:

		q = '''SELECT 
		gufi, departure_runway_actual_time, departure_runway_position_derived
		FROM
		matm_flight_summary
		where departure_runway_actual_time is not null 
		and departure_runway_position_derived is not null
		and departure_runway_actual_time > '%s'
		and departure_runway_actual_time < '%s'
		''' %(timestamp_0,timestamp_1)  
		df_matm_dept = psql.read_sql(q, conn)
		print('Got Departure Data')


		q = '''SELECT 
		gufi, arrival_runway_actual_time,
		COALESCE( arrival_runway_assigned , arrival_runway_position_derived) as arrival_runway
		FROM
		matm_flight_summary
		where
		arrival_runway_actual_time is not null
		and COALESCE( arrival_runway_assigned , arrival_runway_position_derived) is not null
		and arrival_runway_actual_time > '%s'
		and arrival_runway_actual_time < '%s'
		''' %(timestamp_0,timestamp_1)  
		df_matm_arv = psql.read_sql(q, conn)
		print('Got Arrival Data')


		all_runways = df_matm_dept['departure_runway_position_derived'].unique()
		print(all_runways)
		all_runways = np.append(all_runways,df_matm_arv['arrival_runway'].unique())
		print(all_runways)

		msg_time_vec = df_sched['msg_time'].unique()
		bin_edges = []
		x_tick_vec = []
		for ts in range(len(msg_time_vec)):
			if ts % 30 == 0:
				bin_edges.append(str(msg_time_vec[ts]).split('.')[0])
				x_tick_vec.append(str(msg_time_vec[ts]).split('.')[0].replace('T' , ' '))
				print(str(msg_time_vec[ts]).split('.')[0])


		all_runways = np.unique(all_runways)

		for rwy in range(len(all_runways)):
			plt.figure(figsize = (12,10))

			plt_scheduled_dept = np.zeros(len(bin_edges))
			plt_realized_dept = np.zeros(len(bin_edges))
			plt_residual_dept = np.zeros(len(bin_edges))
			plt_scheduled_arv = np.zeros(len(bin_edges))
			plt_realized_arv = np.zeros(len(bin_edges))
			plt_residual_arv = np.zeros(len(bin_edges))
			for bin in range(len(bin_edges)):
				###### Calculate departure data
				plt_scheduled_dept[bin] = len( df_sched[ (df_sched['msg_time'] ==  bin_edges[bin]) \
				& (df_sched['general_stream'] == 'DEPARTURE') \
				& (df_sched['runway'] == all_runways[rwy]) \
				& (df_sched['runway_sta'] > pd.Timestamp(bin_edges[bin])) \
				& (df_sched['runway_sta'] < pd.Timestamp(bin_edges[bin]) + pd.Timedelta( str(binsize) + ' minutes')  )     ])

				plt_realized_dept[bin] = len(df_matm_dept[ (df_matm_dept['departure_runway_position_derived'] == all_runways[rwy]) \
				&(df_matm_dept['departure_runway_actual_time'] > pd.Timestamp(bin_edges[bin])) \
				&(df_matm_dept['departure_runway_actual_time'] < pd.Timestamp(bin_edges[bin]) + pd.Timedelta( str(binsize) + ' minutes')  )     ])

				plt_residual_dept[bin] = plt_scheduled_dept[bin] - plt_realized_dept[bin]

				###### Calculate arrival data
				plt_scheduled_arv[bin] = len( df_sched[ (df_sched['msg_time'] ==  bin_edges[bin]) \
				& (df_sched['general_stream'] == 'ARRIVAL') \
				& (df_sched['runway'] == all_runways[rwy]) \
				& (df_sched['runway_sta'] > pd.Timestamp(bin_edges[bin])) \
				& (df_sched['runway_sta'] < pd.Timestamp(bin_edges[bin]) + pd.Timedelta( str(binsize) + ' minutes')  )     ])

				plt_realized_arv[bin] = len(df_matm_arv[ (df_matm_arv['arrival_runway'] == all_runways[rwy]) \
				&(df_matm_arv['arrival_runway_actual_time'] > pd.Timestamp(bin_edges[bin])) \
				&(df_matm_arv['arrival_runway_actual_time'] < pd.Timestamp(bin_edges[bin]) + pd.Timedelta( str(binsize) + ' minutes')  )     ])

				plt_residual_arv[bin] = plt_scheduled_arv[bin] - plt_realized_arv[bin]

			#### plot departures
			plt.plot(plt_scheduled_dept,'--',label='scheduled departures',linewidth=line_width_var ,marker='o',color='blue',alpha=0.8)
			plt.plot(plt_realized_dept,'-',label='realized departures',linewidth=line_width_var ,marker='o',color='blue',alpha=0.8)
			plt.bar(np.arange(len(plt_residual_dept)),plt_residual_dept,label='scheduled - realized departures',color='blue',alpha=0.4)

			##### plot arrivals
			plt.plot(plt_scheduled_arv,'--',label='scheduled arrivals',linewidth=line_width_var ,marker='s',color='grey',alpha=0.8)
			plt.plot(plt_realized_arv,'-',label='realized arrivals',linewidth=line_width_var ,marker='s',color='grey',alpha=0.8)
			plt.bar(np.arange(len(plt_residual_arv)),plt_residual_arv,label='scheduled - realized arrivals',color='grey',alpha=0.4)


			plt.title('Runway ' + all_runways[rwy] + ' Runway Rate Analysis')
			plt.xticks(np.arange(len(plt_residual_dept)),x_tick_vec,fontsize=6,rotation=90)
			plt.legend(fontsize=12,loc='lower right')
			plt.ylim([-(binsize+1),(binsize+1)])
			plt.tight_layout()
			tmp_st = date_st.replace(' ','_')
			plt.savefig('figs/' + tmp_st.replace(':','.') + '_' + all_runways[rwy] + '_rate_analysis.png')
			plt.close('all')


date_vec = []
time_vec = []

for i in range(2,32):
	if i < 10:
		num = '0' + str(i)
	else:
		num = str(i)
	date_vec.append('2018-01-' + num )
	time_vec.append(' 14:00:00')

for i in range(1,29):
	if i < 10:
		num = '0' + str(i)
	else:
		num = str(i)
	date_vec.append('2018-02-' + num )
	time_vec.append(' 14:00:00')

for i in range(1,12):
	if i < 10:
		num = '0' + str(i)
	else:
		num = str(i)
	date_vec.append('2018-03-' + num )
	time_vec.append(' 14:00:00')



for i in range(12,32):
	if i < 10:
		num = '0' + str(i)
	else:
		num = str(i)
	date_vec.append('2018-03-' + num )
	time_vec.append(' 13:00:00')


for i in range(1,19):
	if i < 10:
		num = '0' + str(i)
	else:
		num = str(i)
	date_vec.append('2018-04-' + num )
	time_vec.append(' 13:00:00')

for d in range(len(date_vec)):
	plot_rate(date_vec[d],time_vec[d])


