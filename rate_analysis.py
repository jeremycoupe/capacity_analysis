import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import psycopg2
import pandas.io.sql as psql


print('Attempting to connect to database')
# conn = psycopg2.connect("dbname='fuser' user='fuser' password='fuser' host='tack' ")
conn = psycopg2.connect("dbname='fuserclt' user='fuserclt' password='fuserclt' host='localhost' ")

binsize = 15
sample_min = 5
minutes = 120


def compute_separation(df_matm_dept,df_matm_arv,rwy_used):
	shiftArv = 35
	shiftDept = 10
	df1 = df_matm_dept[df_matm_dept['actual_runway'] == rwy_used]
	df2 = df_matm_arv[ df_matm_arv['actual_runway'] == rwy_used ]
	df_actual = df1.append(df2)
	df_actual = df_actual.sort_values(by = ['actual_runway' , 'actualtime'], ascending = [1,1])
	df_actual = df_actual.reset_index(drop=True)
	DD=[]
	DA=[]
	AD=[]
	AA=[]
	for flight in range(1,len(df_actual)):
		if df_actual.loc[flight,'origin'] == 'CLT':
			if df_actual.loc[flight-1,'origin'] == 'CLT':
				sep_val = (df_actual.loc[flight,'actualtime'] - shiftDept) - (df_actual.loc[flight-1,'actualtime'] - shiftDept)
				DD.append(sep_val)
			else:
				sep_val = (df_actual.loc[flight,'actualtime'] - shiftDept) - (df_actual.loc[flight-1,'actualtime'] - shiftArv)
				AD.append(sep_val)

		else:
			if df_actual.loc[flight-1,'origin'] == 'CLT':
				sep_val = (df_actual.loc[flight,'actualtime'] - shiftArv) - (df_actual.loc[flight-1,'actualtime'] - shiftDept)
				DA.append(sep_val)
			else:
				sep_val = (df_actual.loc[flight,'actualtime'] - shiftArv) - (df_actual.loc[flight-1,'actualtime'] - shiftArv)
				AA.append(sep_val)

	return [DD,DA,AD,AA]

def plot_rate(bank_date, bank_start_time,all_residual_dept,all_residual_arv,rwy_key,all_DD,all_DA,all_AD,all_AA):
	date_st = bank_date + bank_start_time
	print(date_st)
	timestamp_0 = pd.Timestamp(date_st)
	timestamp_1 = timestamp_0 + pd.Timedelta( str(minutes) + ' minutes')

	line_width_var = 2

	q = '''SELECT 
	*
	FROM
	scheduler_analysis
	where msg_time > '%s'
	and msg_time < '%s'
	--and gate not in ('GA1','GA2','SC1','AC1')
	order by msg_time ASC
	''' %(timestamp_0,timestamp_1)  
	df_sched = psql.read_sql(q, conn)
	print('Got Scheduler Data')

	x_tick_vec = []

	if len(df_sched) > 0:

		q = '''SELECT 
		gufi, departure_runway_actual_time, departure_runway_position_derived as actual_runway, departure_aerodrome_iata_name as origin,
		extract(epoch from departure_runway_actual_time) as actualtime
		FROM
		matm_flight_summary
		where departure_runway_actual_time is not null 
		and departure_runway_position_derived is not null
		and departure_runway_actual_time > '%s'
		and departure_runway_actual_time < '%s'
		--and departure_stand_decision_tree not in ('GA1','GA2','SC1','AC1')
		''' %(timestamp_0,timestamp_1)  
		df_matm_dept = psql.read_sql(q, conn)
		print('Got Departure Data')


		q = '''SELECT 
		gufi, arrival_runway_actual_time,
		COALESCE( arrival_runway_assigned , arrival_runway_position_derived) as actual_runway, departure_aerodrome_iata_name as origin,
		extract(epoch from arrival_runway_actual_time) as actualtime
		FROM
		matm_flight_summary
		where
		arrival_runway_actual_time is not null
		and COALESCE( arrival_runway_assigned , arrival_runway_position_derived) is not null
		and arrival_runway_actual_time > '%s'
		and arrival_runway_actual_time < '%s'
		--and arrival_stand_decision_tree not in ('GA1','GA2','SC1','AC1')
		''' %(timestamp_0,timestamp_1)  
		df_matm_arv = psql.read_sql(q, conn)
		print('Got Arrival Data')


		all_runways = df_matm_dept['actual_runway'].unique()
		print(all_runways)
		all_runways = np.append(all_runways,df_matm_arv['actual_runway'].unique())
		print(all_runways)

		msg_time_vec = df_sched['msg_time'].unique()
		bin_edges = []
		
		for ts in range(len(msg_time_vec)):
			if ts % (sample_min*6) == 0:
				bin_edges.append(str(msg_time_vec[ts]).split('.')[0])
				x_tick_vec.append(str(msg_time_vec[ts]).split('.')[0].replace('T' , ' '))
				print(str(msg_time_vec[ts]).split('.')[0])


		all_runways = np.unique(all_runways)

		if '23' not in all_runways:
			for rwy in range(len(all_runways)):

				[DD,DA,AD,AA] = compute_separation(df_matm_dept,df_matm_arv,all_runways[rwy])


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

					plt_realized_dept[bin] = len(df_matm_dept[ (df_matm_dept['actual_runway'] == all_runways[rwy]) \
					&(df_matm_dept['departure_runway_actual_time'] > pd.Timestamp(bin_edges[bin])) \
					&(df_matm_dept['departure_runway_actual_time'] < pd.Timestamp(bin_edges[bin]) + pd.Timedelta( str(binsize) + ' minutes')  )     ])

					plt_residual_dept[bin] = plt_scheduled_dept[bin] - plt_realized_dept[bin]

					###### Calculate arrival data
					plt_scheduled_arv[bin] = len( df_sched[ (df_sched['msg_time'] ==  bin_edges[bin]) \
					& (df_sched['general_stream'] == 'ARRIVAL') \
					& (df_sched['runway'] == all_runways[rwy]) \
					& (df_sched['runway_sta'] > pd.Timestamp(bin_edges[bin])) \
					& (df_sched['runway_sta'] < pd.Timestamp(bin_edges[bin]) + pd.Timedelta( str(binsize) + ' minutes')  )     ])

					plt_realized_arv[bin] = len(df_matm_arv[ (df_matm_arv['actual_runway'] == all_runways[rwy]) \
					&(df_matm_arv['arrival_runway_actual_time'] > pd.Timestamp(bin_edges[bin])) \
					&(df_matm_arv['arrival_runway_actual_time'] < pd.Timestamp(bin_edges[bin]) + pd.Timedelta( str(binsize) + ' minutes')  )     ])

					plt_residual_arv[bin] = plt_scheduled_arv[bin] - plt_realized_arv[bin]

				ax1 = plt.subplot2grid((2,4), (0,0), colspan=3)
				#### plot departures
				plt.plot(plt_scheduled_dept,'--',label='scheduled departures',linewidth=line_width_var ,marker='o',color='blue',alpha=0.8)
				plt.plot(plt_realized_dept,'-',label='realized departures',linewidth=line_width_var ,marker='o',color='blue',alpha=0.8)
				plt.bar(np.arange(len(plt_residual_dept)),plt_residual_dept,label='scheduled - realized departures',color='blue',alpha=0.4)

				##### plot arrivals
				plt.plot(plt_scheduled_arv,'--',label='scheduled arrivals',linewidth=line_width_var ,marker='s',color='grey',alpha=0.8)
				plt.plot(plt_realized_arv,'-',label='realized arrivals',linewidth=line_width_var ,marker='s',color='grey',alpha=0.8)
				plt.bar(np.arange(len(plt_residual_arv)),plt_residual_arv,label='scheduled - realized arrivals',color='grey',alpha=0.4)


				try:
					all_residual_dept[all_runways[rwy]].append(plt_residual_dept)
					all_residual_arv[all_runways[rwy]].append(plt_residual_arv)

					all_DD[all_runways[rwy]].append(DD)
					all_DA[all_runways[rwy]].append(DA)
					all_AD[all_runways[rwy]].append(AD)
					all_AA[all_runways[rwy]].append(AA)
				except:
					all_residual_dept[all_runways[rwy]] = []
					all_residual_arv[all_runways[rwy]] = []
					all_residual_dept[all_runways[rwy]].append(plt_residual_dept)
					all_residual_arv[all_runways[rwy]].append(plt_residual_arv)
					rwy_key.append(all_runways[rwy])

					all_DD[all_runways[rwy]] = []
					all_DA[all_runways[rwy]] = []
					all_AD[all_runways[rwy]] = []
					all_AA[all_runways[rwy]] = []
					all_DD[all_runways[rwy]].append(DD)
					all_DA[all_runways[rwy]].append(DA)
					all_AD[all_runways[rwy]].append(AD)
					all_AA[all_runways[rwy]].append(AA)

				plt.title('Runway ' + all_runways[rwy] + ' Runway Rate Analysis with ' +str(binsize) + ' Minute Bin')
				plt.xticks(np.arange(len(plt_residual_dept)),x_tick_vec,fontsize=6,rotation=45)
				plt.legend(fontsize=8,loc='lower left')
				plt.ylim([-(binsize+1),(binsize+1)])
				


				ax2 = plt.subplot2grid((2,4), (0,3))
				dept_cum_sum = np.cumsum(plt_residual_dept)
				arv_cum_sum = np.cumsum(plt_residual_arv)
				plt.plot(dept_cum_sum,'-',marker = 's',color='blue',alpha=0.8,label='departure')
				plt.plot(arv_cum_sum,'-',marker='s',color='grey',alpha=0.8,label='arrival')
				plt.xticks([0,len(x_tick_vec)-1],[x_tick_vec[0],x_tick_vec[len(x_tick_vec)-1]],fontsize=6,rotation=45)
				plt.legend(fontsize=8)

				

				ax3 =  plt.subplot2grid((2,4), (1,0))
				plt.hist(DD,bins=20,range=[0,200],edgecolor='black')
				plt.xlabel('Departure -> Departure')

				ax4 =  plt.subplot2grid((2,4), (1,1))
				plt.hist(AD,bins=20,range=[0,200],edgecolor='black')
				plt.xlabel('Arrival -> Departure')

				ax5 =  plt.subplot2grid((2,4), (1,2))
				plt.hist(DA,bins=20,range=[0,200],edgecolor='black')
				plt.xlabel('Departure -> Arrival')

				ax6 =  plt.subplot2grid((2,4), (1,3))
				plt.hist(AA,bins=20,range=[0,200],edgecolor='black')
				plt.xlabel('Arrival -> Arrival')





				plt.tight_layout()
				tmp_st = date_st.replace(' ','_')
				plt.savefig('figs/' + tmp_st.replace(':','.') + '_' + all_runways[rwy] + '_' + str(binsize)+ '_binsize_rate_analysis_v5.png')
				#plt.show()
				plt.close('all')

	return [all_residual_dept,all_residual_arv,rwy_key,all_DD,all_DA,all_AD,all_AA,x_tick_vec]

	#plt.show()
#plot_rate('2018-04-20',' 22:00:00')

date_vec = []
time_vec = []

# for i in range(2,32):
# 	if i < 10:
# 		num = '0' + str(i)
# 	else:
# 		num = str(i)
# 	date_vec.append('2018-01-' + num )
# 	time_vec.append(' 14:00:00')

# for i in range(1,29):
# 	if i < 10:
# 		num = '0' + str(i)
# 	else:
# 		num = str(i)
# 	date_vec.append('2018-02-' + num )
# 	time_vec.append(' 14:00:00')

for i in range(1,11):
	if i < 10:
		num = '0' + str(i)
	else:
		num = str(i)
	date_vec.append('2018-03-' + num )
	time_vec.append(' 14:00:00')



for i in range(11,32):
	if i < 10:
		num = '0' + str(i)
	else:
		num = str(i)
	date_vec.append('2018-03-' + num )
	time_vec.append(' 13:00:00')


for i in range(1,23):
	if i < 10:
		num = '0' + str(i)
	else:
		num = str(i)
	date_vec.append('2018-04-' + num )
	time_vec.append(' 13:00:00')

all_residual_dept = {}
all_residual_arv = {}
rwy_key = []
all_DD ={}
all_DA ={}
all_AD ={}
all_AA ={}
for d in range(len(date_vec)):
	[all_residual_dept,all_residual_arv,rwy_key,all_DD,all_DA,all_AD,all_AA,x_tick_vec] = plot_rate(date_vec[d],time_vec[d],all_residual_dept,all_residual_arv,rwy_key,all_DD,all_DA,all_AD,all_AA)

rwy_key = np.unique(rwy_key)

for rwy in range(len(rwy_key)):

	mean_vec = np.zeros(len(all_residual_dept[rwy_key[rwy]][0]))
	std_vec = np.zeros(len(all_residual_dept[rwy_key[rwy]][0]))
	mean_vec_arv = np.zeros(len(all_residual_dept[rwy_key[rwy]][0]))
	std_vec_arv = np.zeros(len(all_residual_dept[rwy_key[rwy]][0]))
	for bucket in range(len(all_residual_dept[rwy_key[rwy]][0])):
		val_bucket = []
		val_bucket_arv = []
		for day in range(len(all_residual_dept[rwy_key[rwy]])):
			val_bucket.append(all_residual_dept[rwy_key[rwy]][day][bucket])
			val_bucket_arv.append(all_residual_arv[rwy_key[rwy]][day][bucket])
		mean_vec[bucket] = np.mean(val_bucket)
		mean_vec_arv[bucket] = np.mean(val_bucket_arv)
		std_vec[bucket] = np.std(val_bucket)
		std_vec_arv[bucket] = np.std(val_bucket_arv)

		print(rwy_key[rwy])
		print('bucket number')
		print(bucket)
		print(val_bucket)

	plt.figure(figsize = (12,10))
	ax1 = plt.subplot2grid((2,4), (0,0), colspan=4)
	plt.plot(mean_vec,'-',label='mean departure residual',linewidth=2 ,marker='o',color='blue',alpha=0.8)
	plt.plot(mean_vec_arv,'-',label='mean arrival residual',linewidth=2 ,marker='o',color='grey',alpha=0.8)
	plt.fill_between( np.arange(len(mean_vec)) , mean_vec + std_vec , mean_vec - std_vec,color='blue',alpha=0.2)
	plt.fill_between( np.arange(len(mean_vec_arv)) , mean_vec_arv + std_vec_arv , mean_vec_arv - std_vec_arv,color='grey',alpha=0.2)
	
	
	x_tick_vec_2 = []
	for v in range(len(x_tick_vec)):
		x_tick_vec_2.append(str(x_tick_vec[v]).split(' ')[1])

	plt.title(rwy_key[rwy] + ' Error (Scheduled - Realized) with ' +str(binsize) + ' Minute Bin')
	plt.xticks(np.arange(len(x_tick_vec)),x_tick_vec_2,fontsize=6,rotation=45)
	plt.legend()
	#plt.show()
	

	ax3 =  plt.subplot2grid((2,4), (1,0))
	flat_list_DD = [item for sublist in all_DD[rwy_key[rwy]] for item in sublist]
	plt.hist(flat_list_DD,bins=20,range=[0,200],edgecolor='black')
	plt.xlabel('Departure -> Departure')

	ax4 =  plt.subplot2grid((2,4), (1,1))
	flat_list_AD = [item for sublist in all_AD[rwy_key[rwy]] for item in sublist]
	plt.hist(flat_list_AD,bins=20,range=[0,200],edgecolor='black')
	plt.xlabel('Arrival -> Departure')

	ax5 =  plt.subplot2grid((2,4), (1,2))
	flat_list_DA = [item for sublist in all_DA[rwy_key[rwy]] for item in sublist]
	plt.hist(flat_list_DA,bins=20,range=[0,200],edgecolor='black')
	plt.xlabel('Departure -> Arrival')

	ax6 =  plt.subplot2grid((2,4), (1,3))
	flat_list_AA = [item for sublist in all_AA[rwy_key[rwy]] for item in sublist]
	plt.hist(flat_list_AA,bins=20,range=[0,200],edgecolor='black')
	plt.xlabel('Arrival -> Arrival')



	plt.savefig(rwy_key[rwy]+ '_' + str(binsize) + '_all_rate_data_v5.png')







