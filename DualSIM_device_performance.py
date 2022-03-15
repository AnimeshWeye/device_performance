#!/usr/bin/env python
# coding: utf-8

from turtle import delay, distance
import pandas as pd
import sqlalchemy
import shutil
from tqdm import tqdm
from sqlalchemy import create_engine
galaxy=create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format("byogesh","W1MOi90gFZOH"))
import datetime
import time as tm
import json
# import datetime as dt
import time as tm
#import upload_s3_to_redshift_new
import s3_module
import pickle
import numpy as np
from pymongo import MongoClient
from math import radians, cos, sin, asin, sqrt
import pymysql
from datetime import datetime,timedelta
import json
import urllib.request
import warnings
import csv
warnings.filterwarnings("ignore")
gettime=lambda x: pd.Timestamp(x).timestamp()
getdate=lambda x: pd.datetime.fromtimestamp(x)
getDay=lambda x: pd.datetime.fromtimestamp(x).date()
dttoday=int(tm.time()-tm.time()%86400-19800)

rearrange = 0

pd.set_option("display.precision", 9)

# path for csv
date_csv_path = "/home/ubuntu/vibhor/IoT/device_performance/device_performance/dates.csv"
path_vNum = "/home/ubuntu/vibhor/IoT/device_performance/device_performance/vehicle_number.csv"
vid_path = "/home/ubuntu/vibhor/IoT/device_performance/device_performance/vehicle_id.csv"
report_path = "/home/ubuntu/vibhor/IoT/device_performance/device_performance/reports/report_{}_{}_{}.csv"

def haversine(L1,L2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """

    lon1=L1[0]
    lat1=L1[1]
    lon2=L2[0]
    lat2=L2[1]
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    r = 6371000 # Radius of earth in meters. Use 3956 for miles
    return c * r

def ping_analysis(l):
    vid=l[2]
    try:
        lc=s3_module.fetch_raw_gps(l)
        hb=s3_module.fetch_hb(l)
        if (len(hb)==0)&(len(lc)>0):
            lc['created']=lc['created']/1000
            lc.sort_values('created',inplace=True,ascending=True)
            lc2=lc.copy()
            lc3=lc.copy()

            lc.reset_index(drop=True,inplace=True)
            lc['date_time']=lc['time'].apply(getdate)
            lc.set_index('date_time',inplace=True)
            T_counts_array=lc.location.resample('T').count().values
            min_wise=len(T_counts_array)-len(T_counts_array[T_counts_array==0])

            lc2['delay']=lc2['created']-lc2['time']
            lc2=lc2[lc2['delay']<20].reset_index(drop=True)
            if len(lc2)>0:
                lc2.reset_index(drop=True,inplace=True)
                lc2['date_time']=lc2['created'].apply(getdate)
                lc2.set_index('date_time',inplace=True)
                T_counts_array2=lc2.location.resample('T').count().values
                live_time=len(T_counts_array2)-len(T_counts_array2[T_counts_array2==0])
                print("vid: " + str(vid) + " min_wise: " + str(min_wise) + "," + str(len(T_counts_array)) + "," + str(len(T_counts_array[T_counts_array==0])) + " live_time: " + str(live_time) + "," + str(len(T_counts_array2)) + "," + str(len(T_counts_array2[T_counts_array2==0])))
                print("consistency: ")
                for x in range(0, len(T_counts_array)):
                    print(T_counts_array[x]),
                print("")
                print("live: ")
                for x in range(0, len(T_counts_array2)):
                    print(T_counts_array2[x]),
                
                lc5 = lc2[lc2['delay']>20]
                print(lc5)
            else:
                live_time=0

            try:
                gsm_average=lc3['gsmPct'].mean()
            except:
                gsm_average=0
            
            heart_beat='No heartbeat'       

        elif (len(hb)>0)&(len(lc)>0):
            hb['created']=hb['created']/1000
            hb.sort_values('created',inplace=True,ascending=True)
            hb2=hb.copy()

            hb_times=hb['created']
            try:
                lc_times=lc['time']
                complete_times=hb_times.append(lc_times)

            except:
                complete_times=hb_times

            random=list(range(0,len(complete_times),1))
            combined=pd.DataFrame()
            combined['time']=complete_times
            combined['random']=random
            combined['date_time']=combined['time'].apply(getdate)
            combined.set_index('date_time',inplace=True)
            T_counts_array=combined.time.resample('T').count().values          
            min_wise=len(T_counts_array)-len(T_counts_array[T_counts_array==0])

            lc['created']=lc['created']/1000
            lc.sort_values('created',inplace=True,ascending=True)
            lc['delay']=lc['created']-lc['time']
            lc=lc[lc['delay']<20].reset_index(drop=True)
            hb_times2=hb['created']
            try:
                lc_times2=lc['created']
                complete_times2=hb_times2.append(lc_times2)

            except:
                complete_times2=hb_times2

            random2=list(range(0,len(complete_times2),1))
            combined2=pd.DataFrame()
            combined2['time']=complete_times2
            combined2['random']=random2
            combined2['date_time']=combined2['time'].apply(getdate)    
            combined2.set_index('date_time',inplace=True)
            T_counts_array2=combined2.random.resample('T').count().values          
            live_time=len(T_counts_array2)-len(T_counts_array2[T_counts_array2==0])
            
            try:
                gsm_average=hb2['gsmPct'].mean()
            except:
                gsm_average=0
            
            heart_beat='Has heartbeat'

        elif (len(hb)>0)&(len(lc)==0):
            hb['created']=hb['created']/1000
            hb.sort_values('created',inplace=True,ascending=True)
            hb2=hb.copy()

            hb.reset_index(drop=True,inplace=True)
            hb['date_time']=hb['created'].apply(getdate)
            hb.set_index('date_time',inplace=True)
            T_counts_array=hb.voltPct.resample('T').count().values
            min_wise=len(T_counts_array)-len(T_counts_array[T_counts_array==0])
            live_time = min_wise
            
            try:
                gsm_average=hb2['gsmPct'].mean()
            except:
                gsm_average=0

            heart_beat='Has heartbeat' 

        elif (len(hb)==0)&(len(lc)==0):
            min_wise=0
            live_time=0
            gsm_average=0
            heart_beat='No data'

    except:
        print("error")
        min_wise=0
        live_time=0
        gsm_average=0
        heart_beat='No data' 
        
    return [vid,round(min_wise/14.4,2),round(live_time/14.4,2),round(gsm_average,2),heart_beat,getDay((l[0]+l[1])/2)]
# yr, mnth, dy
def run_etl(yr, mnth, dy):
    import datetime as dt
    # print("Started at {}".format(pd.to_datetime(tm.time()+19800,unit='s')))
    # print(type(yr), type(mnth), type(dy))
    try:
        # take input from user
        # Day for analysys
        # yyyy-mm-dd
        # analysis_date = input("Enter Day for Analysis (YYYY-MM-DD): ")
        print("Enter Date (D + 2) to generate report for date D;\ne.g: to generate report for 2022-01-13, enter 2022-01-15.")
        # yr = input("Enter year: ")
        # mnth = input("Enter month: ")
        # dy = input("Enter day: ")
        print(type(yr), type(mnth), type(dy))
        analysis_date = dt.datetime(int(yr), int(mnth), int(dy))
        # analysis_date = dt.datetime(yr, mnth, dy)
        try :
            today=dt.date()
        except:
            today=dt.date.today()

        if (gettime(analysis_date) > (gettime(today) - (2*24*60*60))):
            print("out of date")
            return None

        # read csv for vehicle numbers
        try:
            vehicle_num = pd.read_csv(path_vNum)
            vnum_sql = ""
            for index, vnum in enumerate(vehicle_num['vehicle_number']):
                vnum_sql += "'" + str(vnum) + "'"
                if (index < (len(vehicle_num['vehicle_number']) - 1)):
                    vnum_sql += ','
        except Exception as e:
            print("The error is: {}".format(e))

        # time adjustment for start and end time of analysis
        start=gettime(analysis_date) - (29.5*60*60)
        end=gettime(analysis_date) - (5.5*60*60)

        # erase datetime for erasing previous day redundant data
        erase_date=getDay(start-(86400*2))

        try:
            shutil.rmtree(s3_module.get_hb_dir_path_erase_sp())
            shutil.rmtree(s3_module.get_gps_dir_path_erase_sp())
        except Exception as e:    
            print(e)
        
        # Query 1: query for extracting vehicle id for corresponding vehicle numbers stored in csv
        query="""SELECT vehicle_number, vehicle_id,date(actual_live_time) as installation_date,model_name
                                    FROM analytics.vehicle_details 
                                    WHERE date(actual_live_time) < CURRENT_DATE - INTERVAL'1 day' and 
                                    vehicle_state='LIVE' and 
                                    vehicle_number in ({})""".format(vnum_sql)
        inst_veh=pd.read_sql(query , galaxy)
        print(inst_veh)

        # from multiprocessing import Pool
        # pool = Pool(processes=2)
        # pool.apply_async(s3_module.downloadGpsFroms3, [start])
        # pool.apply_async(s3_module.downloadHbFroms3, [start])
        # pool.close()
        # pool.join()

        # Downloading data and making part of new sql query 
        vid_sql = ""
        for index, vid in enumerate(inst_veh['vehicle_id']):
            from multiprocessing import Pool
            pool = Pool(processes=4)
            pool.apply_async(s3_module.downloadGpsFroms3_sp, [start, str(vid)])
            pool.apply_async(s3_module.downloadHbFroms3_sp, [start, str(vid)])
            pool.apply_async(s3_module.downloadGpsFroms3_sp, [(start - (24*60*60)), str(vid)])
            pool.apply_async(s3_module.downloadHbFroms3_sp, [(start - (24*60*60)), str(vid)])
            pool.close()
            pool.join()
            # s3_module.downloadHbFroms3_sp(start, str(vid))
            # s3_module.downloadGpsFroms3_sp(start, str(vid))
            # s3_module.downloadHbFroms3_sp((start - (24*60*60)), str(vid))
            # s3_module.downloadGpsFroms3_sp((start - (24*60*60)), str(vid))
            vid_sql += str(vid)
            if (index < (len(inst_veh['vehicle_id']) - 1)) :
                vid_sql += ','
        print(vid_sql)

        # Query2: query to extract no-info data according to vid extracted earlier
        query="""select vehicleid as vehicle_id,date(timestamp 'epoch' + ((fromtime+19800) * interval '1 second')) as analysis_for_day,
                        count(*) as no_info_instances,round(sum((totime-fromtime)*1.00/3600)/no_info_instances,2) as avg_no_info_hrs
                 from trucking.trucking_mongo_devicehistory
                 where analysis_for_day='{}' and
                 vehicle_id in ({})  
                 group by 1,2""".format(str(analysis_date-dt.timedelta(days=2)), vid_sql)
        no_info_data=pd.read_sql(query , galaxy)
        # print("query2:")
        # print(no_info_data)

        # Query3: query for extracting distance data for online and no-info time
        query="""select vehicleid as vehicle_id,date(timestamp 'epoch' + ((time+19800) * interval '1 second')) as analysis_for_day,
                        round(distance/1000,2) as total_km,round(noinfodistance/1000,2) as no_info_km
                 from analytics.vehicledaydata
                 where analysis_for_day='{}' and
                 vehicle_id in ({}) """.format(str(analysis_date-dt.timedelta(days=2)), vid_sql)
        distance_data=pd.read_sql(query , galaxy)
        # print("query3:")
        # print(distance_data)

        inst_veh['installation_date']=pd.to_datetime(inst_veh['installation_date'])

        installed15=inst_veh
        installed15.sort_values(by='installation_date',ascending=True,inplace=True)

        analysis_of=list(installed15['vehicle_id'].unique())
        vehicle_list=[[int(start - (24*60*60)),int(end - (24*60*60)),int(x)] for x in analysis_of]

        final=[]
        for j in tqdm(range(0, len(vehicle_list), 1000)):
            import multiprocessing
            pool = multiprocessing.Pool(2)
            results1=pool.map(ping_analysis,vehicle_list[j:(j+1000)])
            # results1 = ping_analysis(vehicle_list[j:(j+1000)])
            pool.close()
            pool.join()
            final += results1
        data1=pd.DataFrame(final,columns=['vehicle_id','consistency_pct','live_pct','gsm_average','heart_beat','analysis_for_day'])
        result3=pd.merge(data1 , installed15[['vehicle_number','vehicle_id','model_name','installation_date']],how='left',on='vehicle_id')
        result2=pd.merge(result3 , no_info_data, how='left',on=['vehicle_id','analysis_for_day'])
        result=pd.merge(result2 , distance_data, how='left',on=['vehicle_id','analysis_for_day'])
        result.fillna(0,inplace=True)
        result['analysis_for_day']=result['analysis_for_day'].apply(lambda x : pd.to_datetime(x , format="%Y-%m-%d",errors='coerce'))
        result['days_post_installation']=result['analysis_for_day']-result['installation_date']
        result['days_post_installation']=result.apply(lambda x : x['days_post_installation'].days,axis=1)
        result['online']=result['consistency_pct'].apply(lambda x : False if x==0 else True)
        #result.to_pickle('/home/ubuntu/vibhor/IoT/master_device_performance/check.pkl')
        # print(data1)
        # print(result3)
        # print(result2)
        print(result.columns)
        final_result = result[['analysis_for_day', 'vehicle_id', 'vehicle_number', 'model_name', 'consistency_pct', 'live_pct', 'gsm_average', 'no_info_instances', 'days_post_installation']]
        # result3.to_csv(report_path.format(str(getDay(gettime(analysis_date-dt.timedelta(days=2))).year), str(getDay(gettime(analysis_date-dt.timedelta(days=2))).month), str(getDay(gettime(analysis_date-dt.timedelta(days=2))).day)))
        print(final_result)
        print(len(final_result))
        col = ['analysis_for_day', 'vehicle_id', 'vehicle_number', 'model_name', 'consistency_pct', 'live_pct', 'gsm_average', 'no_info_instances', 'days_post_installation']
        arranged_report = pd.DataFrame(columns=col)
        if rearrange:
            for x in range(len(final_result)) :
                print("rearranging report")
                if((final_result['model_name'][x] == "WEYE01") | (final_result['model_name'][x] == "TMG")):
                    vhnum_str = final_result['vehicle_number'][x]
                    vhnum_str_last = vhnum_str[len(vhnum_str) - 6 : len(vhnum_str)]
                    search_index = final_result['vehicle_number'].str.find(vhnum_str_last)
                    for y in range(len(final_result)) :
                        if (search_index[y] > 0):
                            arranged_report = arranged_report.append(final_result.loc[[y]])
                
            print(arranged_report)
            arranged_report.to_csv(report_path.format(str(getDay(gettime(analysis_date-dt.timedelta(days=2))).year), str(getDay(gettime(analysis_date-dt.timedelta(days=2))).month), str(getDay(gettime(analysis_date-dt.timedelta(days=2))).day)))
        else:
            final_result.to_csv(report_path.format(str(getDay(gettime(analysis_date-dt.timedelta(days=2))).year), str(getDay(gettime(analysis_date-dt.timedelta(days=2))).month), str(getDay(gettime(analysis_date-dt.timedelta(days=2))).day)))
        print("Done1")

        # rearranging report : final_result
        # for x in range(len)
        
        # previous_data=pd.read_sql("select * from analytics.master_device_performance limit 1",galaxy)
        # result=result[previous_data.columns]
        # print("Done2")
        # data=result
        # table_name='master_device_performance'
        # update_type='append_concat'
        # if update_type != 'update_table':
        #    col_name=None
        # else:
        #    col_name=etl_data[3]
        # print("Done3")
        # upload_s3_to_redshift_new.upload_to_s3(data,table_name)
        # print("Done4")
        # upload_s3_to_redshift_new.update_execute(update_type,data,table_name,col_name)
        # print("Done5")
        # upload_s3_to_redshift_new.delete_from_s3(table_name)
        # print("Done6")
   
    except Exception as e:
        print("run_etl:The error is: {}".format(e))
        
    # print("Completed at {}".format(pd.to_datetime(tm.time()+19800,unit='s')))
    
    return None

# run_etl()

date_csv = pd.read_csv(date_csv_path)
print("Started at {}".format(pd.to_datetime(tm.time()+19800,unit='s')))
for dt in range(len(date_csv)):
    yr = int(date_csv['yr'][dt])
    mnth = int(date_csv['mnth'][dt])
    dy = int(date_csv['dy'][dt])
    print(type(yr), type(mnth), type(dy))
    try:
        run_etl(yr, mnth, dy)
    except Exception as e:
        print("error in run_etl is {}".format(e))

print("Completed at {}".format(pd.to_datetime(tm.time()+19800,unit='s')))