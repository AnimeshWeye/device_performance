#!/usr/bin/env python
# coding: utf-8

from turtle import delay
import pandas as pd
import sqlalchemy
import shutil
from tqdm import tqdm
from sqlalchemy import create_engine
galaxy=create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format("byogesh","W1MOi90gFZOH"))
import datetime
import time as tm
import json
import datetime as dt
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

pd.set_option("display.precision", 9)

# path for vehicle number csv
path_vNum = "/home/ubuntu/vibhor/IoT/device_performance/device_performance/vehicle_number.csv"
vid_path = "/home/ubuntu/vibhor/IoT/device_performance/device_performance/vehicle_id.csv"

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
        min_wise=0
        live_time=0
        gsm_average=0
        heart_beat='No data' 
        
    return [vid,round(min_wise/14.4,2),round(live_time/14.4,2),round(gsm_average,2),heart_beat,getDay((l[0]+l[1])/2)]

def run_etl():
    print("Started at {}".format(pd.to_datetime(tm.time()+19800,unit='s')))
    
    try:
        # take input from user
        # Day for analysys
        # yyyy-mm-dd
        # analysis_date = input("Enter Day for Analysis (YYYY-MM-DD): ")
        # print("Analysis Day: " + analysis_date)
        analysis_date = "2022-01-13"
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

            #print(vnum_sql)
        except Exception as e:
            print("The error is: {}".format(e))
        
        start=gettime(analysis_date)
        end=gettime(analysis_date) + (24*60*60)
        # print(start, getDay(start))

        # erase_date=getDay(start-(86400*2))
        try:
            shutil.rmtree(s3_module.get_hb_dir_path_erase(), ignore_errors = True)
            # shutil.rmtree(s3_module.get_gps_dir_path_erase(), ignore_errors = True)
        except Exception as e:    
            print(e)

        # from multiprocessing import Pool
        # pool = Pool(processes=2)
        # pool.apply_async(s3_module.downloadGpsFroms3, [start])
        # pool.apply_async(s3_module.downloadHbFroms3, [start])
        # pool.close()
        # pool.join()

        query="""SELECT vehicle_id,date(actual_live_time) as installation_date,model_name
                                    FROM analytics.vehicle_details 
                                    WHERE date(actual_live_time) < CURRENT_DATE - INTERVAL'1 day' and 
                                    vehicle_state='LIVE' and 
                                    vehicle_number in ({})""".format(vnum_sql)
        # print(query)
        inst_veh=pd.read_sql(query , galaxy)
        # vid_df = inst_veh['vehicle_id']
        # vid_df.to_csv(vid_path)
        # delay(5)
        
        # s3_module.downloadGpsFroms3(start)
        # s3_module.downloadHbFroms3_sp(start, vid)

        vid_sql = ""
        for index, vid in enumerate(inst_veh['vehicle_id']):
            s3_module.downloadHbFroms3_sp(start, str(vid))
            vid_sql += str(vid)
            if (index < (len(inst_veh['vehicle_id']) - 1)) :
                vid_sql += ','
        print(vid_sql)

        # query="""select vehicleid as vehicle_id,date(timestamp 'epoch' + ((fromtime+19800) * interval '1 second')) as analysis_for_day,
        #                 count(*) as no_info_instances,round(sum((totime-fromtime)*1.00/3600)/no_info_instances,2) as avg_no_info_hrs
        #          from trucking.trucking_mongo_devicehistory
        #          where analysis_for_day='{}' and
        #          vehicle_id in ({})  
        #          group by 1,2""".format(str(analysis_date),vid_sql)
        # #print(query)
        # no_info_data=pd.read_sql(query , galaxy)
        # # print(no_info_data)

        # query="""select vehicleid as vehicle_id,date(timestamp 'epoch' + ((time+19800) * interval '1 second')) as analysis_for_day,
        #                 round(distance/1000,2) as total_km,round(noinfodistance/1000,2) as no_info_km
        #          from analytics.vehicledaydata
        #          where analysis_for_day='{}' """.format(str(analysis_date))
        # distance_data=pd.read_sql(query , galaxy)

        # inst_veh['installation_date']=pd.to_datetime(inst_veh['installation_date'])

        # installed15=inst_veh
        # installed15.sort_values(by='installation_date',ascending=True,inplace=True)

        # analysis_of=list(installed15['vehicle_id'].unique())
        # vehicle_list=[[int(start),int(end),int(x)] for x in analysis_of]

        # final=[]
        # for j in tqdm(range(0,len(vehicle_list),1000)):
        #     import multiprocessing
        #     pool = multiprocessing.Pool(10)
        #     results1=pool.map(ping_analysis,vehicle_list[j:(j+1000)])
        #     pool.close()
        #     pool.join()
        #     final+=results1
        # data1=pd.DataFrame(final,columns=['vehicle_id','consistency_pct','live_pct','gsm_average','heart_beat','analysis_for_day'])
        # result3=pd.merge(data1 , installed15[['vehicle_id','model_name','installation_date']],how='left',on='vehicle_id')
        # result2=pd.merge(result3 , no_info_data, how='left',on=['vehicle_id','analysis_for_day'])
        # result=pd.merge(result2 , distance_data, how='left',on=['vehicle_id','analysis_for_day'])
        # result.fillna(0,inplace=True)
        # result['analysis_for_day']=result['analysis_for_day'].apply(lambda x : pd.to_datetime(x , format="%Y-%m-%d",errors='coerce'))
        # result['days_post_installation']=result['analysis_for_day']-result['installation_date']
        # result['days_post_installation']=result.apply(lambda x : x['days_post_installation'].days,axis=1)
        # result['online']=result['consistency_pct'].apply(lambda x : False if x==0 else True)
        # #result.to_pickle('/home/ubuntu/vibhor/IoT/master_device_performance/check.pkl')
        # print(data1)
        print("Done1")
        
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
        
    print("Completed at {}".format(pd.to_datetime(tm.time()+19800,unit='s')))
    
    return None

run_etl()
