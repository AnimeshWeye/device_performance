import pandas as pd
import datetime
import fastavro
import os

getepoch=lambda x: pd.Timestamp(x).timestamp()-19800
getdate=lambda x: pd.datetime.fromtimestamp(x).date()
getdatetime=lambda x: pd.datetime.fromtimestamp(x)

gps_base_path = "s3://weye-archives/device/gps/year={}/month={}/date={}/"
hb_base_path = "s3://weye-archives/device/hb/year={}/month={}/date={}/"

gps_base_path_sp = "s3://weye-archives/device/gps/year={}/month={}/date={}/vehicleid={}/"
hb_base_path_sp = "s3://weye-archives/device/hb/year={}/month={}/date={}/vehicleid={}/"

gps_dir_path = "/data/device_dualsim/gps/year={}/month={}/date={}/vehicleid={}"
hb_dir_path = "/data/device_dualsim/hb/year={}/month={}/date={}/vehicleid={}"

gps_data_path = "/data/device_dualsim/gps/year={}/month={}/date={}/vehicleid={}"
hb_data_path = "/data/device_dualsim/hb/year={}/month={}/date={}/vehicleid={}"

vid_path = "/home/ubuntu/vibhor/IoT/device_performance/device_performance/vehicle_id.csv"

def get_avro_reader(log_file):
    if hasattr(log_file, 'read'):
        reader = fastavro.reader(log_file)
    else:
        reader = None
        try:
            source_file = open(log_file, 'rb')
        except Exception:
            raise
        else:
            reader = fastavro.reader(source_file)
    return reader

def get_gps_base_path(year, month, date):
    s3_path_gps = gps_base_path.format("%04d" % (year), "%02d" % (month), "%02d" % date)
    return s3_path_gps

def get_hb_base_path(year, month, date):
    s3_path_hb = hb_base_path.format("%04d" % (year), "%02d" % (month), "%02d" % date)
    return s3_path_hb

def get_gps_base_path_sp(year, month, date, vehicleid):
    s3_path_gps = gps_base_path_sp.format("%04d" % (year), "%02d" % (month), "%02d" % date, "%s" % vehicleid)
    return s3_path_gps

def get_hb_base_path_sp(year, month, date, vehicleid):
    s3_path_hb = hb_base_path_sp.format("%04d" % (year), "%02d" % (month), "%02d" % date, "%s" % vehicleid)
    return s3_path_hb

def get_hb_dir_path_erase():
    # s3_dir_gps = gps_dir_path.format("%04d" % (year), "%02d" % (month), "%02d" % date)
    s3_dir_gps = "/data/device_dualsim/hb"
    return s3_dir_gps

def get_gps_dir_path_erase():
    # s3_dir_gps = gps_dir_path.format("%04d" % (year), "%02d" % (month), "%02d" % date)
    s3_dir_gps = "/data/device_dualsim/gps"
    return s3_dir_gps

def get_gps_dir_path(year, month, date):
    s3_dir_gps = gps_dir_path.format("%04d" % (year), "%02d" % (month), "%02d" % date)
    # s3_dir_gps = "/data/device_dualsim/gps"
    return s3_dir_gps

def get_hb_dir_path(year, month, date):
    s3_dir_hb = hb_dir_path.format("%04d" % (year), "%02d" % (month), "%02d" % date)
    # s3_dir_hb = "/data/device_dualsim/hb"
    return s3_dir_hb

def get_gps_data_path(year, month, date, vehicle_id):
    s3_data_gps = gps_data_path.format("%04d" % (year), "%02d" % (month), "%02d" % date, "%d" % (vehicle_id))
    return s3_data_gps

def get_hb_data_path(year, month, date, vehicle_id):
    s3_data_hb = hb_data_path.format("%04d" % (year), "%02d" % (month), "%02d" % date, "%d" % (vehicle_id))
    return s3_data_hb
   
def downloadGpsFroms3(epochtime):
    from_date = getdate(epochtime)
    gps_storage_path = get_gps_dir_path(from_date.year, from_date.month, from_date.day)
    # read csv for reading vehicle id to download
    try:
        vid_df = pd.read_csv(vid_path)
    except Exception as e:
        print("The error is: {}".format(e))
    for index, v_id in enumerate(vid_df['vehicle_id']):
        gps_path = get_gps_base_path_sp(from_date.year, from_date.month, from_date.day, v_id)
        os.system("aws s3 --region ap-south-1 cp {} {} --recursive".format(gps_path,gps_storage_path))
        
def downloadHbFroms3(epochtime):
    from_date = getdate(epochtime)
    hb_storage_path = get_hb_dir_path(from_date.year, from_date.month, from_date.day)
    # read csv for reading vehicle id to download
    try:
        vid_df = pd.read_csv(vid_path)
    except Exception as e:
        print("The error is: {}".format(e))
    for index, v_id in enumerate(vid_df['vehicle_id']):
        hb_path = get_hb_base_path(from_date.year, from_date.month, from_date.day, v_id)
        os.system("aws s3 --region ap-south-1 cp {} {} --recursive".format(hb_path,hb_storage_path))
    
def fetch_raw_gps(l2):
    from_date = getdate(l2[0])
    to_date = getdate(l2[1])
    nod = int((to_date - from_date).days) #no_of_days
    s3_gps_data_path = []
    for i in range(0, nod + 1):
        for_date = from_date + datetime.timedelta(days=i)
        s3_gps_data_path.append(get_gps_data_path(for_date.year, for_date.month, for_date.day, l2[2]))
    final_df = pd.DataFrame()  # concat data from all files to one df
    data_len = []  # number of data_points/pings
    count = 0
    data_df = pd.DataFrame()  # need to remove just for testing
    for d in s3_gps_data_path:
        for path, subdirs, files in os.walk(d):
            for name in files:
                flag = False
                final_data = []
                count += 1
                file_name = os.path.join(path, name)
                try:
                    data = get_avro_reader(file_name)
                    flag = True
                except Exception as e:
                    a = 1
                if flag:
                    try:
                        for record in data:
                            final_data.append(record)
                        data_df = pd.DataFrame(final_data)
                        data_len.append(len(data_df))
                        final_df = pd.concat([final_df, data_df])
                    except Exception as e:
                        data_len.append(e)
                else:
                    data_len.append("error in get_avro_reader function")
    if len(final_df) > 0:
        final_df.rename(columns={'createdat': 'created'}, inplace=True)
        final_df['created'] = final_df.apply(
            lambda x: x['created'] * 1000 if len(str(x['created'])) < 13 else x['created'], axis=1)
        final_df = final_df.sort_values(by=['time','created'], ascending=True).reset_index(drop=True)
        final_df = final_df.drop_duplicates(subset=['time'], keep='last').reset_index(drop=True)
        final_df = final_df[(final_df['time'] >= l2[0]) & (final_df['time'] <= l2[1])].reset_index(drop=True)
    gps_data = final_df
    return gps_data

def fetch_hb(l1):
    from_date = getdate(l1[0])
    to_date = getdate(l1[1])
    nod = int((to_date - from_date).days) #no_of_days
    s3_hb_data_path = []
    for i in range(0, nod + 1):
        for_date = from_date + datetime.timedelta(days=i)
        s3_hb_data_path.append(get_hb_data_path(for_date.year, for_date.month, for_date.day, l1[2]))
    final_df = pd.DataFrame()  # concat data from all files to one df
    data_len = []  # number of data_points/pings
    count = 0
    data_df = pd.DataFrame()
    for d in s3_hb_data_path:
        for path, subdirs, files in os.walk(d):
            for name in files:
                flag = False
                final_data = []
                count += 1
                file_name = os.path.join(path, name)
                try:
                    data = get_avro_reader(file_name)
                    flag = True
                except Exception as e:
                    print(e)
                if flag:
                    try:
                        for record in data:
                            final_data.append(record)
                        data_df = pd.DataFrame(final_data)
                        data_len.append(len(data_df))
                        final_df = pd.concat([final_df, data_df])
                        final_df['created'] = final_df.apply(
                            lambda x: x['created'] * 1000 if len(str(x['created'])) < 13 else x['created'], axis=1)
                    except Exception as e:
                        data_len.append(e)
                else:
                    data_len.append("error in get_avro_reader function")
    if len(final_df) > 0:
        final_df = final_df[(final_df['created'] >= l1[0] * 1000) & (final_df['created'] <= l1[1] * 1000)]
    hb_data = final_df
    return hb_data