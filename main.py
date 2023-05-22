from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
import datetime
import time
import random
from iotdb.utils.Tablet import Tablet

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
session.open(False)

# create database
session.set_storage_group("root.test")

# set aligned timeseries
measurement_list = [
    "sensor_1",
    "sensor_2",
    "sensor_3",
]
datatype_list = [
    TSDataType.FLOAT,
    TSDataType.FLOAT,
    TSDataType.FLOAT,
]
encoding_list = [
    TSEncoding.PLAIN for _ in range(len(datatype_list))
]
compressor_list = [
    Compressor.SNAPPY for _ in range(len(datatype_list))
]
session.create_aligned_time_series("root.test.vehicle_01", measurement_list, datatype_list, encoding_list, compressor_list)

#create timestamp
a = "2023-5-1 08:00:00"
timeArray = time.strptime(a, "%Y-%m-%d %H:%M:%S")
timeStamp = int(time.mktime(timeArray))

# insert a tablet
value_list = []
timestamp_list = []
for i in range(10000):
    list_ = []
    timestamp_list.append(timeStamp*1000+i*10000)
    for j in range(len(datatype_list)):
        if j == 1:
            list_.append(random.uniform(90.0, 100.0))
        if j == 2:
            list_.append(random.uniform(50.0, 60.0))
        if j == 3:
            list_.append(random.uniform(0.0, 10.0))
    value_list.append(list_)
tablet = Tablet(
    "root.test.vehicle_01",measurement_list,datatype_list,value_list,timestamp_list
)
session.insert_aligned_tablet(tablet)


