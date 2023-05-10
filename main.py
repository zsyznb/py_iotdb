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

# insert a tablet
value_list = []
timestamp_list = []
for i in range(100):
    list_ = []
    timestamp_list.append(int(time.time() * 1000)-i*1000*30)
    for _ in range(len(datatype_list)):
        list_.append(random.uniform(50.0, 100.0))
    value_list.append(list_)
tablet = Tablet(
    "root.test.vehicle_01",measurement_list,datatype_list,value_list,timestamp_list
)
session.insert_aligned_tablet(tablet)


