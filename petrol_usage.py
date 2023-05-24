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


def petrol_usage(session_1):
    petrolMeasureList = [
        "vehicle_01",
        "vehicle_02",
        "vehicle_03",
        "vehicle_04",
        "vehicle_05",
        "vehicle_06",
    ]
    DataTypeList = [
        TSDataType.FLOAT for _ in range(len(petrolMeasureList))
    ]
    EncodingList = [
        TSEncoding.PLAIN for _ in range(len(petrolMeasureList))
    ]
    CompressorList = [
        Compressor.SNAPPY for _ in range(len(petrolMeasureList))
    ]
    session_1.create_aligned_time_series("root.test.petrol_usage", petrolMeasureList, DataTypeList, EncodingList,
                                         CompressorList)

    # create timestamp
    startTime = "2023-5-24 00:00:00"
    timeArray = time.strptime(startTime, "%Y-%m-%d %H:%M:%S")
    StartTimeStamp = int(time.mktime(timeArray))
    stopTime = "2023-6-1 00:00:00"
    timeArray = time.strptime(stopTime, "%Y-%m-%d %H:%M:%S")
    StopTimeStamp = int(time.mktime(timeArray))

    value_list = []
    timestamp_list = []
    for i in range(StartTimeStamp, StopTimeStamp, 300):
        list_ = []
        timestamp_list.append(i * 1000)
        for _ in range(len(petrolMeasureList)):
            list_.append(random.uniform(10.0, 60.0))
        value_list.append(list_)
    tablet = Tablet(
        "root.test.petrol_usage", petrolMeasureList, DataTypeList, value_list, timestamp_list
    )
    session.insert_aligned_tablet(tablet)
