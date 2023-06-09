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


def disk_space(session_1):
    name_list = []
    for i in range(1, 100):
        name_ = 'vehicle_' + str(i)
        name_list.append(name_)
    DiskSpaceMeasurementList = name_list
    DataTypeList = [
        TSDataType.FLOAT for _ in range(len(DiskSpaceMeasurementList))
    ]
    EncodingList = [
        TSEncoding.PLAIN for _ in range(len(DiskSpaceMeasurementList))
    ]
    CompressorList = [
        Compressor.SNAPPY for _ in range(len(DiskSpaceMeasurementList))
    ]
    session_1.create_aligned_time_series("root.test.disk_space", DiskSpaceMeasurementList, DataTypeList, EncodingList,
                                         CompressorList)

    # create timestamp
    startTime = "2023-5-24 00:00:00"
    timeArray = time.strptime(startTime, "%Y-%m-%d %H:%M:%S")
    StartTimeStamp = int(time.mktime(timeArray))
    stopTime = "2023-6-1 00:00:00"
    timeArray = time.strptime(stopTime, "%Y-%m-%d %H:%M:%S")
    StopTimeStamp = int(time.mktime(timeArray))

    # insert static
    value_list = []
    timestamp_list = []
    for i in range(StartTimeStamp, StopTimeStamp, 10):
        list_ = []
        timestamp_list.append(i * 1000)
        for t in range(len(DiskSpaceMeasurementList)):
            if t % 6 ==0:
                a_1 = random.uniform(50.0, 60.0)
                list_.append(a_1)
            if t % 6 ==1:
                a_2 = random.uniform(50.0, 55.0)
                list_.append(a_2)
            if t % 6 ==2:
                a_3 = random.uniform(80.0, 90.0)
                list_.append(a_3)
            if t % 6 ==3:
                a_4 = random.uniform(30.0, 40.0)
                list_.append(a_4)
            if t % 6 ==4:
                a_5 = random.uniform(20.0, 25.0)
                list_.append(a_5)
            if t % 6 ==5:
                a_6 = random.uniform(10.0, 15.0)
                list_.append(a_6)
        value_list.append(list_)
    tablet = Tablet(
        "root.test.disk_space", DiskSpaceMeasurementList, DataTypeList, value_list, timestamp_list
    )
    session.insert_aligned_tablet(tablet)


disk_space(session)
