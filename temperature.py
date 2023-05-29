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


# CPU memory
def temperature(session_1):
    name_list = []
    for i in range(1, 100):
        name_ = 'vehicle_' + str(i)
        name_list.append(name_)

    temperatureMeasurementList = name_list
    DataTypeList = [
        TSDataType.FLOAT for _ in range(len(temperatureMeasurementList))
    ]
    EncodingList = [
        TSEncoding.PLAIN for _ in range(len(temperatureMeasurementList))
    ]
    CompressorList = [
        Compressor.SNAPPY for _ in range(len(temperatureMeasurementList))
    ]
    session_1.create_aligned_time_series("root.test.temperature", temperatureMeasurementList, DataTypeList,
                                         EncodingList,
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
        for t in range(len(temperatureMeasurementList)):
            if t % 3 == 0:
                a = random.uniform(85.0, 90.0)
                list_.append(a)
            if t % 3 == 1:
                b = random.uniform(70.0, 75.0)
                list_.append(b)
            if t % 3 == 2:
                c = random.uniform(95.0, 100.0)
                list_.append(c)
        value_list.append(list_)
    tablet = Tablet(
        "root.test.temperature", temperatureMeasurementList, DataTypeList, value_list, timestamp_list
    )
    session.insert_aligned_tablet(tablet)


temperature(session)
