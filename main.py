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
def cpu_memory(session_1):
    session.set_storage_group("root.test")
    CpuMemoryMeasurementList = [
        "CPU",
        "memory",
    ]
    DataTypeList = [
        TSDataType.FLOAT,
        TSDataType.FLOAT,
    ]
    EncodingList = [
        TSEncoding.PLAIN for _ in range(len(CpuMemoryMeasurementList))
    ]
    CompressorList = [
        Compressor.SNAPPY for _ in range(len(CpuMemoryMeasurementList))
    ]
    session_1.create_aligned_time_series("root.test.cpu_memory", CpuMemoryMeasurementList, DataTypeList, EncodingList,
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
        a = random.uniform(50.0, 70.0)
        b = random.uniform(500.0, 550.0)
        list_.append(a)
        list_.append(b)
        value_list.append(list_)
    tablet = Tablet(
        "root.test.cpu_memory", CpuMemoryMeasurementList, DataTypeList, value_list, timestamp_list
    )
    session.insert_aligned_tablet(tablet)


cpu_memory(session)
