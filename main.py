from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
import datetime
import time


def TimetoTimeSpan(timeInfo):
    timeArray = time.strptime(timeInfo, "%Y-%m-%d %H:%M:%S")
    timeSpan = int(time.mktime(timeArray))
    return timeSpan


ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
session.open(False)

# set and delete databases
session.set_storage_group("root.sg_test_01")

session.create_time_series(
    "root.sg_test_01.d_01.s_01", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY
)

session.insert_record("root.sg_test_01.d_01", TimetoTimeSpan("2023-05-09 22:00:00"), "s_01", TSDataType.BOOLEAN, False)
