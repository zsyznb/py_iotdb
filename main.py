from iotdb.Session import Session

ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8")
session.open(False)

# set and delete databases
session.set_storage_group("root.sg_test_01")