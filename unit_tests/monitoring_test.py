# Monitoring Unit Tests
from dmm.monitoring import PrometheusSession
import time

m = PrometheusSession()
host_ip = '2605:d9c0:2:10::2:17' 
site_name = 'T2_US_Caltech_DTNs'

try:
    print(m.get_total_bytes_transmitted(host_ip, site_name, time.time()-1000, time.time()))
except:
    raise Exception("Failed get_total_data_transferred test")
finally:
    print("Passed total_data_transferred test")

try:
    print(m.get_average_throughput(host_ip, site_name, time.time()-1000, time.time()))
except:
    raise Exception("Failed get_average_throughput test")
finally:
    print("Passed total_get_average_throughput test")
