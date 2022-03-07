# Monitoring Unit Tests
from dmm.monitoring import MonitoringSession
import time

m = MonitoringSession()
host_ip = '172.19.0.1' 
site_name = 'pi.aashayarora.com:9100'

try:
    m.get_total_data_transferred(host_ip, site_name, time.time()-1000)
except:
    raise Exception("Failed get_total_data_transferred test")
finally:
    print("Passed total_data_transferred test")

try:
    m.get_average_throughput(host_ip, site_name, time.time()-1000)
except:
    raise Exception("Failed get_average_throughput test")
finally:
    print("Passed total_get_average_throughput test")