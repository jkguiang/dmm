# for a prometheus instance running on P_HOST, get network metrics via http api and perform required ops
from os import environ as env
import yaml
import requests
import time

class MonitoringSession(object):
    def __init__(self) -> None:
        with open("config.yaml", "r") as f_in:
            config = yaml.safe_load(f_in)
        prometheus_host = config["prometheus"]["host"]
        prometheus_port = config["prometheus"]["port"]
        self.prometheus_addr = f"http://{prometheus_host}:{prometheus_port}"
        self.session = requests.Session()
        
        self.dev_map = dict()
        self.get_dev_map() 
        
    def get_dev_map(self) -> None:
        query = {"query": "node_network_address_info"}
        response = self.session.get(self.prometheus_addr + "/api/v1/query", params=query).json()
        if response["status"] == "success":
            for metric in response["data"]["result"]:
                self.dev_map[metric["metric"]["address"]] = metric["metric"]["device"]
    
    @staticmethod
    def query_builder(metric, time) -> str:
        query_dict = dict()
        query_dict["query"] = metric
        query_dict["time"] = time 
        return query_dict

    @staticmethod 
    def get_val_from_response(response):
        return response["data"]["result"][0]["value"][1]

    # takes IPv6 of source and transfer_start_time as input and returns the total number of bytes transferred
    def get_total_data_transferred(self, source_ipv6, site_name, start_time, end_time=time.time()) -> float:
        if self.dev_map[source_ipv6] is None:
            self.get_dev_map()
        if self.dev_map[source_ipv6] is None:
            raise Exception("IPv6 DNE")
        at_start, at_end = 0, 0
        start_query = self.query_builder(
            metric="node_network_transmit_bytes_total{device=\"%s\",job=\"%s\"}" % (self.dev_map[source_ipv6], site_name),
            time=start_time)
        start_response = self.session.get(self.prometheus_addr + "/api/v1/query", params=start_query).json()
        if start_response["status"] == "success":
            at_start = self.get_val_from_response(start_response)
        end_query = self.query_builder(
            metric="node_network_transmit_bytes_total{device=\"%s\",job=\"%s\"}" % (self.dev_map[source_ipv6], site_name),
            time=end_time)
        end_response = self.session.get(self.prometheus_addr + "/api/v1/query", params=end_query).json()
        if end_response["status"] == "success":
            at_end = self.get_val_from_response(end_response) 
        return (float(at_end) - float(at_start))

    def get_average_throughput(self, **kwargs) -> float:
        return self.get_total_data_transferred(**kwargs) / (kwargs["end_time"] - kwargs["start_time"])  
