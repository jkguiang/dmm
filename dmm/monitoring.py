import yaml
import requests
import logging
import time

class PrometheusSession:
    """
    Get network metrics from Prometheus via its HTTP API and return aggregations of 
    those metrics
    """
    def __init__(self) -> None:
        with open("config.yaml", "r") as f_in:
            prometheus_config = yaml.safe_load(f_in).get("prometheus")
            prometheus_host = prometheus_config["host"]
            prometheus_port = prometheus_config["port"]
        self.prometheus_addr = f"http://{prometheus_host}:{prometheus_port}"
        self.session = requests.Session()
        self.dev_map = {}
        # Update dev map if prometheus address is reachable
        try:
            self.session.head(self.prometheus_addr, stream=True)
            self.update_dev_map() 
        except requests.exceptions.ConnectionError as error:
            logging.warning(f"Prometheus unreachable - {error}")

    def submit_query(self, query_dict, endpoint="api/v1/query") -> dict:
        query_addr = f"{self.prometheus_addr}/{endpoint}"
        return self.session.get(query_addr, params=query_dict).json()
        
    def update_dev_map(self) -> None:
        """Update IPv6 --> Device Name mapping"""
        response = self.submit_query({"query": "node_network_address_info"})
        if response["status"] == "success":
            for metric in response["data"]["result"]:
                self.dev_map[metric["metric"]["address"]] = metric["metric"]["device"]

    @staticmethod 
    def get_val_from_response(response):
        """Extract desired value from typical location in Prometheus response"""
        return response["data"]["result"][0]["value"][1]

    def get_total_bytes_transmitted(self, ipv6, rse_name, start_time, end_time) -> float:
        """
        Returns the total number of bytes transmitted from a given Rucio RSE via a given
        ipv6 address
        """
        if self.dev_map[ipv6] is None:
            self.update_dev_map()
        if self.dev_map[ipv6] is None:
            raise Exception("IPv6 does not exist")
        params = f"device=\"{self.dev_map[ipv6]}\",job=~\"{rse_name}.+\""
        metric = f"node_network_transmit_bytes_total{{{params}}}"
        # Get bytes transferred at the start time
        start_response = self.submit_query({"query": metric, "time": start_time})
        if start_response["status"] == "success":
            bytes_transferred_at_start = self.get_val_from_response(start_response)
        else:
            raise Exception(f"query {metric} failed")
        # Get bytes transferred at the end time
        end_response = self.submit_query({"query": metric, "time": end_time})
        if end_response["status"] == "success":
            bytes_transferred_at_end = self.get_val_from_response(end_response) 
        else:
            raise Exception(f"query {metric} failed")

        return (float(bytes_transferred_at_end) - float(bytes_transferred_at_start))

    def get_average_throughput(self, ipv6, rse_name, start_time, end_time) -> float:
        """Returns the total throughput from a given Rucio RSE via a given ipv6 address"""
        total_bytes = self.get_total_bytes_transmitted(ipv6, rse_name, start_time, end_time)
        return total_bytes/(end_time - start_time)
