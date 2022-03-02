import json
"""
Name-Only Nonfunctional Software defined networking (SDN) for End-to-end Networked 
Science at the Exascale 
"""

DUMMY_LINK = "127.0.0.1"

def get_ipv6_pool(uri):
    """
    GET /discover/URI/ipv6pool
    """
    with open("templates/sense_discover_ipv6pool.json", "r") as f_in:
        template = json.load(f_in)
    return template["ipv6_subnet_pool"].split(",")

def get_uplink_capacity(uri):
    """
    GET /discover/URI/peers
    """
    with open("templates/sense_discover_peers.json", "r") as f_in:
        template = json.load(f_in)
    return float(template["port_capacity"])

def get_uri(rse_name):
    """
    GET /discover/lookup/RSE_NAME
    """
    with open("templates/sense_discover_lookup.json", "r") as f_in:
        template = json.load(f_in)
    return __get_rooturi(template["results"][0]["resource"])

def __get_rooturi(full_uri):
    """
    GET /discover/lookup/FULL_URL/rooturi
    """
    return "urn:ogf:network:ultralight.org:2013"

def get_theoretical_bandwidth(*args, **kwargs):
    return

def build_link(*args, **kwargs):
    return

def reprovision_link(*args, **kwargs):
    return

def delete_link(*args, **kwargs):
    return
