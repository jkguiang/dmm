import json

"""
Name-Only Nonfunctional Software defined networking (SDN) for End-to-end Networked 
Science at the Exascale 
"""

PROFILE_UUID = "dummy_profile_uuid"

DUMMY_LINK = "127.0.0.1"

def get_ipv6_pool(uri):
    """
    GET /discover/URI/ipv6pool
    """
    with open(f"nonsense-o/discover/{uri}/ipv6pool", "r") as f_in:
        response = json.load(f_in)
    ipv6_pool = response["routing"][0]["ipv6_subnet_pool"].split(",")
    ipv6_pool = [DUMMY_LINK for ipv6 in ipv6_pool]
    return ipv6_pool

def get_uplink_capacity(uri):
    """
    GET /discover/URI/peers
    """
    with open(f"nonsense-o/discover/{uri}/peers", "r") as f_in:
        response = json.load(f_in)
    return float(response["peer_points"][0]["port_capacity"])

def get_uri(rse_name):
    """
    GET /discover/lookup/RSE_NAME
    """
    with open(f"nonsense-o/discover/lookup/{rse_name}", "r") as f_in:
        response = json.load(f_in)
    return __get_rooturi(response["results"][0]["resource"])

def __get_rooturi(full_uri):
    """
    GET /discover/lookup/FULL_URI/rooturi
    """
    with open(f"nonsense-o/discover/lookup/{full_uri}/rooturi", "r") as f_in:
        response = f_in.read()
        if "\n" in response:
            response = response[:-1]
    return response

def get_theoretical_bandwidth(profile_uuid):
    return 10**15

def build_link(profile_uuid, src_uri, dst_uri, src_ipv6, dst_ipv6, bandwidth):
    return "dummy_instance_uuid"

def reprovision_link(instance_uuid, new_bandwidth):
    return

def delete_link(instance_uuid):
    return
