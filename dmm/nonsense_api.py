import json
import time
import uuid
import yaml

PROFILE_UUID = ""

def get_profile_uuid():
    global PROFILE_UUID
    if PROFILE_UUID == "":
        with open("config.yaml", "r") as f_in:
            sense_config = yaml.safe_load(f_in).get("sense")
            PROFILE_UUID = sense_config.get("profile_uuid")

    return PROFILE_UUID

def good_response(response):
    return len(response) == 0 or "ERROR" in response or "error" in response

def get_ipv6_pool(uri):
    """Return a list of IPv6 subnets at given site

    Note: not fully supported by SENSE yet
    """
    time.sleep(0.5)
    ipv6_pool = [
        "fc00::0010/124", "fc00::0020/124", "fc00::0030/124", 
        "fc00::0040/124", "fc00::0050/124"
    ]
    return ipv6_pool

def get_uplink_capacity(uri):
    """Return the maximum uplink capacity in Mb/s for a given site

    Notes: not fully supported by SENSE yet
    """
    time.sleep(1)
    return 100000

def get_uri(rse_name, full=True):
    """Return the root SENSE URI for a given Rucio RSE"""
    time.sleep(0.5)
    full_uri = f"urn:ogf:network:{rse_name.lower()}.foo:{rse_name}"
    if full:
        return full_uri
    else:
        return __get_rooturi(full_uri)

def __get_rooturi(full_uri):
    """Return the root SENSE URI for a given full SENSE URI"""
    return full_uri.split(":")[0]

def stage_link(src_uri, dst_uri, src_ipv6, dst_ipv6, instance_uuid="", alias=""):
    """Return the maximum theoretical bandwidth available between two sites

    Note: not fully supported by SENSE yet
    """
    time.sleep(1)
    return uuid.uuid4(), 100000000000

def provision_link(instance_uuid, src_uri, dst_uri, src_ipv6, dst_ipv6, bandwidth, alias=""):
    """Create a SENSE guaranteed-bandwidth link between two sites"""
    time.sleep(5)
    return

def delete_link(instance_uuid):
    """Delete a SENSE link"""
    time.sleep(5)
    return

def reprovision_link(old_instance_uuid, src_uri, dst_uri, src_ipv6, dst_ipv6, 
                     new_bandwidth, alias=""):
    """Reprovision a SENSE link"""
    time.sleep(10)
    return uuid.uuid4()
