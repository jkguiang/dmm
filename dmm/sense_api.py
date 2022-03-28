import json
import yaml
from sense.client.workflow_combined_api import WorkflowCombinedApi
from sense.client.profile_api import ProfileApi
from sense.client.discover_api import DiscoverApi

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
    discover_api = DiscoverApi()
    response = discover_api.discover_domain_id_ipv6pool_get(uri)
    if len(response) == 0 or "ERROR" in response:
        raise ValueError(f"Discover query failed for {uri}")
    else:
        response = json.loads(response)
        return response["routing"][0]["ipv6_subnet_pool"].split(",")

def get_uplink_capacity(uri):
    """Return the maximum uplink capacity in Mb/s for a given site

    Notes: not fully supported by SENSE yet
    """
    discover_api = DiscoverApi()
    response = discover_api.discover_domain_id_peers_get(uri)
    if not good_response(response):
        raise ValueError(f"Discover query failed for {uri}")
    else:
        response = json.loads(response)
        return float(response["peer_points"][0]["port_capacity"])

def get_uri(rse_name, full=True):
    """Return the SENSE URI for a given Rucio RSE"""
    discover_api = DiscoverApi()
    response = discover_api.discover_lookup_name_get(rse_name)
    if not good_response(response):
        raise ValueError(f"Discover query failed for {rse_name}")
    else:
        response = json.loads(response)
        full_uri = response["results"][0]["resource"]
        if full:
            return full_uri
        else:
            return __get_rooturi(full_uri)

def __get_rooturi(full_uri):
    """Return the root SENSE URI for a given full SENSE URI"""
    discover_api = DiscoverApi()
    uri = discover_api.discover_lookup_rooturi_get(full_uri)
    if not good_response(uri):
        raise ValueError(f"Discover query failed for {full_uri}")
    else:
        return uri

def get_theoretical_bandwidth(src_uri, dst_uri, instance_uuid=PROFILE_UUID):
    """Return the maximum theoretical bandwidth available between two sites

    Note: not fully supported by SENSE yet
    """
    workflow_api = WorkflowCombinedApi()
    workflow_api.instance_new()
    # Create service instance
    intent = {
        "service_profile_uuid": instance_uuid,
        "queries": [
            {
                "ask": "edit", 
                "options": [
                    {"data.connections[0].terminals[0].uri": src_uri},
                    {"data.connections[0].terminals[1].uri": dst_uri}
                ]
            },
            {
                "ask": "maximum-bandwidth", 
                "options": [{"name": "Connection 1"}]
            }
        ]
    }
    # Query SENSE and extract theoretical bandwidth from its response
    response = workflow_api.instance_create(json.dumps(intent))
    print(instance_uuid)
    print(response)
    if not good_response(response):
        raise ValueError(f"SENSE query failed for {PROFILE_UUID}")
    else:
        response = json.loads(response)
        for query in response["queries"]:
            if query["asked"] == "maximum-bandwidth":
                return response["service_uuid"], int(query["results"][0]["bandwidth"])

def create_link(src_uri, dst_uri, src_ipv6, dst_ipv6, bandwidth, 
                instance_uuid=PROFILE_UUID, alias=""):
    """Create a SENSE guaranteed-bandwidth link between two sites"""
    workflow_api = WorkflowCombinedApi()
    workflow_api.instance_new()
    # Create service instance
    intent = {
        "service_profile_uuid": instance_uuid,
        "queries": [
            {
                "ask": "edit", 
                "options": [
                    # Bandwidth (QOS == Quality of Service)
                    {"data.connections[0].bandwidth.qos_class": "guaranteedCapped"},
                    {"data.connections[0].bandwidth.capacity": str(bandwidth)},
                    # Source
                    {"data.connections[0].terminals[0].uri": src_uri},
                    {"data.connections[0].terminals[0].assign_ip": "true"},
                    {"data.connections[0].terminals[0].ipv6_prefix_list": src_ipv6},
                    # Destination
                    {"data.connections[0].terminals[1].uri": dst_uri},
                    {"data.connections[0].terminals[1].assign_ip": "true"},
                    {"data.connections[0].terminals[1].ipv6_prefix_list": dst_ipv6},
                ]
            }
        ]
    }
    if alias:
        intent["alias"] = alias
    # Create new service instance
    response = workflow_api.instance_create(json.dumps(intent))
    if not good_response(response):
        raise ValueError(f"SENSE query failed for {PROFILE_UUID}")
    else:
        response = json.loads(response)
        # Provision bandwidth
        workflow_api.instance_operate("provision", sync="true")

def delete_link(instance_uuid):
    """Delete a SENSE link"""
    workflow_api = WorkflowCombinedApi()
    status = workflow_api.instance_get_status(si_uuid=instance_uuid)
    if "error" in status:
        raise ValueError(status)
    if "CREATE" not in status and "REINSTATE" not in status and "MODIFY" not in status:
        raise ValueError(f"cannot cancel an instance in status '{status}'")
    workflow_api.instance_operate(
        "cancel", 
        si_uuid=instance_uuid, 
        sync="true", 
        force=str("READY" not in status).lower()
    )
    status = workflow_api.instance_get_status(si_uuid=instance_uuid)
    if "CANCEL - READY" in status:
        workflow_api.instance_delete(si_uuid=instance_uuid)
    else:
        raise Exception(f"cancel operation disrupted; instance not deleted")

def reprovision_link(old_instance_uuid, src_uri, dst_uri, src_ipv6, dst_ipv6, 
                     new_bandwidth, alias=""):
    """Reprovision a SENSE link

    Note: this currently deletes the existing link, then creates a copy of the old link
          with the new bandwidth provision; this is NOT how it will ultimately be done 
          in production, but an actual reprovisioning is not currently supported
    """
    # Extract necessary information from old link
    profile_api = ProfileApi()
    response = profile_api.profile_describe(old_instance_uuid)
    if not good_response(response):
        raise ValueError(f"SENSE query failed for {PROFILE_UUID}")
    else:
        # Delete old link
        delete_link(old_instance_uuid)
        # Create new link with new bandwidth
        new_instance_uuid = create_link(
            src_uri, dst_uri, 
            src_ipv6, dst_ipv6, 
            new_bandwidth,
            alias=""
        )
        return new_instance_uuid