import json
from sense.client.workflow_combined_api import WorkflowCombinedApi
from sense.client.profile_api import ProfileApi
from sense.client.discover_api import DiscoverApi

PROFILE_UUID = "ddd1dec0-83ab-4d08-bca6-9a83334cd6db"

def get_ipv6_pool(uri):
    """
    GET /discover/URI/ipv6pool
    """
    discover_api = DiscoverApi()
    response = discover_api.discover_domain_id_ipv6pool_get(uri)
    if len(response) == 0 or "ERROR" in response:
        raise ValueError(f"Discover query failed for {uri}")
    ipv6_pool = response["routing"][0]["ipv6_subnet_pool"].split(",")
    ipv6_pool = ["127.0.0.1" for ipv6 in ipv6_pool] # FIXME: delete this when testbed exists
    return ipv6_pool

def get_uplink_capacity(uri):
    """
    GET /discover/URI/peers
    """
    discover_api = DiscoverApi()
    response = discover_api.discover_domain_id_peers_get(uri)
    if len(response) == 0 or "ERROR" in response:
        raise ValueError(f"Discover query failed for {uri}")
    return float(response["peer_points"][0]["port_capacity"])

def get_uri(rse_name):
    """
    GET /discover/lookup/RSE_NAME
    """
    discover_api = DiscoverApi()
    response = discover_api.discover_lookup_name_get(rse_name)
    if len(response) == 0 or "ERROR" in response:
        raise ValueError(f"Discover query failed for {rse_name}")
    return __get_rooturi(response["results"][0]["resource"])

def __get_rooturi(full_uri):
    """
    GET /discover/lookup/FULL_URI/rooturi
    """
    discover_api = DiscoverApi()
    uri = discover_api.discover_lookup_rooturi_get(full_uri)
    if len(uri) == 0 or "ERROR" in uri:
        raise ValueError(f"Discover query failed for {full_uri}")
    return uri

def get_theoretical_bandwidth(src_uri, dst_uri):
    workflow_api = WorkflowCombinedApi()
    workflow_api.instance_new()
    # Create service instance
    intent = {
        "service_profile_uuid": PROFILE_UUID,
        "queries": [
            {
                "ask": "edit", 
                "options": [
                    {"data.connections[0].terminals[0].uri": src_uri},
                    {"data.connections[0].terminals[1].uri": dst_uri},
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
    return int(response["results"][0]["bandwidth"])

def build_link(src_uri, dst_uri, src_ipv6, dst_ipv6, bandwidth):
    workflow_api = WorkflowCombinedApi()
    workflow_api.instance_new()
    # Create service instance
    intent = {
        "service_profile_uuid": PROFILE_UUID,
        "queries": [
            {
                "ask": "edit", 
                "options": [
                    # TODO: add IPv6 assignment
                    {"data.connections[0].bandwidth.qos_class": "guaranteedCapped"},
                    {"data.connections[0].bandwidth.capacity": str(bandwidth)},
                    {"data.connections[0].terminals[0].uri": src_uri},
                    {"data.connections[0].terminals[0].assign_ip": "true"},
                    {"data.connections[0].terminals[1].uri": dst_uri},
                    {"data.connections[0].terminals[1].assign_ip": "true"},
                ]
            }
        ]
    }
    # Create new service instance
    si_uuid = workflow_api.instance_create(json.dumps(intent))
    # Provision bandwidth
    workflow_api.instance_operate("provision", sync="true")
    status = workflow_api.instance_get_status()
    return si_uuid, status

def reprovision_link(instance_uuid, new_bandwidth):
    workflow_api = WorkflowCombinedApi()
    # TODO: add actual edit/reprovisioning of bandwidth
    status = workflow_api.instance_get_status(si_uuid=instance_uuid)
    if "error" in status:
        raise ValueError(status)
    if "CANCEL" not in status:
        raise ValueError(f"cannot reprovision an instance in status '{status}'")
    elif "READY" not in status:
        workflow_api.instance_operate(
            "reprovision", 
            si_uuid=instance_uuid, 
            sync="true", 
            force="true"
        )
    else:     
        workflow_api.instance_operate("reprovision", si_uuid=instance_uuid, sync="true")

    return workflow_api.instance_get_status(si_uuid=instance_uuid)

def delete_link(instance_uuid):
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
        print(f"cancel operation disrupted - instance not deleted - contact admin")

    return workflow_api.instance_get_status(si_uuid=instance_uuid)
