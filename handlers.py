import multiprocessing as mp
import time
import nonsense

def preparer_handler(payload, cache):
    to_cache = {}
    for priority, prepared_jobs in payload.items():
        to_cache[priority] = {}
        for rse_pair_id, transfer_data in prepared_jobs.items():
            additional_transfer_data = {
                "transferred_bytes": 0,
                "waiting_transfers": transfer_data["total_transfers"],
                "active_transfers": 0,
                "finished_transfers": 0
            }
            transfer_data.update(additional_transfer_data)
            to_cache[priority][rse_pair_id] = transfer_data

    cache.update(to_cache)
    return

def submitter_handler(payload, cache):
    priority = payload.get("priority")
    rse_pair_id = payload.get("rse_pair_id")
    submitted_transfers = payload.get("submitted_transfers")
    # Fetch transfer metadata
    transfer_data = cache[priority][rse_pair_id]
    # Update counters
    transfer_data["waiting_transfers"] -= submitted_transfers
    transfer_data["active_transfers"] += submitted_transfers
    # Get dummy SENSE links
    nonsense.allocate_links(transfer_data)
    src_link, dst_link = nonsense.get_links(priority, rse_pair_id)
    transfer_data["sense_map"] = {
        transfer_data["source_rse_id"]: src_link,
        transfer_data["dest_rse_id"]: dst_link,
    }
    cache[priority][rse_pair_id].update(transfer_data)
    return transfer_data["sense_map"]

def finisher_handler(payload, cache):
    for priority, updated_jobs in payload.items():
        active_jobs = cache[priority]
        for rse_pair_id, updated_data in updated_jobs.items():
            transfer_data = active_jobs[rse_pair_id]
            transfer_data["transferred_bytes"] += updated_data["transferred_bytes"]
            transfer_data["active_transfers"] -= updated_data["finished_transfers"]
            transfer_data["finished_transfers"] += updated_data["finished_transfers"]
            if transfer_data["finished_transfers"] == transfer_data["total_transfers"]:
                nonsense.free_links(priority, rse_pair_id)
                active_jobs.pop(rse_pair_id)
        if active_jobs == {}:
            cache.delete(priority)
        else:
            cache[priority] = active_jobs
