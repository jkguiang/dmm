import argparse
import multiprocessing as mp
from multiprocessing.connection import Listener

class DMM:
    def __init__(self, host, port, authkey_file, n_workers=4):
        self.host = host
        self.port = port
        with open(authkey_file, "rb") as f_in:
            self.authkey = f_in.read()
        self.pool = mp.Pool(processes=n_workers)
        self.lock = mp.Lock()
        self.cache = {}

    def start(self):
        listener = Listener((self.host, self.port), authkey=self.authkey)
        while True:
            print("waiting for the next connection")
            with listener.accept() as connection:
                print("connection accepted from", listener.last_accepted)
                daemon, payload = connection.recv()
                if daemon == "PREPARER":
                    pool.apply_async(self.preparer_handler, (payload,))
                elif daemon == "SUBMITTER":
                    # FIXME: currently, any request that expects a response breaks
                    #        parallelism, because next listener.accept() is not allowed
                    #        until connection.send(resp.get()) finishes
                    resp = pool.apply_async(self.submitter_handler, (payload,))
                    connection.send(resp.get())
                elif daemon == "FINISHER":
                    pool.apply_async(self.finisher_handler, (payload,))

    def preparer_handler(self, payload):
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
        # Update cache
        self.lock.aquire()
        self.cache.update(to_cache)
        self.lock.release()

    def submitter_handler(self, payload):
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
        # Update cache
        self.lock.aquire()
        cache[priority][rse_pair_id].update(transfer_data)
        self.lock.release()
        return transfer_data["sense_map"]

    def finisher_handler(self, payload):
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
            lock.aquire()
            if active_jobs == {}:
                cache.delete(priority)
            else:
                cache[priority] = active_jobs
            lock.release()

if __name__ == "__main__":
    cli = argparse.ArgumentParser(description="Rucio-SENSE data movement manager")
    cli.add_argument(
        "--host", type=str, default="localhost", 
        help="hostname for DMM"
    )
    cli.add_argument(
        "--port", type=int, default=5000, 
        help="port for DMM to listen to"
    )
    cli.add_argument(
        "--authkey", type=str, default="dummykey", 
        help="path to file with authorization key for DMM listener"
    )
    cli.add_argument(
        "-n", "--n_workers", type=int, default=4, 
        help="maximum number of worker processes"
    )
    args = cli.parse_args()

    print("Starting DMM...")
    dmm(args.host, args.port, args.authkey, n_workers=args.n_workers)
