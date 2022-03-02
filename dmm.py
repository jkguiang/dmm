import nonsense as sense
import argparse
from multiprocessing.connection import Listener

class Site:
    def __init__(self, rse_name):
        self.rse_name = rse_name
        self.sense_name = sense.get_uri(rse_name)
        self.free_ipv6_pool = sense.get_ipv6_pool(self.sense_name)
        self.used_ipv6_pool = []
        self.default_ipv6 = self.ipv6_pool.pop(0) # reserve an ipv6 for best effort service
        self.total_uplink_capacity = sense.get_uplink_capacity(self.sense_name)
        self.partners = set()

    def get_uplink_provision(self):
        return self.total_uplink_capacity/(2*len(self.partners))

    def update(self):
        self.total_uplink_capacity = sense.get_uplink_capacity(self.sense_name)

    def reserve_ipv6(self):
        ipv6 = free_ipv6_pool.pop(0)
        used_ipv6_pool.append(ipv6)
        return ipv6

    def free_ipv6(self, ipv6):
        used_ipv6_pool.remove(ipv6)
        free_ipv6_pool.append(ipv6)

class Link:
    def __init__(self, src_site, dst_site, bandwidth, best_effort=False):
        self.src_site = src_site
        self.dst_site = dst_site
        self.best_effort = best_effort
        self.is_open = False
        self.src_ipv6 = ""
        self.dst_ipv6 = ""
        self.sense_link_id = "" # SENSE service instance UUID
        self.bandwidth = bandwidth
        self.max_bandwidth = sense.get_theoretical_bandwidth(
            self.src_site.sense_name,
            self.dst_site.sense_name,
            self.src_ipv6,
            self.dst_ipv6,
        )
        self.logs = [(time.time(), bandwidth, None, "init")]

    def update(self, new_bandwidth, msg=""):
        # Update logs
        actual_bandwidth = -1 # FIXME: add this
        self.logs.append((time, new_bandwidth, actual_bandwidth, msg))
        self.bandwidth = new_bandwidth
        # Update SENSE link
        sense.reprovision_link(self.sense_link_id, new_bandwidth)

    def open(self):
        if self.best_effort:
            self.src_ipv6 = src_site.default_ipv6()
            self.dst_ipv6 = dst_site.default_ipv6()
        else:
            self.src_ipv6 = src_site.reserve_ipv6()
            self.dst_ipv6 = dst_site.reserve_ipv6()
            self.sense_link_id = sense.build_link(
                self.src_site.sense_name,
                self.dst_site.sense_name,
                self.src_ipv6,
                self.dst_ipv6,
                self.bandwidth
            )

        self.is_open = True

    def close(self):
        if not self.best_effort:
            self.src_site.free_ipv6(self.src_ipv6)
            self.dst_site.free_ipv6(self.dst_ipv6)
            sense.delete_link(self.sense_link_id)
        self.sense_link_id = ""
        self.src_ipv6 = ""
        self.dst_ipv6 = ""
        self.is_open = False

class Request:
    def __init__(self, rucio_id, src_site, dst_site, priority, n_bytes_total, 
                 n_transfers_total):
        self.rucio_id = rucio_id
        self.src_site = src_site
        self.dst_site = dst_site
        self.priority = priority
        self.n_bytes_total = n_bytes_total
        self.n_transfers_total = n_transfers_total
        self.n_transfers_submitted = 0
        self.n_transfers_finished = 0
        self.bandwidth_fraction = 0.

class DMM:
    def __init__(self, host, port, authkey_file, n_workers=4):
        self.host = host
        self.port = port
        with open(authkey_file, "rb") as f_in:
            self.authkey = f_in.read()
        self.n_workers = n_workers
        self.sites = []
        self.requests_and_links = []

    def start(self):
        listener = Listener((self.host, self.port), authkey=self.authkey)
        while True:
            print("waiting for the next connection")
            with listener.accept() as connection:
                print("connection accepted from", listener.last_accepted)
                daemon, payload = connection.recv()
                if daemon == "PREPARER":
                    self.preparer_handler(payload)
                elif daemon == "SUBMITTER":
                    result = self.submitter_handler(payload)
                    connection.send(result)
                elif daemon == "FINISHER":
                    self.finisher_handler(payload)

    def add_request_and_link(self, new_request, new_link):
        # Find requests that share the same source and destination sites
        requests_to_update = [new_request]
        for request, _ in self.requests_and_links:
            same_src = request.src_site.rse_name == new_request.src_site.rse_name
            same_dst = request.dst_site.rse_name == new_request.dst_site.rse_name
            if same_src and same_dst:
                requests_to_update.append(request)
        # Update relative priority for these requests
        priority_sum = sum([req.priority for req in requests])
        for request in requests_to_update:
            request.bandwidth_fraction = request.priority/priority_sum
        # Update bandwidth provisions for all links
        self.requests_and_links.append((new_request, new_link))
        for i, (_, link) in self.requests_and_links:
            max_bandwidth = min([
                link.src_site.get_uplink_provision(),
                link.dst_site.get_uplink_provision(),
                link.max_bandwidth
            ])
            new_bandwidth = max_bandwidth*requests.bandwidth_fraction
            if i < len(self.requests_and_links) - 1:
                # Update the existing links
                link.update(new_bandwidth, msg="accommodating new request")
            else:
                # Open the new link
                link.bandwidth = new_bandwidth
                link.open()

    def preparer_handler(self, payload):
        rse_names = [site.rse_name for site in self.sites]
        for prepared_request in payload:
            # Construct or retrieve source Site object
            src_rse_name = prepared_request["src_rse_name"]
            if src_rse_name not in rse_names:
                src_site = Site(src_rse_name)
                self.sites.append(src_site)
            else:
                src_site = self.sites[rse_names.index(src_rse_name)]
            # Construct or retrieve destination Site object
            dst_rse_name = prepared_request["dst_rse_name"]
            if dst_rse_name not in rse_names:
                dst_site = Site(dst_rse_name)
                self.sites.append(dst_site)
            else:
                dst_site = self.sites[rse_names.index(dst_rse_name)]
            # Update partners set for each site
            src_site.partners.add(dst_site)
            dst_site.partners.add(src_site)
            # Create new Request and Link objects
            request = Request(src_site, dst_site, **prepared_request["info"])
            link = Link(src_site, dst_site, 0, best_effort=(request.priority > 0))
            # Compute new bandwidth provisions and open link
            self.add_request_and_link(request, link)
        

    def submitter_handler(self, payload):
        priority = payload.get("priority")
        rse_pair_id = payload.get("rse_pair_id")
        submitted_transfers = payload.get("submitted_transfers")
        # Fetch transfer metadata
        transfer_data = self.cache[priority][rse_pair_id]
        # Update counters
        transfer_data["waiting_transfers"] -= submitted_transfers
        transfer_data["active_transfers"] += submitted_transfers
        # Get dummy SENSE links
        sense.allocate_links(transfer_data)
        src_ipv6, dst_ipv6 = sense.get_route_endpoints(priority, rse_pair_id)
        transfer_data["sense_map"] = {
            transfer_data["source_rse_id"]: src_ipv6,
            transfer_data["dest_rse_id"]: dst_ipv6
        }
        # Update cache
        self.cache[priority][rse_pair_id].update(transfer_data)
        return transfer_data["sense_map"]

    def finisher_handler(self, payload):
        for priority, updated_jobs in payload.items():
            active_jobs = self.cache[priority]
            for rse_pair_id, updated_data in updated_jobs.items():
                transfer_data = active_jobs[rse_pair_id]
                transfer_data["transferred_bytes"] += updated_data["transferred_bytes"]
                transfer_data["active_transfers"] -= updated_data["finished_transfers"]
                transfer_data["finished_transfers"] += updated_data["finished_transfers"]
                if transfer_data["finished_transfers"] == transfer_data["total_transfers"]:
                    sense.free_links(priority, rse_pair_id)
                    active_jobs.pop(rse_pair_id)
            if active_jobs == {}:
                self.cache.pop(priority)
            else:
                self.cache[priority] = active_jobs

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
    dmm = DMM(args.host, args.port, args.authkey, n_workers=args.n_workers)
    dmm.start()
