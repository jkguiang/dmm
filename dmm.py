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
        self.theoretical_bandwidth = sense.get_theoretical_bandwidth(
            self.src_site.sense_name,
            self.dst_site.sense_name,
            self.src_ipv6,
            self.dst_ipv6,
        )
        self.logs = [(time.time(), bandwidth, None, "init")]

    def get_max_bandwidth(self):
        return min(
            self.src_site.get_uplink_provision(),
            self.dst_site.get_uplink_provision(),
            self.theoretical_bandwidth
        )

    def update(self, new_bandwidth, msg=""):
        if new_bandwidth != self.bandwidth:
            # Update logs
            actual_bandwidth = -1 # FIXME: add this
            self.logs.append((time.time(), new_bandwidth, actual_bandwidth, msg))
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
    def __init__(self, src_site, dst_site, rucio_rule_id, rucio_request_id, priority, 
                 n_bytes_total, n_transfers_total):
        self.src_site = src_site
        self.dst_site = dst_site
        # Unpacked from prepared_rule["attr"]
        self.rucio_rule_id = rucio_rule_id
        self.rucio_request_id = rucio_request_id
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
        self.sites = {}
        self.rules = {}

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

    def process_rule(self, new_request, new_link):
        # Find requests that share the same source and destination sites
        requests_to_update = [new_request]
        for rule_id, (request, link) in self.rules:
            same_src = request.src_site.rse_name == new_request.src_site.rse_name
            same_dst = request.dst_site.rse_name == new_request.dst_site.rse_name
            if same_src and same_dst:
                requests_to_update.append(request)
        # Update relative priority for these requests
        priority_sum = sum([req.priority for req in requests])
        for request in requests_to_update:
            request.bandwidth_fraction = request.priority/priority_sum
        # Update bandwidth provisions for existing links
        for rule_id, (request, link) in self.rules:
            new_bandwidth = link.get_max_bandwidth()*request.bandwidth_fraction
            link.update(new_bandwidth, msg="accommodating new request")
        # Open new link
        new_link.bandwidth = new_link.get_max_bandwidth()*new_request.bandwidth_fraction
        new_link.open()
        # Keep track of new rule
        self.rules[new_request.rucio_rule_id] = (new_request, new_link)

    def preparer_handler(self, payload):
        for rule_id, prepared_rule in payload.items():
            if rule_id in self.rules.keys():
                continue
            # Construct or retrieve source Site object
            src_rse_name = prepared_rule["src_rse_name"]
            if src_rse_name not in self.sites.keys():
                src_site = Site(src_rse_name)
                self.sites[src_rse_name] = src_site
            else:
                src_site = self.sites[src_rse_name]
            # Construct or retrieve destination Site object
            dst_rse_name = prepared_rule["dst_rse_name"]
            if dst_rse_name not in self.sites.keys():
                dst_site = Site(dst_rse_name)
                self.sites[dst_rse_name] = dst_site
            else:
                dst_site = self.sites[dst_rse_name]
            # Update partners set for each site
            src_site.partners.add(dst_site)
            dst_site.partners.add(src_site)
            # Create new Request and Link objects
            request = Request(src_site, dst_site, **prepared_rule["attr"])
            link = Link(src_site, dst_site, 0, best_effort=(request.priority > 0))
            # Compute new bandwidth provisions and open link
            self.process_rule(request, link)

    def submitter_handler(self, payload):
        rucio_rule_id = payload.get("rucio_rule_id")
        n_transfers_submitted = payload.get("n_transfers_submitted")
        # Fetch transfer metadata
        request, link = self.rules[rucio_rule_id]
        # Update counters
        request.n_transfers_submitted += submitted_transfers
        # Get SENSE link endpoints
        sense_map = {
            link.src_site.rse_name: link.src_ipv6,
            link.dst_site.rse_name: link.dst_ipv6
        }
        return sense_map

    def finisher_handler(self, payload):
        for rule_id, finisher_report in payload.items():
            request, link = self.rules[rule_id]
            request.n_transfers_finished += finisher_report["n_transfers_finished"]
            request.n_bytes_transferred += finisher_report["n_bytes_transferred"]
            if request.n_transfers_finished == request.n_transfers_total:
                link.close()

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
