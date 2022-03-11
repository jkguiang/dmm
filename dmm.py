import argparse
import time
import sys
import yaml
import signal
import logging
from multiprocessing.connection import Listener
import nonsense_api as sense_api
import monitoring

class Site:
    def __init__(self, rse_name):
        self.rse_name = rse_name
        self.sense_name = sense_api.get_uri(rse_name)
        self.free_ipv6_pool = sense_api.get_ipv6_pool(self.sense_name)
        self.used_ipv6_pool = []
        self.default_ipv6 = self.free_ipv6_pool.pop(0) # reserve an ipv6 for best effort service
        self.total_uplink_capacity = sense_api.get_uplink_capacity(self.sense_name)
        self.prio_sums = {}
        self.all_prios_sum = 0
        # Note: the mapping below is a temporary hack; should not be needed in the future
        self.block_to_ipv6 = {}
        with open("config.yaml", "r") as f_in:
            config = yaml.safe_load(f_in)
            for ipv6_info in config["sites"][rse_name]["ipv6_pool"]:
                self.block_to_ipv6[ipv6_info["block"]] = ipv6_info["ipv6"]
                
    def add_request(self, partner_name, priority):
        self.all_prios_sum += priority
        if partner_name in self.prio_sums.keys():
            self.prio_sums[partner_name] += priority
        else:
            self.prio_sums[partner_name] = priority

    def remove_request(self, partner_name, priority):
        self.all_prios_sum -= priority
        self.prio_sums[partner_name] -= priority
        if self.prio_sums[partner_name] == 0:
            self.prio_sums.pop(partner_name)

    def get_uplink_provision(self, partner_name):
        uplink_fraction = self.prio_sums.get(partner_name, 0)/self.all_prios_sum
        return self.total_uplink_capacity*uplink_fraction

    def update_uplink_capacity(self):
        self.total_uplink_capacity = sense_api.get_uplink_capacity(self.sense_name)

    def reserve_ipv6(self):
        ipv6 = self.free_ipv6_pool.pop(0)
        self.used_ipv6_pool.append(ipv6)
        return ipv6

    def free_ipv6(self, ipv6):
        self.used_ipv6_pool.remove(ipv6)
        self.free_ipv6_pool.append(ipv6)

class Link:
    def __init__(self, src_site, dst_site, bandwidth=0, best_effort=False):
        self.src_site = src_site
        self.dst_site = dst_site
        self.best_effort = best_effort
        self.is_open = False
        self.src_ipv6 = ""
        self.dst_ipv6 = ""
        self.bandwidth = bandwidth
        self.history = [(time.time(), bandwidth, 0, "init")]
        self.sense_link_id, self.theoretical_bandwidth = sense_api.get_theoretical_bandwidth(
            self.src_site.sense_name,
            self.dst_site.sense_name
        )
        self.prometheus = monitoring.PrometheusSession()

    def update_history(self, msg):
        time_last, _, _, _ = self.history[-1]
        time_now = time.time()
        actual_bandwidth = self.prometheus.get_average_throughput(
            self.src_ipv6,
            self.src_site.rse_name,
            time_last,
            time_now
        )
        self.history.append((time_now, self.bandwidth, actual_bandwidth, msg))

    def update_theoretical_bandwidth(self):
        _, self.theoretical_bandwidth = sense_api.get_theoretical_bandwidth(
            self.src_site.sense_name,
            self.dst_site.sense_name,
            instance_uuid=self.sense_link_id
        )

    def get_max_bandwidth(self):
        return min(
            self.src_site.get_uplink_provision(self.dst_site.rse_name),
            self.dst_site.get_uplink_provision(self.src_site.rse_name),
            self.theoretical_bandwidth
        )

    def get_summary(self, string=False):
        times, promised_bw, actual_bw, _ = zip(*self.history)
        dts = [t - times[t_i] for t_i, t in enumerate(times[1:])]
        avg_promise = sum([bw*dt for bw, dt in zip(promised_bw, dts)])/sum(dts)
        avg_actual = self.prometheus.get_average_throughput(
            self.src_ipv6,
            self.src_site.rse_name,
            times[1], # times[1] is when the link is actually provisioned
            times[-1]
        )
        if string:
            return f"{avg_promise}, {avg_actual} (promised, actual bandwidth [Mb/s])"
        else:
            return avg_promise, avg_actual

    def reprovision(self, new_bandwidth, msg):
        if new_bandwidth != self.bandwidth:
            self.bandwidth = new_bandwidth
            # Update SENSE link; note: in the future, this should not change the link ID
            self.sense_link_id = sense_api.reprovision_link(
                self.sense_link_id, 
                self.src_site.sense_name,
                self.dst_site.sense_name,
                self.src_ipv6,
                self.dst_ipv6,
                new_bandwidth
            )
            self.update_history(msg)

    def open(self):
        if self.best_effort:
            self.src_ipv6 = self.src_site.default_ipv6
            self.dst_ipv6 = self.dst_site.default_ipv6
        else:
            self.src_ipv6 = self.src_site.reserve_ipv6()
            self.dst_ipv6 = self.dst_site.reserve_ipv6()
            sense_api.create_link(
                self.src_site.sense_name,
                self.dst_site.sense_name,
                self.src_ipv6,
                self.dst_ipv6,
                self.bandwidth,
                instance_uuid=self.sense_link_id
            )
        self.is_open = True
        self.update_history("opened link")

    def close(self):
        if not self.best_effort:
            sense_api.delete_link(self.sense_link_id)
            self.src_site.free_ipv6(self.src_ipv6)
            self.dst_site.free_ipv6(self.dst_ipv6)
        self.sense_link_id = ""
        self.src_ipv6 = ""
        self.dst_ipv6 = ""
        self.is_open = False
        self.update_history("closed link")

class Request:
    def __init__(self, request_id, rule_id, src_site, dst_site, transfer_ids, priority, 
                 n_bytes_total, n_transfers_total):
        self.request_id = request_id
        self.rule_id = rule_id
        self.src_site = src_site
        self.dst_site = dst_site
        # Unpacked from prepared_request["attr"]
        self.transfer_ids = transfer_ids
        self.priority = priority
        self.n_bytes_total = n_bytes_total
        self.n_bytes_transferred = 0
        self.n_transfers_total = n_transfers_total
        self.n_transfers_submitted = 0
        self.n_transfers_finished = 0

    def __str__(self):
        return f"Request({self.request_id})"

    def get_bandwidth_fraction(self):
        """Return bandwidth fraction

                                     my priority
        fraction = ----------------------------------------------
                   sum(all priorities between my source and dest)
        """
        return self.priority/self.src_site.prio_sums.get(self.dst_site.rse_name)

    def same_site_pair(self, other_request):
        """Return if another request involves the same sites as this one"""
        same_src = self.src_site == other_request.src_site
        same_dst = self.dst_site == other_request.dst_site
        inverted_src = self.src_site == other_request.dst_site
        inverted_dst = self.dst_site == other_request.src_site
        return (same_src and same_dst) or (inverted_src and inverted_dst)

    def register(self):
        """Register new request at the source and destination sites"""
        self.src_site.add_request(self.dst_site.rse_name, self.priority)
        self.dst_site.add_request(self.src_site.rse_name, self.priority)

    def deregister(self):
        """Deregister new request at the source and destination sites"""
        self.src_site.remove_request(self.dst_site.rse_name, self.priority)
        self.dst_site.remove_request(self.src_site.rse_name, self.priority)

class DMM:
    def __init__(self, host, port, authkey_file, n_workers=4):
        self.host = host
        self.port = port
        with open(authkey_file, "rb") as f_in:
            self.authkey = f_in.read()
        self.n_workers = n_workers
        self.sites = {}
        self.requests = {}

    def __get_request_id(self, rule_id, src_rse_name, dst_rse_name):
        return f"{rule_id}_{src_rse_name}_{dst_rse_name}"

    def __dump(self):
        for request, link in self.requests.values():
            logging.debug(
                f"{request} | "
                f"{link.src_site.rse_name} --> {link.dst_site.rse_name} "
                f"{link.bandwidth} Mb/s"
            )

    def stop(self):
        """
        Placeholder; should eventually do the following:
          - join/close all worker processes
          - close all SENSE links(?)
        """
        return

    def start(self):
        listener = Listener((self.host, self.port), authkey=self.authkey)
        while True:
            logging.info("Waiting for the next connection")
            with listener.accept() as connection:
                client_host, client_port = listener.last_accepted
                logging.info(f"Connection accepted from {client_host}:{client_port}")
                # Process payload from new connection
                daemon, payload = connection.recv()
                if daemon.upper() == "PREPARER":
                    self.preparer_handler(payload)
                    self.__dump()
                elif daemon.upper() == "SUBMITTER":
                    result = self.submitter_handler(payload)
                    connection.send(result)
                elif daemon.upper() == "FINISHER":
                    self.finisher_handler(payload)

    def update_requests(self, msg):
        # Update bandwidth provisions for all links
        for request, link in self.requests.values():
            new_bandwidth = link.get_max_bandwidth()*request.get_bandwidth_fraction()
            if link.is_open:
                link.reprovision(new_bandwidth, msg=msg)
            else:
                link.bandwidth = new_bandwidth
                link.open()

    def preparer_handler(self, payload):
        """
        Organize data (the 'payload') from Rucio preparer daemon into Request objects,
        where each Request == (Rucio Rule ID + RSE Pair), open new links, and 
        reprovision existing links appropriately
        
        payload = {
            rule_id_1: {
                "SiteA&SiteB": {
                    "transfer_ids": [str, str, ...],
                    "priority": int,
                    "n_bytes_total": int,
                    "n_transfers_total": int
                },
                "SiteB&SiteC": { ... },
                ...
            },
            rule_id_2: { ... },
            ...
        }
        """
        for rule_id, prepared_rule in payload.items():
            for rse_pair_id, request_attr in prepared_rule.items():
                src_rse_name, dst_rse_name = rse_pair_id.split("&")
                # Check if request has already been processed
                request_id = self.__get_request_id(rule_id, src_rse_name, dst_rse_name)
                if request_id in self.requests.keys():
                    logging.error("request ID already processed--should never happen!")
                    continue
                # Retrieve or construct source Site object
                src_site = self.sites.get(src_rse_name, Site(src_rse_name))
                if src_rse_name not in self.sites.keys():
                    self.sites[src_rse_name] = src_site
                # Retrieve or construct destination Site object
                dst_site = self.sites.get(dst_rse_name, Site(dst_rse_name))
                if dst_rse_name not in self.sites.keys():
                    self.sites[dst_rse_name] = dst_site
                # Create new Request
                req = Request(request_id, rule_id, src_site, dst_site, **request_attr)
                req.register()
                # Create new Link
                link = Link(src_site, dst_site, best_effort=(req.priority == 0))
                # Store new request and its corresponding link
                self.requests[req.request_id] = (req, link)

        self.update_requests(msg="accommodating for new requests")

    def submitter_handler(self, payload):
        """
        Return the IPv6 pair (source and dest) for a the request being submitted by the 
        Rucio submitter daemon
        
        payload = {
            "rule_id": str,
            "rse_pair_id": str, # e.g. "SiteA&SiteB",
            "n_transfers_submitted": int
        }
        """
        # Unpack payload
        rule_id = payload.get("rule_id")
        src_rse_name, dst_rse_name = payload.get("rse_pair_id").split("&")
        n_transfers_submitted = payload.get("n_transfers_submitted")
        # Update request
        request_id = self.__get_request_id(rule_id, src_rse_name, dst_rse_name)
        request, link = self.requests[request_id]
        request.n_transfers_submitted += n_transfers_submitted
        # Get SENSE link endpoints
        sense_map = {
            # block_to_ipv6 translation is a hack; should not be needed in the future
            request.src_site.rse_name: link.src_site.block_to_ipv6[link.src_ipv6],
            request.dst_site.rse_name: link.dst_site.block_to_ipv6[link.dst_ipv6]
        }
        return sense_map

    def finisher_handler(self, payload):
        """
        Parse data (the 'payload') from Rucio finisher daemon, update progress of 
        every request, close the links for any that have finished, and reprovision 
        existing links if possible
        
        payload = {
            rule_id_1: {
                "SiteA&SiteB": {
                    "n_transfers_finished": int,
                    "n_bytes_transferred": int
                },
                "SiteB&SiteC": { ... },
                ...
            },
            rule_id_2: { ... },
            ...
        }
        """
        n_links_closed = 0
        for rule_id, finisher_reports in payload.items():
            for rse_pair_id, finisher_report in finisher_reports.items():
                # Get request ID
                src_rse_name, dst_rse_name = rse_pair_id.split("&")
                request_id = self.__get_request_id(rule_id, src_rse_name, dst_rse_name)
                # Update request
                request, link = self.requests[request_id]
                request.n_transfers_finished += finisher_report["n_transfers_finished"]
                request.n_bytes_transferred += finisher_report["n_bytes_transferred"]
                if request.n_transfers_finished == request.n_transfers_total:
                    # Close the link and deregister the request
                    link.close()
                    request.deregister()
                    n_links_closed += 1
                    # Log the promised and actual bandwidths
                    logging.debug(f"({request_id} FINISHED) {link.get_summary(string=True)}")
                    # Clean up
                    self.requests.pop(request_id)

        if n_links_closed > 0:
            self.update_requests(msg="adjusting for request deletion")

def sigint_handler(dmm):
    def actual_handler(sig, frame):
        logging.info("Stopping DMM (received SIGINT)")
        dmm.stop()
        sys.exit(0)
    return actual_handler

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
    cli.add_argument(
        "--loglevel", type=str, default="WARNING", 
        help="log level: DEBUG, INFO, WARNING (default), or ERROR"
    )
    cli.add_argument(
        "--logfile", type=str, default="dmm.log", 
        help="path to log file (default: ./dmm.log)"
    )
    args = cli.parse_args()

    handlers = [logging.FileHandler(filename=args.logfile)]
    if args.loglevel.upper() == "DEBUG":
        handlers.append(logging.StreamHandler(sys.stdout))

    logging.basicConfig(
        format="%(levelname)s [%(asctime)s]: %(message)s",
        datefmt="%m-%d-%Y %H:%M:%S %p",
        level=getattr(logging, args.loglevel.upper()),
        handlers=handlers
    )

    logging.info("Starting DMM")
    dmm = DMM(args.host, args.port, args.authkey, n_workers=args.n_workers)
    signal.signal(signal.SIGINT, sigint_handler(dmm))
    dmm.start()
