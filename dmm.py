import argparse
import time
import sys
import yaml
import signal
import logging
from multiprocessing import Pool, Process
from multiprocessing.connection import Listener
import nonsense_api as sense_api
import monitoring

class Site:
    def __init__(self, rse_name):
        self.rse_name = rse_name
        self.sense_name = sense_api.get_uri(rse_name)
        self.free_ipv6_pool = sense_api.get_ipv6_pool(self.sense_name)
        self.used_ipv6_pool = []
        self.total_uplink_capacity = sense_api.get_uplink_capacity(self.sense_name)
        self.prio_sums = {}
        self.all_prios_sum = 0
        # Read site information from config.yaml; should not be needed in the future
        self.block_to_ipv6 = {}
        with open("config.yaml", "r") as f_in:
            site_config = yaml.safe_load(f_in).get("sites").get(rse_name)
            # Best effort IPv6 may be extracted from elsewhere in the future
            self.default_ipv6 = site_config.get("best_effort_ipv6")
            # The mapping below is a temporary hack; should not be needed in the future
            for ipv6_info in site_config.get("ipv6_pool", []):
                self.block_to_ipv6[ipv6_info["block"]] = ipv6_info["ipv6"]
            # Remove best-effort IPv6 block from the free pool
            for block, ipv6 in self.block_to_ipv6.items():
                if ipv6 == self.default_ipv6:
                    self.free_ipv6_pool.remove(block)
                    break
                
    def add_request(self, partner_name, priority):
        """
        Add request priority to the numerator and denominator of the uplink provisioning 
        fraction for this partner
        """
        # Add priority to uplink fraction denominator
        self.all_prios_sum += priority
        # Add priority to uplink fraction numerator
        if partner_name in self.prio_sums.keys():
            self.prio_sums[partner_name] += priority
        else:
            self.prio_sums[partner_name] = priority

    def remove_request(self, partner_name, priority):
        """
        Subtract request priority to the numerator and denominator of the uplink 
        provisioning fraction for this partner
        """
        # Subtract priority to uplink fraction denominator
        self.all_prios_sum -= priority
        # Subtract priority to uplink fraction numerator
        self.prio_sums[partner_name] -= priority
        if self.prio_sums[partner_name] == 0:
            self.prio_sums.pop(partner_name)

    def get_uplink_provision(self, partner_name):
        """
        Return uplink capacity times uplink provisioning fraction for a given partner 
        site; i.e. the fraction of the capacity provisioned for that partner

                          sum(priorities between this site and a partner site)
        uplink fraction = ----------------------------------------------------
                                           sum(all priorities)
        """
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

    def update_history(self, msg, monitoring=False):
        time_last, _, _, _ = self.history[-1]
        time_now = time.time()
        if monitoring:
            actual_bandwidth = self.prometheus.get_average_throughput(
                self.src_ipv6,
                self.src_site.rse_name,
                time_last,
                time_now
            )
        else:
            actual_bandwidth = -1

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

    def get_summary(self, string=False, monitoring=False):
        """Return the average promised and actual bandwidth"""
        times, promised_bw, actual_bw, _ = zip(*self.history)
        dts = [t - times[t_i] for t_i, t in enumerate(times[1:])]
        avg_promise = sum([bw*dt for bw, dt in zip(promised_bw, dts)])/sum(dts)
        if monitoring:
            avg_actual = self.prometheus.get_average_throughput(
                self.src_site.block_to_ipv6[self.src_ipv6], # block_to_ipv6 is a temporary hack
                self.src_site.rse_name,
                times[1], # times[1] is when the link is actually provisioned
                times[-1]
            )
        else:
            avg_actual = -1
        if string:
            return f"{avg_promise:0.1f}, {avg_actual:0.1f} (promised, actual bandwidth [Mb/s])"
        else:
            return avg_promise, avg_actual

    def register(self):
        if self.best_effort:
            self.src_ipv6 = self.src_site.default_ipv6
            self.dst_ipv6 = self.dst_site.default_ipv6
        else:
            self.src_ipv6 = self.src_site.reserve_ipv6()
            self.dst_ipv6 = self.dst_site.reserve_ipv6()

    def deregister(self):
        if not self.best_effort:
            self.src_site.free_ipv6(self.src_ipv6)
            self.dst_site.free_ipv6(self.dst_ipv6)
        self.sense_link_id = ""
        self.src_ipv6 = ""
        self.dst_ipv6 = ""

    def reprovision(self, new_bandwidth):
        """Reprovision SENSE link
        
        Note: this is run asynchronously, so self.bandwidth must be modified externally
        """
        if new_bandwidth != self.bandwidth:
            # Update SENSE link; note: in the future, this should not change the link ID
            self.sense_link_id = sense_api.reprovision_link(
                self.sense_link_id, 
                self.src_site.sense_name,
                self.dst_site.sense_name,
                self.src_ipv6,
                self.dst_ipv6,
                new_bandwidth
            )

    def open(self):
        sense_api.create_link(
            self.src_site.sense_name,
            self.dst_site.sense_name,
            self.src_ipv6,
            self.dst_ipv6,
            self.bandwidth,
            instance_uuid=self.sense_link_id
        )

    def close(self):
        if not self.best_effort:
            sense_api.delete_link(self.sense_link_id)

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
    def __init__(self, n_workers=4):
        self.pool = Pool(processes=n_workers)
        self.pool_jobs = None
        self.sites = {}
        self.requests = {}
        with open("config.yaml", "r") as f_in:
            dmm_config = yaml.safe_load(f_in).get("dmm", {})
            self.host = dmm_config.get("host", "localhost")
            self.port = dmm_config.get("port", 5000)
            authkey_file = dmm_config.get("authkey", "")
            self.monitoring = dmm_config.get("monitoring", True)
        with open(authkey_file, "rb") as f_in:
            self.authkey = f_in.read()

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
        self.pool.terminate()
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

    @staticmethod
    def link_updater(args):
        link, new_bandwidth = args
        if link.is_open:
            link.reprovision(new_bandwidth)
        else:
            link.bandwidth = new_bandwidth
            link.open()

    @staticmethod
    def link_closer(link):
        link.close()

    def update_requests(self, msg):
        """Update bandwidth provisions for all links

        Note: Link.reprovision only contacts sense if the new provision is different from 
              its current bandwidth provision
        """
        link_update_jobs = []
        for request, link in self.requests.values():
            new_bandwidth = link.get_max_bandwidth()*request.get_bandwidth_fraction()
            link_update_jobs.append((link, new_bandwidth))
        # Submit SENSE queries
        logging.info("updating link bandwidth provisions")
        if self.pool_jobs:
            self.pool_jobs.get()
        self.pool.map_async(DMM.link_updater, link_update_jobs)
        # Update link metadata
        logging.info("updating request metadata")
        for link, new_bandwidth in link_update_jobs:
            link.bandwidth = new_bandwidth
            if link.is_open:
                link.update_history(msg, monitoring=self.monitoring)
            else:
                link.update_history("opened link", monitoring=self.monitoring)
            link.is_open = True

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
                link.register()
                # Store new request and its corresponding link
                self.requests[req.request_id] = (req, link)

        self.update_requests("accommodating for new requests")

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
        link_close_jobs = {}
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
                    # Stage the link for closure
                    link_close_jobs[request_id] = link
                    # Deregister the request
                    request.deregister()
                    # Clean up
                    self.requests.pop(request_id)

        self.pool.map_async(DMM.link_closer, link_close_jobs.values())

        for request_id, link in link_close_jobs.items():
            link.deregister()
            link.update_history("closed link", monitoring=self.monitoring)
            link.is_open = False
            # Log the promised and actual bandwidths
            summary = link.get_summary(string=True, monitoring=self.monitoring)
            logging.debug(f"({request_id} FINISHED) {summary}")

        if len(link_close_jobs) > 0:
            self.update_requests("adjusting for request deletion")

def sigint_handler(dmm):
    def actual_handler(sig, frame):
        logging.info("Stopping DMM (received SIGINT)")
        dmm.stop()
        sys.exit(0)
    return actual_handler

if __name__ == "__main__":
    cli = argparse.ArgumentParser(description="Rucio-SENSE data movement manager")
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
    dmm = DMM(n_workers=args.n_workers)
    signal.signal(signal.SIGINT, sigint_handler(dmm))
    dmm.start()
