import argparse
import sys
import yaml
import signal
import logging
from multiprocessing.connection import Listener
from dmm.site import Site
from dmm.link import Link
from dmm.request import Request
from dmm.orchestrator import Orchestrator

class DMM:
    def __init__(self, n_workers=4):
        self.orchestrator = Orchestrator(n_workers=n_workers)
        self.sites = {}
        self.requests = {}
        with open("config.yaml", "r") as f_in:
            dmm_config = yaml.safe_load(f_in).get("dmm")
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
        self.orchestrator.stop()
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
                elif daemon.upper() == "SUBMITTER":
                    result = self.submitter_handler(payload)
                    connection.send(result)
                elif daemon.upper() == "FINISHER":
                    self.finisher_handler(payload)

    @staticmethod
    def link_updater(link, new_bandwidth, msg, monitoring):
        logging.debug(f"{link} | {link.bandwidth} --> {new_bandwidth}; {msg}")
        if link.is_open:
            link.reprovision(new_bandwidth)
        else:
            link.bandwidth = new_bandwidth
            link.open()

        link.update_history(msg, monitoring=monitoring)

    @staticmethod
    def link_closer(link, request_id, monitoring):
        logging.debug(f"{link} | closing link")
        link.close()
        link.update_history("closing link", monitoring=monitoring)
        # Log the promised and actual bandwidths
        summary = link.get_summary(string=True, monitoring=monitoring)
        logging.info(f"({request_id} FINISHED) {summary}")

    def update_links(self, msg):
        """Update bandwidth provisions for all links

        Note: Link.reprovision only contacts sense if the new provision is different from 
              its current bandwidth provision
        """
        logging.info("updating link bandwidth provisions and metadata")
        for request, link in self.requests.values():
            new_bandwidth = link.get_max_bandwidth()*request.get_bandwidth_fraction()
            # Submit SENSE query
            link_updater_args = (
                link,
                new_bandwidth,
                msg if link.is_open else "opened link",
                self.monitoring
            )
            self.orchestrator.put(str(link), DMM.link_updater, link_updater_args)

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

        self.update_links("accommodating for new requests")

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
                    link.deregister()
                    closer_args = (link, request_id, self.monitoring)
                    self.orchestrator.put(str(link), DMM.link_closer, closer_args)
                    # Deregister the request
                    request.deregister()
                    # Clean up
                    self.requests.pop(request_id)

        if len(link_close_jobs) > 0:
            self.update_links("adjusting for request deletion")

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
        format="(%(threadName)s) [%(asctime)s] %(levelname)s: %(message)s",
        datefmt="%m-%d-%Y %H:%M:%S %p",
        level=getattr(logging, args.loglevel.upper()),
        handlers=handlers
    )

    logging.info("Starting DMM")
    dmm = DMM(n_workers=args.n_workers)
    signal.signal(signal.SIGINT, sigint_handler(dmm))
    dmm.start()
