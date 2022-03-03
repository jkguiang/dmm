import nonsense as sense
import argparse
import time
import sys
import signal
import logging
from multiprocessing.connection import Listener

class Site:
    def __init__(self, rse_name):
        self.rse_name = rse_name
        self.sense_name = sense.get_uri(rse_name)
        self.free_ipv6_pool = sense.get_ipv6_pool(self.sense_name)
        self.used_ipv6_pool = []
        self.default_ipv6 = self.free_ipv6_pool.pop(0) # reserve an ipv6 for best effort service
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
    def __init__(self, src_site, dst_site, bandwidth=0, best_effort=False):
        self.src_site = src_site
        self.dst_site = dst_site
        self.best_effort = best_effort
        self.is_open = False
        self.src_ipv6 = ""
        self.dst_ipv6 = ""
        self.sense_link_id = "" # SENSE service instance UUID
        self.bandwidth = bandwidth
        self.theoretical_bandwidth = sense.get_theoretical_bandwidth(sense.PROFILE_UUID)
        self.logs = [(time.time(), bandwidth, None, "init")]

    def get_max_bandwidth(self):
        return min(
            self.src_site.get_uplink_provision(),
            self.dst_site.get_uplink_provision(),
            self.theoretical_bandwidth
        )

    def update(self, new_bandwidth):
        if new_bandwidth != self.bandwidth:
            # Update logs
            actual_bandwidth = -1 # FIXME: add this
            self.logs.append((time.time(), new_bandwidth, actual_bandwidth, msg))
            self.bandwidth = new_bandwidth
            # Update SENSE link
            sense.reprovision_link(self.sense_link_id, new_bandwidth)

    def open(self):
        if self.best_effort:
            self.src_ipv6 = self.src_site.default_ipv6
            self.dst_ipv6 = self.dst_site.default_ipv6
        else:
            self.src_ipv6 = self.src_site.reserve_ipv6()
            self.dst_ipv6 = self.dst_site.reserve_ipv6()
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

class Rule:
    def __init__(self, rule_id, src_site, dst_site, request_ids, priority, 
                 n_bytes_total, n_transfers_total):
        self.rule_id = rule_id
        self.src_site = src_site
        self.dst_site = dst_site
        # Unpacked from prepared_rule["attr"]
        self.request_ids = request_ids
        self.priority = priority
        self.n_bytes_total = n_bytes_total
        self.n_bytes_transferred = 0
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

    def __dump(self):
        for rule_id, (rule, link) in self.rules.items():
            logging.debug(
                f"Rule({rule.rule_id}) | "
                f"{link.src_site.rse_name} --> {link.dst_site.rse_name} "
                f"{link.bandwidth} Mb/s"
            )

    def stop(self):
        # Placeholder for func that joins/closes workers
        # Should also probably close all SENSE links
        return

    def start(self):
        listener = Listener((self.host, self.port), authkey=self.authkey)
        while True:
            logging.info("Waiting for the next connection")
            with listener.accept() as connection:
                client_host, client_port = listener.last_accepted
                logging.info(f"Connection accepted from {client_host}:{client_port}")
                daemon, payload = connection.recv()
                if daemon == "PREPARER":
                    self.preparer_handler(payload)
                    self.__dump()
                elif daemon == "SUBMITTER":
                    result = self.submitter_handler(payload)
                    connection.send(result)
                elif daemon == "FINISHER":
                    self.finisher_handler(payload)

    def process_rule(self, new_rule, new_link):
        # Find rules that share the same source and destination sites
        rules_to_update = [new_rule]
        for rule_id, (rule, link) in self.rules.items():
            same_src = rule.src_site.rse_name == new_rule.src_site.rse_name
            same_dst = rule.dst_site.rse_name == new_rule.dst_site.rse_name
            if same_src and same_dst:
                rules_to_update.append(rule)
        # Update relative priority for these rules
        priority_sum = sum([rule.priority for rule in rules_to_update])
        for rule in rules_to_update:
            rule.bandwidth_fraction = rule.priority/priority_sum
        # Update bandwidth provisions for existing links
        for rule_id, (rule, link) in self.rules.items():
            new_bandwidth = link.get_max_bandwidth()*rule.bandwidth_fraction
            link.update(new_bandwidth, msg="accommodating new rule")
        # Open new link
        new_link.bandwidth = new_link.get_max_bandwidth()*new_rule.bandwidth_fraction
        new_link.open()
        # Keep track of new rule
        self.rules[new_rule.rule_id] = (new_rule, new_link)

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
            # Create new Rule and Link objects
            rule = Rule(rule_id, src_site, dst_site, **prepared_rule["attr"])
            link = Link(src_site, dst_site, best_effort=(rule.priority > 0))
            # Compute new bandwidth provisions and open link
            self.process_rule(rule, link)

    def submitter_handler(self, payload):
        # Unpack payload
        rule_id = payload.get("rule_id")
        n_transfers_submitted = payload.get("n_transfers_submitted")
        # Update rule
        rule, link = self.rules[rule_id]
        rule.n_transfers_submitted += n_transfers_submitted
        # Get SENSE link endpoints
        sense_map = {
            link.src_site.rse_name: link.src_ipv6,
            link.dst_site.rse_name: link.dst_ipv6
        }
        return sense_map

    def finisher_handler(self, payload):
        # Unpack payload
        rule_id = payload.get("rule_id")
        n_transfers_finished = payload.get("n_transfers_finished")
        n_bytes_transferred = payload.get("n_bytes_transferred")
        # Update rule
        rule, link = self.rules[rule_id]
        rule.n_transfers_finished += n_transfers_finished
        rule.n_bytes_transferred += n_bytes_transferred
        if rule.n_transfers_finished == rule.n_transfers_total:
            link.close()

def sigint_handler(dmm):
    def actual_handler(sig, frame):
        logging.info("Stopping DMM")
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
    if args.loglevel == "DEBUG":
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
