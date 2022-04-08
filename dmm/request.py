import time
import dmm.nonsense_api as sense_api
from dmm.monitoring import PrometheusSession

class Request:
    def __init__(self, rule_id, src_site, dst_site, transfer_ids, priority, 
                 n_bytes_total, n_transfers_total):
        # General attributes
        self.request_id = Request.id(rule_id, src_site.rse_name, dst_site.rse_name)
        self.rule_id = rule_id
        self.src_site = src_site # DMM Site object
        self.dst_site = dst_site # DMM Site object

        # Attributes unpacked from prepared_request["attr"]
        self.transfer_ids = transfer_ids
        self.priority = priority
        self.n_bytes_total = n_bytes_total
        self.n_bytes_transferred = 0
        self.n_transfers_total = n_transfers_total
        self.n_transfers_submitted = 0
        self.n_transfers_finished = 0

        # SENSE link attributes
        self.best_effort = (self.priority == 0)
        self.link_is_open = False
        self.src_ipv6 = ""
        self.dst_ipv6 = ""
        self.bandwidth = 0
        self.history = [(time.time(), self.bandwidth, 0, "init")]
        self.prometheus = PrometheusSession()
        self.sense_link_id = sense_api.get_profile_uuid()
        self.theoretical_bandwidth = -1
        self.update_theoretical_bandwidth()

    @staticmethod
    def id(rule_id, src_rse_name, dst_rse_name):
        return f"{rule_id}_{src_rse_name}_{dst_rse_name}"

    def __str__(self):
        return f"Request({self.request_id})"

    def update_history(self, msg, monitoring=False):
        """Track the promised and actual bandwidth"""
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
        """Register new request at the source and destination sites"""
        self.src_site.add_request(self.dst_site.rse_name, self.priority)
        self.dst_site.add_request(self.src_site.rse_name, self.priority)
        if self.best_effort:
            self.src_ipv6 = self.src_site.default_ipv6
            self.dst_ipv6 = self.dst_site.default_ipv6
        else:
            self.src_ipv6 = self.src_site.reserve_ipv6()
            self.dst_ipv6 = self.dst_site.reserve_ipv6()

    def deregister(self):
        """Deregister new request at the source and destination sites"""
        self.src_site.remove_request(self.dst_site.rse_name, self.priority)
        self.dst_site.remove_request(self.src_site.rse_name, self.priority)
        if not self.best_effort:
            self.src_site.free_ipv6(self.src_ipv6)
            self.dst_site.free_ipv6(self.dst_ipv6)
        self.src_ipv6 = ""
        self.dst_ipv6 = ""

    def update_theoretical_bandwidth(self):
        if self.best_effort:
            return
        link_id, theoretical_bandwidth = sense_api.get_theoretical_bandwidth(
            self.src_site.sense_name,
            self.dst_site.sense_name,
            instance_uuid=self.sense_link_id
        )
        self.sense_link_id = link_id
        self.theoretical_bandwidth = theoretical_bandwidth

    def get_max_bandwidth(self):
        if self.best_effort:
            return 0
        else:
            return min(
                self.src_site.get_uplink_provision(self.dst_site.rse_name),
                self.dst_site.get_uplink_provision(self.src_site.rse_name),
                self.theoretical_bandwidth
            )

    def get_bandwidth_fraction(self):
        """Return bandwidth fraction

                                     my priority
        fraction = ----------------------------------------------
                   sum(all priorities between my source and dest)
        """
        if self.best_effort:
            return 0
        else:
            return self.priority/self.src_site.prio_sums.get(self.dst_site.rse_name)

    def reprovision_link(self):
        """Reprovision SENSE link"""
        old_bandwidth = self.bandwidth
        new_bandwidth = self.get_max_bandwidth()*self.get_bandwidth_fraction()
        if not self.best_effort and new_bandwidth != old_bandwidth:
            # Update SENSE link; note: in the future, this should not change the link ID
            self.sense_link_id = sense_api.reprovision_link(
                self.sense_link_id, 
                self.src_site.sense_name,
                self.dst_site.sense_name,
                self.src_ipv6,
                self.dst_ipv6,
                new_bandwidth
            )
            self.bandwidth = new_bandwidth

    def open_link(self):
        """Create SENSE link"""
        if not self.best_effort:
            self.bandwidth = self.get_max_bandwidth()*self.get_bandwidth_fraction()
            self.sense_link_id = sense_api.create_link(
                self.src_site.sense_name,
                self.dst_site.sense_name,
                self.src_ipv6,
                self.dst_ipv6,
                self.bandwidth,
                instance_uuid=self.sense_link_id
            )
        self.link_is_open = True

    def close_link(self):
        """Close SENSE link"""
        if not self.best_effort:
            sense_api.delete_link(self.sense_link_id)
            self.sense_link_id = ""
        self.link_is_open = False
