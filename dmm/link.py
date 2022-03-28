import time
import dmm.nonsense_api as sense_api
from dmm.monitoring import PrometheusSession

class Link:
    def __init__(self, src_site, dst_site, bandwidth=0, best_effort=False):
        self.src_site = src_site # DMM Site object
        self.dst_site = dst_site # DMM Site object
        self.best_effort = best_effort
        self.is_open = False
        self.src_ipv6 = ""
        self.dst_ipv6 = ""
        self.bandwidth = bandwidth
        self.history = [(time.time(), bandwidth, 0, "init")]
        self.prometheus = PrometheusSession()
        self.sense_link_id = sense_api.get_profile_uuid()
        self.theoretical_bandwidth = -1
        self.update_theoretical_bandwidth()

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
        self.src_ipv6 = ""
        self.dst_ipv6 = ""

    def reprovision(self, new_bandwidth):
        """Reprovision SENSE link
        
        Note: this is run in a child process (not on shared memory), so self.bandwidth 
              must be updated externally
        """
        if not self.best_effort and new_bandwidth != self.bandwidth:
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

    def open(self):
        """Create SENSE link
        
        Note: this is run in a child process (not on shared memory), so self.bandwidth 
              must be updated externally
        """
        if not self.best_effort:
            self.sense_link_id = sense_api.create_link(
                self.src_site.sense_name,
                self.dst_site.sense_name,
                self.src_ipv6,
                self.dst_ipv6,
                self.bandwidth,
                instance_uuid=self.sense_link_id
            )
        self.is_open = True

    def close(self):
        if not self.best_effort:
            sense_api.delete_link(self.sense_link_id)
            self.sense_link_id = ""
        self.is_open = False
