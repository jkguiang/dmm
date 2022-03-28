class Request:
    def __init__(self, request_id, rule_id, src_site, dst_site, transfer_ids, priority, 
                 n_bytes_total, n_transfers_total):
        self.request_id = request_id
        self.rule_id = rule_id
        self.src_site = src_site # DMM Site object
        self.dst_site = dst_site # DMM Site object
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
