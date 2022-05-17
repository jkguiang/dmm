import yaml
import logging
import dmm.sense_api as sense_api

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
            if not site_config:
                logging.error(f"no config for {rse_name} in config.yaml!")
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
