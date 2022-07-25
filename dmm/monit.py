import yaml
import logging
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class FTSmonit:
    def __init__(self):
        with open("config.yaml", "r") as f_in:
            dmm_config = yaml.safe_load(f_in).get("fts")
            self.fts_host = dmm_config.get("host", "fts")
            usercert = dmm_config.get("usercert") 
        self.cert = (usercert, usercert)
        self.headers = {'Content-Type': 'application/json'}
        self.verify = False

    def get_log_addr(self, transfer_id):
        job = requests.get(f"{self.fts_host}/jobs/{transfer_id}/files",
                           cert=self.cert, verify=self.verify, headers=self.headers)
        if job and (job.status_code == 200 or job.status_code == 207):
            file = job.json()[0]
            return f"https://{file['transfer_host']}:8449{file['log_file']}"

    @staticmethod        
    def write_log(transfer_id, log):
        with open(f"/tmp/fts-transfer-{transfer_id}.log", 'w+') as file:
            file.write(log)

    def log_request(self, transfer_ids):
        for transfer_id in transfer_ids:
            try:
                log_addr = self.get_log_addr(transfer_id)
                log = requests.get(log_addr, verify=self.verify)
                if log and log.status_code == 200:
                    self.write_log(transfer_id, log.text)
            except:
                logging.debug(f"Exception: Job {transfer_id} not found")
