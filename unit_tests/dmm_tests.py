import time
import unittest
from multiprocessing.connection import Client

preparer_payload = {
    "0c632b8a3e2e4710b43a7776db5d84c5": {
        "XRD1&XRD3": {
            "transfer_ids": [
                "125aeed6ff7f4ce4b66725e94d1a79fc",
                "160d072cb64e4e769436dbf9b4135ca8",
                "690d1730c0f241d4812ace19051326e4",
                "2a844e43ee9140b39bb607534703ef76"
            ],
            "priority": 3,
            "n_transfers_total": 4,
            "n_bytes_total": 4194304
        },
        "XRD1&XRD4": {
            "transfer_ids": [
                "3a49ecd56ed44eb7a6dfe872a1192847",
                "7258a0bf0ac34907b053f5d7c66c6b22"
            ],
            "priority": 1,
            "n_transfers_total": 2,
            "n_bytes_total": 2097152
        }
    }
}

submitter_payloads = [
    {
        "rule_id": "0c632b8a3e2e4710b43a7776db5d84c5",
        "rse_pair_id": "XRD1&XRD3",
        "n_transfers_submitted": 4
    },
    {
        "rule_id": "0c632b8a3e2e4710b43a7776db5d84c5",
        "rse_pair_id": "XRD1&XRD4",
        "n_transfers_submitted": 2
    }
]

finisher_payload = {
    "0c632b8a3e2e4710b43a7776db5d84c5": {
        "XRD1&XRD3": {
            "n_transfers_finished": 4,
            "n_bytes_transferred": 2097152
        },
        "XRD1&XRD4": {
            "n_transfers_finished": 2,
            "n_bytes_transferred": 4194304
        }
    }
}

class TestDMM(unittest.TestCase):
    def setUp(self):
        self.wait_time = 1
        self.address = ("localhost", 5000)
        with open("dummykey", "rb") as f_in:
            self.authkey = f_in.read()

    def preparer_step(self):
        with Client(self.address, authkey=self.authkey) as client:
            client.send(("PREPARER", preparer_payload))
        time.sleep(self.wait_time)

    def submitter_step(self):
        expected_responses = [
            {"XRD1": "127.0.0.1", "XRD3": "127.0.0.1"},
            {"XRD1": "127.0.0.1", "XRD4": "127.0.0.1"}
        ]
        for i, submitter_payload in enumerate(submitter_payloads):
            with Client(self.address, authkey=self.authkey) as client:
                rule_id = submitter_payload["rule_id"]
                rse_pair_id = submitter_payload["rse_pair_id"]
                client.send(("SUBMITTER", submitter_payload))
                resp = client.recv()
                self.assertEqual(resp, expected_responses[i])
            time.sleep(self.wait_time)
        time.sleep(50)

    def finisher_step(self):
        with Client(self.address, authkey=self.authkey) as client:
            client.send(("FINISHER", finisher_payload))

    def __workflow(self):
        steps = [
            "preparer_step", 
            "submitter_step", 
            "finisher_step"
        ]
        for step_name in steps:
            yield step_name, getattr(self, step_name)

    def test_workflow_1sec(self):
        self.wait_time = 1
        for func_name, func in self.__workflow():
            try:
                func()
            except Exception as e:
                self.fail("{} failed ({}: {})".format(func_name, type(e), e))

    # def test_workflow_2sec(self):
    #     self.wait_time = 2
    #     for func_name, func in self.__workflow():
    #         try:
    #             func()
    #         except Exception as e:
    #             self.fail("{} failed ({}: {})".format(func_name, type(e), e))
