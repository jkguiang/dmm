from datetime import datetime
from os import environ as env
import yaml
import logging

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from dmm.sql.model import BaseSchema

from dmm.request import Request
from dmm.site import Site

class SQLSession(object):
    def __init__(self):
        Session = sessionmaker()

        with open("config.yaml", "r") as f_in:
            dmm_config = yaml.safe_load(f_in).get("sql_db")
            self.db_hostname = dmm_config.get("host", "ruciodb") 

        _ENGINE = create_engine(
            f"postgresql://rucio:secret@{self.db_hostname}/rucio")
        Session.configure(bind=_ENGINE)
        BaseSchema.metadata.create_all(_ENGINE)
        logging.info("Initializing database")
        self.session = Session()
        self.current_state = None

    @staticmethod
    def get_schema_from_req(request):    
        req = BaseSchema(
            request_id = request.request_id,
            rule_id = request.rule_id,
            src_site = request.src_site.rse_name,
            dst_site = request.dst_site.rse_name,
            transfer_ids = ",".join(request.transfer_ids),
            priority = request.priority,
            n_bytes_total = request.n_bytes_total,
            n_transfers_total = request.n_transfers_total,
            src_ipv6 = request.src_ipv6,
            dst_ipv6 = request.dst_ipv6,
            bandwidth = request.bandwidth,
            sense_link_id = request.sense_link_id
        )
        return req

    @staticmethod
    def get_req_from_schema(query):
        req = Request(
            query.rule_id, 
            Site(query.src_site),
            Site(query.dst_site), 
            query.transfer_ids.split(','), 
            query.priority, 
            query.n_bytes_total, 
            query.n_transfers_total
        ) 
        req.src_ipv6 = query.src_ipv6
        req.dst_ipv6 = query.dst_ipv6
        req.bandwidth = query.bandwidth
        req.sense_link_id = query.sense_link_id
        req_id = Request.id(query.rule_id, query.src_site, query.dst_site)
        return req_id, req

    def add_request(self, request):
        logging.info(f"Adding request {request.request_id} to database")
        req = self.get_schema_from_req(request)
        self.session.add(req)
        self.session.commit()

    def delete_request(self, request):
        req_to_be_deleted = self.session.query(BaseSchema).filter(
            BaseSchema.rule_id == request.rule_id).one()
        logging.info("deleting request from database")
        self.session.delete(req_to_be_deleted)
        self.session.commit()
    
    def query_db(self):
        query = self.session.query(BaseSchema).all()
        self.current_state = query
        return sum(1 for _ in query)

    def restore_from_curr_state(self):
        logging.info("Found requests in database, restoring state")
        requests_ = {}
        for sch in self.current_state:
            req = self.get_req_from_schema(sch) 
            requests_.update({req[0]:req[1]})
        return requests_