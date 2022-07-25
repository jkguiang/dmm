from sqlalchemy import Column, Integer, String, Boolean, Float, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()

class BaseSchema(BASE):
    __tablename__ = "DMM"
    request_id = Column(String(100), primary_key=True)
    rule_id = Column(String(100))
    src_site = Column(String(100))
    dst_site = Column(String(100))
    transfer_ids = Column(String(100000)) # CSV
    priority = Column(Integer())
    n_bytes_total = Column(Integer())
    n_transfers_total = Column(Integer())
    src_ipv6 = Column(String(100))
    dst_ipv6 = Column(String(100))
    bandwidth = Column(Float())
    sense_link_id = Column(String(100))