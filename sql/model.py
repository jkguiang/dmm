from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()

class BaseSchema(BASE):
    __tablename__ = "DMM"
    transfer_id = Column(Integer(),primary_key=True)
    source_url = Column(String(50))
    destination_url = Column(String(50))
    priority = Column(Integer())
    total_transfer_size = Column(Integer())
