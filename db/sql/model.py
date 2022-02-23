from sqlalchemy import Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.ext.declarative import declarative_base

BASE = declarative_base()

class BaseSchema(BASE):
    __tablename__ = "DMM"
    source_url = Column(String(50),primary_key=True)
    destination_url = Column(String(50),primary_key=True)
    priority = Column(Integer(),primary_key=True)
    total_transfer_size = Column(Integer(),primary_key=True)
