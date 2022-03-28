from datetime import datetime
from os import environ as env

from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sql.model import BaseSchema


class SQLSession(object):
    def __init__(self):
        self.session = sessionmaker()
        hostname = env.get("RUCIO_DB_ADDR")
        _ENGINE = create_engine(f"postgresql://rucio:secret@{hostname}:3306/rucio")
        self.session.configure(bind=_ENGINE)
        BaseSchema.metadata.create_all(_ENGINE)

    def write(self, transfer):
        self.session.add(transfer)
        self.session.commit()

    def delete(self, transfer):
        self.session.delete(transfer)
        self.session.commit()
        # Add check to make sure entry has been deleted
