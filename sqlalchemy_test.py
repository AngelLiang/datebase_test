# coding=utf-8
"""
"""

import sys
import time
import datetime as dt

from sqlalchemy import (
    create_engine,
    Column,
    Integer, String, Float, Boolean, Enum, Date, DateTime, Time, Text,
    DECIMAL
)
from sqlalchemy.dialects.mysql import LONGTEXT, TIMESTAMP

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

HOST = '127.0.0.1'
PORT = 3306
USERNAME = 'root'
PASSWORD = 'root'
DATABASE = 'test'

# DB_URI = 'sqlite///:memory:'

# n = 100_000
# common: 10.31 s
# bulk_insert: 1.28 s
# DB_URI = 'sqlite:///test.db'
###
# n = 100_000
# common: 48.64 s
# bulk_insert: 4.83 s
DB_URI = f'mysql+mysqlconnector://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}?charset=utf8'

engine = create_engine(DB_URI)
Base = declarative_base(engine)
Session = sessionmaker(engine)
session = Session()


class IotData(Base):
    __tablename__ = 'iot_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    datatime = Column(DateTime, index=True, default=dt.datetime.utcnow)
    payload = Column(String(1024))
    # timestamp = Column(TIMESTAMP(True), nullable=False,
    #                    default=dt.datetime.utcnow)


Base.metadata.drop_all()
Base.metadata.create_all()


def create_iot_data(n=100_000):
    start = time.time()
    for i in range(n):
        iot_data = IotData(payload='hello')
        session.add(iot_data)
        if i % 1000 == 0:
            session.flush()
            print(f'finish:{i}')
    session.commit()
    finish = time.time()
    print(f'run time: {finish-start}s')


def create_iot_data_using_bulk_insert(n=100_000):
    """每次批量插入1万条数据"""
    start = time.time()
    n1 = n
    while n1 > 0:
        session.bulk_insert_mappings(
            IotData,
            [{'payload': 'hello'} for i in range(min(10000, n1))]
        )
        n1 = n1 - 10000
        print(f'n1:{n1}')

    session.commit()
    finish = time.time()
    print(f'run time: {finish-start}s')


if __name__ == "__main__":
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
    else:
        n = 100_000
    print('start')
    # create_iot_data(n)
    create_iot_data_using_bulk_insert(n)
