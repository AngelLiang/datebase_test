# coding=utf-8
"""
n = 100_000, run time: 1.97 s
n = 1_000_000, run time: 18.84 s
"""

import sys
import time
import datetime as dt
from influxdb import InfluxDBClient

HOST = '127.0.0.1'
PORT = 8086
USERNAME = None
PASSWORD = None
DATABSE = 'test'

client = InfluxDBClient(HOST, PORT, USERNAME, PASSWORD, DATABSE)
client.create_database(DATABSE)


def IotData(payload='hello'):
    return {
        'measurement': 'iot_data',
        'tags': {
            # 'tag': 'iot'
        },
        'fields': {
            "payload": payload
        },
        'time': dt.datetime.utcnow()
    }


def create_iot_data(n=100_000):
    start = time.time()
    points = []
    for i in range(n):
        iot_data = IotData(payload='hello')
        points.append(iot_data)
        if i % 1000 == 0:
            print(f'finish:{i}')
            client.write_points(points)
            points.clear()
    finish = time.time()
    print(f'run time: {finish-start}s')


def create_iot_data2(n=100_000):
    """每次批量插入1万条数据"""
    start = time.time()
    n1 = n
    min_size = 10000
    while n1 > 0:
        client.write_points([IotData(payload='hello')
                             for i in range(min(min_size, n1))])
        n1 = n1 - min_size
        print(f'n1:{n1}')
    finish = time.time()
    print(f'run time: {finish-start}s')


if __name__ == "__main__":
    if len(sys.argv) > 1:
        n = int(sys.argv[1])
    else:
        n = 100_000
    print('start')
    # create_iot_data(n)
    create_iot_data2(n)
