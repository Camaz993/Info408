#!/usr/local/bin/python

import sys
import time
from datetime import datetime, timedelta

terminate = datetime.now() + timedelta(seconds=120)
refresh = 0

from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='kafka:9092')

with open("/mnt/data/2017_01_en_clickstream.tsv", "r") as clicks:
    count = 0
    for line in clicks:
        # skip the header line
        if count > 0:
            producer.send("clickstream", bytes(line, encoding="utf-8"))
        count += 1
        if count % 10000 == 0:
            sys.stdout.write("{c}\r".format(c=count))
            sys.stdout.flush()
        time.sleep(refresh)
