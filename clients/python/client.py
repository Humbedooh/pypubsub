#!/usr/bin/env python3
import asfpy.pubsub


def process(payload):
    keys = list(payload.keys())
    if keys == ['stillalive']:
        print("Got a ping-back from pubsub")
    else:
        print("Got a payload!")
        print(payload)


pps = asfpy.pubsub.Listener('http://localhost:2069/')
pps.attach(process, raw=True)
