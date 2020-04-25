### PyPubSub - An asynchronous pubsub protocol written in Python 3

## Introduction
PyPubSub is a simple publisher/subscriber service, where clients can connect and either deliver a payload (in JSON format) or listen for specific payloads as a stream of events. It is written as an asynchronous Python service, and can handle thousands of connections at any given time on a single core. It utilizes the HTTP protocol and JSON for a simplistic delivery scheme.

A working copy of this program is in service by the Apache Software Foundation, listing all development events going on at the organization (see [this page](https://infra.apache.org/pypubsub.html) for an introduction to their service).

## Installing

- Download or clone this repository: `git clone https://github.com/Humbedooh/pypubsub.git`
- Install dependencies: `pip3 install -r requirements.txt`
- Edit `pypubsub.yaml` and (for ACL) `pypubsub_acl.yaml` to fit your needs
- Launch the program in the foreground or as a systemd service: `python3 pypubsub.py`
- Check that your pubsub service is working: `curl -I http://localhost:2069`

## Topics and publishing/subscribing
PyPubSub is designed around topics for both publishing and subscribing. I client can use topics to describe what an event is for when publishing, as well as what a client expects to subscribe to. Subscriptions are made on a "highest common denominator" basis, meaning the more topics you subscribe to, the fewer events you will receive, as the topics of an event must, at least, match all the topics a subscriber has subscribed to. Topics are set using the path segment of a URI.

As an example, let's imagine we wish to subscribe to all events for the topics surrounding `apples`, which is a sub-topic of `fruits`. We would then subscribe to `http://localhost:2069/fruits/apples` and listen for events.  
If a payload with `fruits/apples` comes in, we would receive it. If a payload with just `fruits` come in, we would not receive it, because we are specifically asking for `apples` to be present as a topic. Neither would `fruit/oranges` match our subscription, while `fruits/apples/macintosh`  would, as it contains our topics (and a bit more).

The below matrix shows how subscription paths match topics:

| Topics | `/fruits` | `/fruits/apples` | `/fruits/apples/red` | `/fruits/oranges` |
| --- | --- | --- | --- | --- |
| fruits | ✓ | ✗ | ✗ | ✗ |
| fruits + apples| ✓ | ✓ | ✗ | ✗ |
| fruits + apples + red | ✓ | ✓ | ✓ | ✗ |
| fruits + oranges | ✓ | ✗ | ✗ | ✓ |


## Pushing an event to PyPubSub
Event payloads requires that the IP or IP range (Ipv4 or IPv6) is listed in `pypubsub.yaml` under `payloaders` first.
Once whitelisted, clients can do a POST or PUT to the pubsub service on port 2069, passing a JSON object as the request body, for instance: 
~~~shell
curl -XPUT -d '{"foo": "bar"}' http://localhost:2069/some/topic/here
~~~

Event payloads *MUST* be in dictionary (hash) format, or they will be rejected.

### Pushing an event via Python
To push an event to PyPubSub via Python, you can make use of the requests library in Python:

~~~python
import requests
requests.put('http://localhost:2069/fruits/apples', json = {"applesort": "macintosh"})
~~~

## Listening for events in PyPubSub via cURL
You can subscribe to topics via cURL like so: `curl http://localhost:2069/topics/here` where `topics/here` are the topics you are subscribing to, with `/` as a delimiter between topics. To subscribe to *all* events, you can omit the topics.

## Listening for events via Python
For Python, you can import the `asfpy` package via pip and utilize its pubsub plugin:
~~~python
import asfpy.pubsub

def process_event(payload):
    print("we got an event from pubsub")
    ...

def main():
    pubsub = asfpy.pubsub.Listener('http://localhost:2069')
    pubsub.attach(process_event) # poll forever
~~~

## Access-Control-List and private events
PyPubSub supports private events that only authenticated clients can receive.

### Pushing a private event
To mark an event as private, simply prepend `private` as the first topic when you push the event:
~~~shell
curl -XPUT -d '{"private_text": "Squeamish Ossifrage"}' http://localhost/private/topics/here
~~~

### Retreiving private events
Clients ACL is defined in `pypubsub_acl.yaml` (and is entirely optional, you can omit the file). 
See the example ACL configuration for an example.
Access is, as with public events, defined with "highest common denominator" in mind, meaning access to topics is granted 
to the specific topic group specified in the yaml and its sub-groups. Thus, if you grant access to `internal` and `foo` in one ACL segment, events pushed to `private/internal/foo` would be seen by that client, whereas pushes to `private/internal/bar` would not.

To authenticate and receive private events, use Basic authentication, such as:
~~~shell
curl -u 'user:pass' http://localhost:2069/internal/topics/here
~~~

## License
PyPubSub is licensed under the Apache License v/2.
