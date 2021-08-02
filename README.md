## PyPubSub - An asynchronous pubsub protocol written in Python 3
![PyPubSub Logo](pypubsub.svg)

<!-- toc -->

- [Introduction](#introduction)
- [Installing](#installing)
- [Topics and publishing/subscribing](#topics-and-publishingsubscribing)
  * [Subscribing to multiple topic streams](#subscribing-to-multiple-topic-streams)
- [Pushing an event to PyPubSub](#pushing-an-event-to-pypubsub)
  * [Pushing an event via Python](#pushing-an-event-via-python)
- [Listening for events](#listening-for-events)
  * [Listening for events via cURL](#listening-for-events-via-curl)
  * [Listening for events via Python](#listening-for-events-via-python)
  * [Listening for events via node.js](#listening-for-events-via-nodejs)
  * [Listening for events via Ruby](#listening-for-events-via-ruby)
  * [Accessing older payloads via the backlog catalogue](#accessing-older-payloads-via-the-backlog-catalogue)
- [Access-Control-List and private events](#access-control-list-and-private-events)
  * [Pushing a private event](#pushing-a-private-event)
  * [Retrieving private events](#retrieving-private-events)
  * [LDAP-based ACL](#ldap-based-acl)
  * [Securing certain topics](#securing-certain-topics)
- [Working with Amazon SQS](#working-with-amazon-sqs)
- [Persistent backlogs](#persistent-backlogs)
- [License](#license)

<!-- tocstop -->

## Introduction
PyPubSub is a simple publisher/subscriber service, where clients can connect and either deliver a payload (in JSON format) or listen for specific payloads as a stream of events. It is written as an asynchronous Python service, and can handle thousands of connections at any given time on a single core. It utilizes the HTTP protocol and JSON for a simplistic delivery scheme.

A working copy of this program is in service by the Apache Software Foundation, listing all development events going on at the organization (see [this page](https://infra.apache.org/pypubsub.html) for an introduction to their service).

## Installing

- Download or clone this repository: `git clone https://github.com/Humbedooh/pypubsub.git`
- Install dependencies using [pipenv](https://github.com/pypa/pipenv): `pipenv --three install -r requirements.txt`
- Edit `pypubsub.yaml` and (for ACL) `pypubsub_acl.yaml` to fit your needs
- Launch the program in the foreground or as a systemd service: `pipenv run python3 pypubsub.py`
- Check that your pubsub service is working: `curl -I http://localhost:2069`

## Topics and publishing/subscribing
PyPubSub is designed around topics for both publishing and subscribing. A client can use topics to 
describe what an event is for when publishing, as well as what a client expects to subscribe to. 
Topics are generally simple words consisting of letters and numbers, but can be anything that is 
allowed in a URI path, apart from forward slashes (`/`) and commas (`,`) (these are reserved characters).
Subscriptions are made on a "highest common denominator" basis, meaning the more topics you subscribe 
to, the fewer events you will receive, as the topics of an event must, at least, match all the topics 
a subscriber has subscribed to. Topics are set using the path segment of a URI, and are order agnostic, 
meaning `fruits and apples` is the same as `apples and fruits` internally.

As an example, let's imagine we wish to subscribe to all events for the topics surrounding `apples`, which is a sub-topic of `fruits`. We would then subscribe to `http://localhost:2069/fruits/apples` and listen for events.  
If a payload with `fruits/apples` comes in, we would receive it. If a payload with just `fruits` come in, we would not receive it, because we are specifically asking for `apples` to be present as a topic. Neither would `fruit/oranges` match our subscription, while `fruits/apples/macintosh`  would, as it contains our topics (and a bit more).

The below matrix shows how subscription paths match topics:

| Topics | `/fruits` | `/fruits/apples` | `/fruits/apples/red` | `/fruits/oranges` | `/apples` |
| --- | --- | --- | --- | --- | --- |
| fruits | ✓ | ✗ | ✗ | ✗ | ✗ |
| fruits + apples| ✓ | ✓ | ✗ | ✗ | ✓ |
| fruits + apples + red | ✓ | ✓ | ✓ | ✗ | ✓ |
| fruits + oranges | ✓ | ✗ | ✗ | ✓ | ✗ |

### Subscribing to multiple topic streams
As mentioned above, subscription topics are typically AND'ed together, with more topics narrowing the 
event stream. Thus, if you wanted to subscribe to `bar` OR `foo`, you would need two streams.

It is possible (from 0.7.0 onwards) to utilize a single stream to subscribe to multiple topic streams at once,
using a comma as a delimiter between topic batches. For instance, to subscribe to both 
`apples/red` events AND `oranges/ripe` events, you may subscribe to:

`http://localhost:2069/apples/red,oranges/ripe`.

## Pushing an event to PyPubSub
Event payloads requires that the IP or IP range (Ipv4 or IPv6) is listed in `pypubsub.yaml` under `payloaders` first.
Once whitelisted, clients can do a POST or PUT to the pubsub service on port 2069, passing a JSON object as the request body, for instance: 
~~~shell
curl -XPUT -d '{"text": "Apples are delicious"}' http://localhost:2069/fruits/apples
~~~

Event payloads *MUST* be in dictionary (hash) format, or they will be rejected.

On the subscription side, any client listening to `http://localhost:2069/fruits` or `http://localhost:2069/fruits/apples` will receive the following event in their stream:

~~~json
{
  "text": "Apples are delicious",
  "pubsub_topics": ["fruits", "apples"],
  "pubsub_path": "/fruits/apples",
  "pubsub_timestamp": 1588293679.5432327
}
~~~


### Pushing an event via Python
To push an event to PyPubSub via Python, you can make use of the requests library in Python:

~~~python
import requests
requests.put('http://localhost:2069/fruits/apples', json = {"cultivar": "macintosh"})
~~~

## Listening for events
Events are broadcast as JSON chunks in a [chunked HTTP stream](https://en.wikipedia.org/wiki/Chunked_transfer_encoding). 
Each chunk contains either a payload from a publisher, or a keep-alive ping from the 
PyPubSub server, which looks like this:

~~~json
{"stillalive": 1588132959.6066568}
~~~
Ths `stillalive` object is a simple timestamp showing, in epoch seconds, when the ping was sent.

### Listening for events via cURL
You can subscribe to topics via cURL like so: `curl http://localhost:2069/topics/here` where `topics/here` are the topics you are subscribing to, with `/` as a delimiter between topics. To subscribe to *all* events, you can omit the topics.

### Listening for events via Python
For Python, you can import the `asfpy` package via pip and utilize its pubsub plugin:
~~~python
import asfpy.pubsub

def process_event(payload):
    print("we got an event from pubsub")
    ...

def main():
    pubsub = asfpy.pubsub.Listener('http://localhost:2069')
    pubsub.attach(process_event, raw=True) # poll forever
~~~

### Listening for events via node.js
Using the PyPubSub class in [`clients/nodejs/client.js`](clients/nodejs/client.js) one can 
listen for pubsub events and process them using node.js:

~~~javascript
function process(payload) {
    // ping-back?
    if (payload.stillalive) {
        console.log("Got a ping-back");
    // Actual payload? process it!
    } else {
        console.log("Got a payload from PyPubSub!");
        console.log(payload);
    }
}

const pps = new PyPubSub('http://localhost:2069/');
pps.attach(process);
~~~

### Listening for events via Ruby
Likewise, using Ruby is a pretty straightforward case:

~~~ruby
require 'net/http'
require 'json'
require 'thread'

pubsub_URL = 'http://localhost:2069/'

def do_stuff_with(event)
  print("Got a pubsub event!:\n")
  print(event)
end

def listen(url)
  ps_thread = Thread.new do
    begin
      uri = URI.parse(url)
      Net::HTTP.start(uri.host, uri.port) do |http|
        request = Net::HTTP::Get.new uri.request_uri
        http.request request do |response|
          body = ''
          response.read_body do |chunk|
            event = JSON.parse(chunk)
            if event['stillalive']  # pingback
              print("ping? PONG!\n")
            else
              do_stuff_with(event)
            end
          end
        end
      end
  end
  return ps_thread
end

begin
  ps_thread = listen(pubsub_URL)
  print("Pubsub thread started, waiting for results...")
  while ps_thread.alive?
    sleep 10
  end
  print("Pubsub thread died :(\n")
end
~~~

### Accessing older payloads via the backlog catalogue
If configured, via the `payload_backlog_size` setting in the main configuration, clients can 
request payloads that were pushed before they subscribed, using an `X-Fetch-Since` request 
header denoting from when (in seconds since the UNIX epoch) they wish to receive events:

~~~shell
curl -H 'X-Fetch-Since: 1588293679' http://localhost:2069/
~~~

If there are any events in the backlog (private or public) that match this (aka are younger 
than the timestamp presented by the client requesting a backlog), they will be
delivered to the client, assuming they are younger than the backlog maximum age requirement. 

*It is worth noting here*, for pseudo security reasons, that if the backlog maximum is set 
sufficiently low (or the age requirement is omitted), this feature could be used to deduce 
whether or not private events have happened, as a client can request everything in the backlog 
and potentially gauge whether the size of the backlog differs from time to time. Clients without 
authorization cannot see private payloads this way, but it is theoretically possible to deduce 
_that they happened_. A sane approach is to always set the maximum age configuration to a 
value that is sufficiently low compared to the backlog max size. If you expect 1,000 payloads 
per day, you could set your max age to 48h and the backlog size to 5,000 to easily mitigate 
any potential deduction.

The backlog maximum age configuration expect a number with a time unit after it, for instance:
- `45s`: 45 seconds
- `30m`: 30 minutes
- `12h`: 12 hours
- `7d`: 7 days


## Access-Control-List and private events
PyPubSub supports private events that only authenticated clients can receive.

### Pushing a private event
To mark an event as private, simply prepend `private` as the first topic when you push the event:
~~~shell
curl -XPUT -d '{"private_text": "Squeamish Ossifrage"}' http://localhost/private/topics/here
~~~

Events broadcast with a `/private` prefix will only allude to its privacy via the `pubsub_path` 
element in the JSON blob. The topics list does not include 'private' (as it's technically not 
a topic for the broadcast). Thus the above example would output the following event to all
authed subscribers with access:

~~~json
{
  "private_text": "Squeamish Ossifrage",
  "pubsub_topics": ["topics", "here"],
  "pubsub_path": "/private/topics/here",
  "pubsub_timestamp": 1588293679.5432327
}
~~~


### Retrieving private events
Clients ACL is defined in `pypubsub_acl.yaml` (and is entirely optional, you can omit the file). 
See the example ACL configuration for an example.
Access is, as with public events, defined with "highest common denominator" in mind, meaning access to topics is granted 
to the specific topic group specified in the yaml and its sub-groups. Thus, if you grant access to `internal` and `foo` in one ACL segment, events pushed to `private/internal/foo` would be seen by that client, whereas pushes to `private/internal/bar` would not.

To authenticate and receive private events, use Basic authentication, such as:
~~~shell
curl -u 'user:pass' http://localhost:2069/internal/topics/here
~~~

### LDAP-based ACL
PyPubSub supports ACL via asynchronous LDAP, either through group memberships or single users via their dn.

See `pypubsub.yaml` for an LDAP example.

### Securing certain topics
You can secure topics, meaning only authenticated users with special credentials may post using 
those topics. To do so, you will need to edit the `secure_topics`  list in the `clients` section of 
your configuration file, for instance:

~~~yaml
clients:
  secure_topics:
    - bread
    - syrup
~~~
The above would lock publishing the topics `bread` and `syrup` for anyone not specifically allowed
 to use those topics in their ACL segment. Users or LDAP groups can be allowed topics via the 
`topics` directive in their ACL segment. See the `pypubsub_acl.yaml` file for an example.

## Working with Amazon SQS
PyPubSub supports AWS SQS for weaving in payloads from their server-less Simple Queue Services.
Multiple queues can be supported and items pushed to SQS will seamlessly appear in the 
pubsub stream. For these objects to be compatible with pypubsub, they must be JSONified 
strings with a `pubsub_path` element specifying the URI they would have otherwise been 
posted to via PyPubSub, for instance `"pubsub_path": "/fruits/apples"` for a public 
payload or `"pubsub_path": "/private/secretstuff"` for a private (auth-required) 
payload. If no such path is specified, PyPubSub will assume a default empty topic 
list (free-for-all).

For more information on how to configure SQS, please see `pypubsub.yaml`.
SQS support assumes that the AWS CLI has been set up, and the user has AWS configured 
in their .aws directory before startup.


## Persistent backlogs
PyPubSub supports using a filesystem-based backlog file for persistent storage of the 
backlog through restarts. To enable this feature, uncomment the backlog configuration 
in the pypubsub.yaml config file. This will store up to whatever the max backlog 
queue size is every 10 seconds assuming the backlog has changed. On restart, the 
backlog file will be read in and added to the in-memory backlog.

## License
PyPubSub is licensed under the Apache License v/2.
