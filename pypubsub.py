#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

""" This is the ASF's simplified publisher/subscriber service """
import asyncio
import aiohttp.web
import time
import json
import yaml
import netaddr
import binascii
import base64

# Some consts
PUBSUB_VERSION = '0.3.0'
PUBSUB_BAD_REQUEST = "I could not understand your request, sorry! Please see https://pubsub.apache.org/api.html \
for usage documentation.\n"
PUBSUB_PAYLOAD_RECEIVED = "Payload received, thank you very much!\n"
PUBSUB_NOT_ALLOWED = "You are not authorized to deliver payloads!\n"
PUBSUB_BAD_PAYLOAD = "Bad payload type. Payloads must be JSON dictionary objects, {..}!\n"
CONF = None
ACL = None
OLD_SCHOOLERS = ['svnwcsub', ]  # Old-school clients that use \0 terminators.

# Internal score-keeping vars
NUM_REQUESTS = 0   # Number of total requests served
SUBSCRIBERS = []   # Current subscribers to everything
PENDING_PUBS = []  # Payloads pending publication
LAST_PING = 0      # Last time we did a global ping (every 5 seconds)
PAYLOADERS = []    # IPs that can deliver payloads


class Subscriber:
    """ Basic subscriber (client) class.
        Holds information about the connection and ACL
    """
    def __init__(self, connection, request):
        self.connection = connection
        self.request = request
        self.acl = {}

        # Set topics subscribed to
        self.topics = [x for x in request.path.split('/') if x]

        # Is the client old and expecting zero-terminators?
        self.old_school = False
        for ua in OLD_SCHOOLERS:
            if ua in request.headers.get('User-Agent', ''):
                self.old_school = True
                break
        # Is there a basic auth in this request? If so, set up ACL
        auth = request.headers.get('Authorization')
        if auth:
            self.set_acl(auth)

    def set_acl(self, basic):
        """ Sets the ACL if possible, based on Basic Auth """
        try:
            decoded = str(base64.decodebytes(bytes(basic.replace('Basic ', ''), 'utf-8')), 'utf-8')
            u, p = decoded.split(':', 1)
            if u in ACL:
                acl_pass = ACL[u].get('password')
                if acl_pass and acl_pass == p:
                    self.acl = ACL[u].get('acl', {})
                    # Vet ACL for user
                    if not isinstance(self.acl, dict):
                        raise AssertionError(f"ACL for user {u} must be a dictionary of sub-IDs and topics, but is not.")
                    # Make sure each ACL segment is a list of topics
                    for k, v in self.acl.items():
                        if not isinstance(v, list):
                            raise AssertionError(f"ACL segment {k} for user {u} is not a list of topics!")
                    print(f"Client {u} successfully authenticated (and ACL is valid).")
        except binascii.Error as e:
            self.acl = {}
        except AssertionError as e:
            print(e)
            print(f"ACL configuration error: ACL scheme for {u} contains errors, setting ACL to nothing.")
            self.acl = {}
        except Exception as e:
            print(f"Basic unknown exception occurred: {e}")

    async def ping(self):
        """ Generic ping-back to the client """
        js = b"%s\n" % json.dumps({"stillalive": time.time()}).encode('utf-8')
        if self.old_school:
            js += b"\0"
        await self.connection.write(js)


class Payload:
    """ A payload (event) object sent by a registered publisher. """
    def __init__(self, path, data):
        self.json = data
        self.topics = [x for x in path.split('/') if x]
        self.path = path
        self.private = False

        # Private payload?
        if self.topics[0] == 'private':
            self.private = True
            self.topics = self.topics[1:]  # Remove the private bit from topics now.

        self.json['pubsub_topics'] = self.topics
        self.json['pubsub_path'] = self.path

    async def publish(self, subscribers):
        """ Publishes an object to all subscribers using those topics (or a sub-set thereof) """
        js = b"%s\n" % json.dumps(self.json).encode('utf-8')
        ojs = js + b"\0"
        for sub in subscribers:
            # If a private payload, check ACL and bail if not a match
            if self.private:
                can_see = False
                for key, private_topics in sub.acl.items():
                    if all(el in self.topics for el in private_topics):
                        can_see = True
                        break
                if not can_see:
                    continue
            # If subscribed to all the topics, tell a subscriber about this
            if all(el in self.topics for el in sub.topics):
                try:
                    if sub.old_school:
                        await sub.connection.write(ojs)
                    else:
                        await sub.connection.write(js)
                except ConnectionResetError:
                    pass
                except RuntimeError:
                    pass
                except AssertionError:  # drain helper throws these sometimes
                    pass


async def poll():
    """ Polls for new stuff to publish, and if found, publishes to whomever wants it. """
    global LAST_PING, PENDING_PUBS
    while True:
        for payload in PENDING_PUBS:
            await payload.publish(SUBSCRIBERS)
        PENDING_PUBS = []
        await asyncio.sleep(0.5)


async def handler(request):
    """ Generic handler for all incoming HTTP requests """
    global NUM_REQUESTS
    NUM_REQUESTS += 1
    # Define response headers first...
    headers = {
        'Server': 'PyPubSub/%s' % PUBSUB_VERSION,
        'X-Subscribers': str(len(SUBSCRIBERS)),
        'X-Requests': str(NUM_REQUESTS),
    }

    # Are we handling a publisher payload request? (PUT/POST)
    if request.method in ['PUT', 'POST']:
        ip = netaddr.IPAddress(request.remote)
        allowed = False
        for network in PAYLOADERS:
            if ip in network:
                allowed = True
                break
        if not allowed:
            resp = aiohttp.web.Response(headers=headers, status=403, text=PUBSUB_NOT_ALLOWED)
            return resp
        if request.can_read_body:
            try:
                body = await request.json()
                assert isinstance(body, dict)  # Payload MUST be an dictionary object, {...}
                PENDING_PUBS.append(Payload(request.path, body))
                resp = aiohttp.web.Response(headers=headers, status=202, text=PUBSUB_PAYLOAD_RECEIVED)
                return resp
            except json.decoder.JSONDecodeError:
                resp = aiohttp.web.Response(headers=headers, status=400, text=PUBSUB_BAD_REQUEST)
                return resp
            except AssertionError:
                resp = aiohttp.web.Response(headers=headers, status=400, text=PUBSUB_BAD_PAYLOAD)
                return resp
    # Is this a subscriber request? (GET)
    elif request.method == 'GET':
        resp = aiohttp.web.StreamResponse(headers=headers)
        # We do not support HTTP 1.0 here...
        if request.version.major == 1 and request.version.minor == 0:
            return resp
        subscriber = Subscriber(resp, request)
        SUBSCRIBERS.append(subscriber)
        # We'll change the content type once we're ready
        # resp.content_type = 'application/vnd.apache-pubsub-stream'
        resp.content_type = 'application/json'
        try:
            resp.enable_chunked_encoding()
            await resp.prepare(request)
            while True:
                await subscriber.ping()
                await asyncio.sleep(5)
        # We may get exception types we don't have imported, so grab ANY exception and kick out the subscriber
        except:
            pass
        SUBSCRIBERS.remove(subscriber)
        return resp
    elif request.method == 'HEAD':
        resp = aiohttp.web.Response(headers=headers, status=204, text="")
        return resp
    #  I don't know this type of request :/ (DELETE, PATCH, etc)
    else:
        resp = aiohttp.web.Response(headers=headers, status=400, text=PUBSUB_BAD_REQUEST)
        return resp


async def main():
    """ Main loop... """
    server = aiohttp.web.Server(handler)
    runner = aiohttp.web.ServerRunner(server)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, CONF['server']['bind'], CONF['server']['port'])
    await site.start()
    print("==== PyPubSub v/%s starting... ====" % PUBSUB_VERSION)
    print("==== Serving up PubSub goodness at %s:%s ====" % (CONF['server']['bind'], CONF['server']['port']))

    await poll()


if __name__ == '__main__':
    CONF = yaml.safe_load(open('pypubsub.yaml'))
    try:
        ACL = yaml.safe_load(open('pypubsub_acl.yaml'))
    except:
        ACL = {}
    PAYLOADERS = [netaddr.IPNetwork(x) for x in CONF['clients']['payloaders']]
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    loop.close()
