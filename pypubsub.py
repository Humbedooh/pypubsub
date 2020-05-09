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

"""PyPubSub - a simple publisher/subscriber service written in Python 3"""
import asyncio
import aiohttp.web
import time
import json
import yaml
import netaddr
import binascii
import base64
import argparse
import collections
import plugins.ldap
import plugins.sqs

# Some consts
PUBSUB_VERSION = '0.6.0'
PUBSUB_CONTENT_TYPE = 'application/vnd.pypubsub-stream'
PUBSUB_DEFAULT_PORT = 2069
PUBSUB_DEFAULT_IP = '0.0.0.0'
PUBSUB_DEFAULT_MAX_PAYLOAD_SIZE = 102400
PUBSUB_DEFAULT_BACKLOG_SIZE = 0
PUBSUB_DEFAULT_BACKLOG_AGE = 0
PUBSUB_BAD_REQUEST = "I could not understand your request, sorry! Please see https://pubsub.apache.org/api.html \
for usage documentation.\n"
PUBSUB_PAYLOAD_RECEIVED = "Payload received, thank you very much!\n"
PUBSUB_NOT_ALLOWED = "You are not authorized to deliver payloads!\n"
PUBSUB_BAD_PAYLOAD = "Bad payload type. Payloads must be JSON dictionary objects, {..}!\n"
PUBSUB_PAYLOAD_TOO_LARGE = "Payload is too large for me to serve, please make it shorter.\n"


class Configuration:
    def __init__(self, yml):

        # LDAP Settings
        self.ldap = None
        lyml = yml.get('clients', {}).get('ldap')
        if isinstance(lyml, dict):
            self.ldap = plugins.ldap.LDAPConnection(lyml)

        # SQS?
        self.sqs = yml.get('sqs')

        # Main server config
        self.server = collections.namedtuple('serverConfig', 'ip port payload_limit')
        self.server.ip = yml['server'].get('bind', PUBSUB_DEFAULT_IP)
        self.server.port = int(yml['server'].get('port', PUBSUB_DEFAULT_PORT))
        self.server.payload_limit = int(yml['server'].get('max_payload_size', PUBSUB_DEFAULT_MAX_PAYLOAD_SIZE))

        # Backlog settings
        self.backlog = collections.namedtuple('backlogConfig', 'max_age queue_size')
        bma = yml['server'].get('backlog', {}).get('max_age', PUBSUB_DEFAULT_BACKLOG_AGE)
        if isinstance(bma, str):
            bma = bma.lower()
            if bma.endswith('s'):
                bma = int(bma.replace('s', ''))
            elif bma.endswith('m'):
                bma = int(bma.replace('m', '')) * 60
            elif bma.endswith('h'):
                bma = int(bma.replace('h', '')) * 3600
            elif bma.endswith('d'):
                bma = int(bma.replace('d', '')) * 86400
        self.backlog.max_age = bma
        self.backlog.queue_size = yml['server'].get('backlog', {}).get('size',
                                                                       PUBSUB_DEFAULT_BACKLOG_SIZE)

        # Payloaders - clients that can post payloads
        self.payloaders = [netaddr.IPNetwork(x) for x in yml['clients'].get('payloaders', [])]

        # Binary backwards compatibility
        self.oldschoolers = yml['clients'].get('oldschoolers', [])

class Server:
    """Main server class, responsible for handling requests and publishing events """

    def __init__(self, args):
        self.yaml = yaml.safe_load(open(args.config))
        self.config = Configuration(self.yaml)
        self.subscribers = []
        self.pending_events = []
        self.backlog = []
        self.last_ping = time.time()
        self.server = None
        self.acl = {}
        try:
            self.acl = yaml.safe_load(open(args.acl))
        except FileNotFoundError:
            print(f"ACL configuration file {args.acl} not found, private events will not be broadcast.")


    async def poll(self):
        """Polls for new stuff to publish, and if found, publishes to whomever wants it."""
        while True:
            for payload in self.pending_events:
                bad_subs = await payload.publish(self.subscribers)
                # Cull subscribers we couldn't deliver payload to.
                for bad_sub in bad_subs:
                    print("Culling %r due to connection errors" % bad_sub)
                    self.subscribers.remove(bad_sub)
            self.pending_events = []
            await asyncio.sleep(0.5)

    async def handle_request(self, request):
        """Generic handler for all incoming HTTP requests"""
        # Define response headers first...
        headers = {
            'Server': 'PyPubSub/%s' % PUBSUB_VERSION,
            'X-Subscribers': str(len(self.subscribers)),
            'X-Requests': str(self.server.requests_count),
        }

        # Are we handling a publisher payload request? (PUT/POST)
        if request.method in ['PUT', 'POST']:
            ip = netaddr.IPAddress(request.remote)
            allowed = False
            for network in self.config.payloaders:
                if ip in network:
                    allowed = True
                    break
            if not allowed:
                resp = aiohttp.web.Response(headers=headers, status=403, text=PUBSUB_NOT_ALLOWED)
                return resp
            if request.can_read_body:
                try:
                    if request.content_length > self.config.server.payload_limit:
                        resp = aiohttp.web.Response(headers=headers, status=400, text=PUBSUB_PAYLOAD_TOO_LARGE)
                        return resp
                    body = await request.text()
                    as_json = json.loads(body)
                    assert isinstance(as_json, dict)  # Payload MUST be an dictionary object, {...}
                    pl = Payload(request.path, as_json)
                    self.pending_events.append(pl)
                    # Add to backlog?
                    if self.config.backlog.queue_size > 0:
                        self.backlog.append(pl)
                        # If backlog has grown too large, delete the first (oldest) item in it.
                        if len(self.backlog) > self.config.backlog.queue_size:
                            del self.backlog[0]

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
            subscriber = Subscriber(self, resp, request)

            # Is there a basic auth in this request? If so, set up ACL
            auth = request.headers.get('Authorization')
            if auth:
                subscriber.acl = await subscriber.parse_acl(auth)

            # Subscribe the user before we deal with the potential backlog request and pings
            self.subscribers.append(subscriber)
            resp.content_type = PUBSUB_CONTENT_TYPE
            try:
                resp.enable_chunked_encoding()
                await resp.prepare(request)

                # Is the client requesting a backlog of items?
                backlog = request.headers.get('X-Fetch-Since')
                if backlog:
                    try:
                        backlog_ts = int(backlog)
                    except ValueError:  # Default to 0 if we can't parse the epoch
                        backlog_ts = 0
                    # If max age is specified, force the TS to minimum that age
                    if self.config.backlog.max_age > 0:
                        backlog_ts = max(backlog_ts, time.time() - self.config.backlog.max_age)
                    # For each item, publish to client if new enough.
                    for item in self.backlog:
                        if item.timestamp >= backlog_ts:
                            await item.publish([subscriber])

                while True:
                    await subscriber.ping()
                    if subscriber not in self.subscribers:  # If we got dislodged somehow, end session
                        break
                    await asyncio.sleep(5)
            # We may get exception types we don't have imported, so grab ANY exception and kick out the subscriber
            except:
                pass
            if subscriber in self.subscribers:
                self.subscribers.remove(subscriber)
            return resp
        elif request.method == 'HEAD':
            resp = aiohttp.web.Response(headers=headers, status=204, text="")
            return resp
        #  I don't know this type of request :/ (DELETE, PATCH, etc)
        else:
            resp = aiohttp.web.Response(headers=headers, status=400, text=PUBSUB_BAD_REQUEST)
            return resp

    async def server_loop(self, loop):
        self.server = aiohttp.web.Server(self.handle_request)
        runner = aiohttp.web.ServerRunner(self.server)
        await runner.setup()
        site = aiohttp.web.TCPSite(runner, self.config.server.ip, self.config.server.port)
        await site.start()
        print("==== PyPubSub v/%s starting... ====" % PUBSUB_VERSION)
        print("==== Serving up PubSub goodness at %s:%s ====" % (
            self.config.server.ip, self.config.server.port))
        if self.config.sqs:
            for key, config in self.config.sqs.items():
                loop.create_task(plugins.sqs.get_payloads(self, config))
        await self.poll()

    def run(self):
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(self.server_loop(loop))
        except KeyboardInterrupt:
            pass
        loop.close()


class Subscriber:
    """Basic subscriber (client) class. Holds information about the connection and ACL"""

    def __init__(self, server, connection, request):
        self.connection = connection
        self.acl = {}
        self.server = server

        # Set topics subscribed to
        self.topics = [x for x in request.path.split('/') if x]

        # Is the client old and expecting zero-terminators?
        self.old_school = False
        for ua in self.server.config.oldschoolers:
            if ua in request.headers.get('User-Agent', ''):
                self.old_school = True
                break

    async def parse_acl(self, basic):
        """Sets the ACL if possible, based on Basic Auth"""
        try:
            decoded = str(base64.decodebytes(bytes(basic.replace('Basic ', ''), 'ascii')), 'utf-8')
            u, p = decoded.split(':', 1)
            if u in self.server.acl:
                acl_pass = self.server.acl[u].get('password')
                if acl_pass and acl_pass == p:
                    acl = self.server.acl[u].get('acl', {})
                    # Vet ACL for user
                    assert isinstance(acl, dict), f"ACL for user {u} " \
                                                  f"must be a dictionary of sub-IDs and topics, but is not."
                    # Make sure each ACL segment is a list of topics
                    for k, v in acl.items():
                        assert isinstance(v, list), f"ACL segment {k} for user {u} is not a list of topics!"
                    print(f"Client {u} successfully authenticated (and ACL is valid).")
                    return acl
            elif self.server.config.ldap:
                acl = {}
                groups = await self.server.config.ldap.get_groups(u,p)
                # Make sure each ACL segment is a list of topics
                for k, v in self.server.config.ldap.acl.items():
                    if k in groups:
                        assert isinstance(v, dict), f"ACL segment {k} for user {u} is not a dictionary of segments!"
                        for segment, topics in v.items():
                            print(f"Enabling ACL segment {segment} for user {u}")
                            assert isinstance(topics,
                                              list), f"ACL segment {segment} for user {u} is not a list of topics!"
                            acl[segment] = topics
                return acl
        except binascii.Error as e:
            pass  # Bad Basic Auth params, bail quietly
        except AssertionError as e:
            print(e)
            print(f"ACL configuration error: ACL scheme for {u} contains errors, setting ACL to nothing.")
        except Exception as e:
            print(f"Basic unknown exception occurred: {e}")
        return {}

    async def ping(self):
        """Generic ping-back to the client"""
        js = b"%s\n" % json.dumps({"stillalive": time.time()}).encode('utf-8')
        if self.old_school:
            js += b"\0"
        await self.connection.write(js)


class Payload:
    """A payload (event) object sent by a registered publisher."""

    def __init__(self, path, data):
        self.json = data
        self.timestamp = time.time()
        self.topics = [x for x in path.split('/') if x]
        self.private = False

        # Private payload?
        if self.topics and self.topics[0] == 'private':
            self.private = True
            del self.topics[0]  # Remove the private bit from topics now.

        self.json['pubsub_timestamp'] = self.timestamp
        self.json['pubsub_topics'] = self.topics
        self.json['pubsub_path'] = path

    async def publish(self, subscribers):
        """Publishes an object to all subscribers using those topics (or a sub-set thereof)"""
        js = b"%s\n" % json.dumps(self.json).encode('utf-8')
        ojs = js + b"\0"
        bad_subs = []
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
                except Exception:
                    bad_subs.append(sub)
        return bad_subs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Configuration file to load (default: pypubsub.yaml)", default="pypubsub.yaml")
    parser.add_argument("--acl", help="ACL Configuration file to load (default: pypubsub_acl.yaml)",
                        default="pypubsub_acl.yaml")
    cliargs = parser.parse_args()
    pubsub_server = Server(cliargs)
    pubsub_server.run()
