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
import aiofile
import os
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
import typing

# Some consts
PUBSUB_VERSION = '0.7.2'
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
PUBSUB_WRITE_TIMEOUT = 0.35  # If we can't deliver to a pipe within N seconds, drop it.


class ServerConfig(typing.NamedTuple):
    ip: str
    port: int
    payload_limit: int


class BacklogConfig(typing.NamedTuple):
    max_age: int
    queue_size: int
    storage: typing.Optional[str]


class Configuration:
    server: ServerConfig
    backlog: BacklogConfig
    payloaders: typing.List[netaddr.ip.IPNetwork]
    oldschoolers: typing.List[str]
    secure_topics: typing.Optional[typing.List[str]]

    def __init__(self, yml: dict):

        # LDAP Settings
        self.ldap = None
        lyml = yml.get('clients', {}).get('ldap')
        if isinstance(lyml, dict):
            self.ldap = plugins.ldap.LDAPConnection(lyml)

        # SQS?
        self.sqs = yml.get('sqs')

        # Main server config
        server_ip = yml['server'].get('bind', PUBSUB_DEFAULT_IP)
        server_port = int(yml['server'].get('port', PUBSUB_DEFAULT_PORT))
        server_payload_limit = int(yml['server'].get('max_payload_size', PUBSUB_DEFAULT_MAX_PAYLOAD_SIZE))
        self.server = ServerConfig(ip=server_ip, port=server_port, payload_limit=server_payload_limit)

        # Backlog settings
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
        bqs = yml['server'].get('backlog', {}).get('size',
                                                   PUBSUB_DEFAULT_BACKLOG_SIZE)
        bst = yml['server'].get('backlog', {}).get('storage')
        self.backlog = BacklogConfig(max_age=bma, queue_size=bqs, storage=bst)

        # Payloaders - clients that can post payloads
        self.payloaders = [netaddr.IPNetwork(x) for x in yml['clients'].get('payloaders', [])]

        # Binary backwards compatibility
        self.oldschoolers = yml['clients'].get('oldschoolers', [])

        # Secure topics, if any
        self.secure_topics = set(yml['clients'].get('secure_topics', []) or [])


class Server:
    """Main server class, responsible for handling requests and publishing events """
    yaml: dict
    config: Configuration
    subscribers: list
    pending_events: asyncio.Queue
    backlog: list
    last_ping = typing.Type[float]
    server: aiohttp.web.Server

    def __init__(self, args: argparse.Namespace):
        self.yaml = yaml.safe_load(open(args.config))
        self.config = Configuration(self.yaml)
        self.subscribers = []
        self.pending_events = asyncio.Queue()
        self.backlog = []
        self.last_ping = time.time()
        self.acl = {}
        try:
            self.acl = yaml.safe_load(open(args.acl))
        except FileNotFoundError:
            print(f"ACL configuration file {args.acl} not found, private events will not be broadcast.")

    async def poll(self):
        """Polls for new stuff to publish, and if found, publishes to whomever wants it."""
        while True:
            payload: Payload = await self.pending_events.get()
            bad_subs: list = await payload.publish(self.subscribers)
            self.pending_events.task_done()

            # Cull subscribers we couldn't deliver payload to.
            for bad_sub in bad_subs:
                print("Culling %r due to connection errors" % bad_sub)
                try:
                    self.subscribers.remove(bad_sub)
                except ValueError:  # Already removed elsewhere
                    pass

    async def handle_request(self, request: aiohttp.web.BaseRequest):
        """Generic handler for all incoming HTTP requests"""
        resp: typing.Union[aiohttp.web.Response, aiohttp.web.StreamResponse]

        # Define response headers first...
        headers = {
            'Server': 'PyPubSub/%s' % PUBSUB_VERSION,
            'X-Subscribers': str(len(self.subscribers)),
            'X-Requests': str(self.server.requests_count),
        }

        subscriber = Subscriber(self, request)
        # Is there a basic auth in this request? If so, set up ACL
        auth = request.headers.get('Authorization')
        if auth:
            await subscriber.parse_acl(auth)

        # Are we handling a publisher payload request? (PUT/POST)
        if request.method in ['PUT', 'POST']:
            ip = netaddr.IPAddress(request.remote)
            allowed = False
            for network in self.config.payloaders:
                if ip in network:
                    allowed = True
                    break
            # Check for secure topics
            payload_topics = set(request.path.split("/"))
            if any(x in self.config.secure_topics for x in payload_topics):
                allowed = False
                # Figure out which secure topics we need permission for:
                which_secure = [x for x in self.config.secure_topics if x in payload_topics]
                # Is the user allowed to post to all of these secure topics?
                if subscriber.secure_topics and all(x in subscriber.secure_topics for x in which_secure):
                    allowed = True

            if not allowed:
                resp = aiohttp.web.Response(headers=headers, status=403, text=PUBSUB_NOT_ALLOWED)
                return resp
            if request.can_read_body:
                try:
                    if request.content_length and request.content_length > self.config.server.payload_limit:
                        resp = aiohttp.web.Response(headers=headers, status=400, text=PUBSUB_PAYLOAD_TOO_LARGE)
                        return resp
                    body = await request.text()
                    as_json = json.loads(body)
                    assert isinstance(as_json, dict)  # Payload MUST be an dictionary object, {...}
                    pl = Payload(request.path, as_json)
                    self.pending_events.put_nowait(pl)
                    # Add to backlog?
                    if self.config.backlog.queue_size > 0:
                        self.backlog.append(pl)
                        # If backlog has grown too large, delete the first (oldest) item in it.
                        while len(self.backlog) > self.config.backlog.queue_size:
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

            # Subscribe the user before we deal with the potential backlog request and pings
            subscriber.connection = resp
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
                        backlog_ts = max(backlog_ts, int(time.time() - self.config.backlog.max_age))
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

    async def write_backlog_storage(self):
        previous_backlog = []
        while True:
            if self.config.backlog.storage:
                try:
                    backlog_list = self.backlog.copy()
                    if backlog_list != previous_backlog:
                        previous_backlog = backlog_list
                        async with aiofile.AIOFile(self.config.backlog.storage, 'w+') as afp:
                            offset = 0
                            for item in backlog_list:
                                js =json.dumps({
                                    'timestamp': item.timestamp,
                                    'topics': item.topics,
                                    'json': item.json,
                                    'private': item.private
                                }) + '\n'
                                await afp.write(js, offset=offset)
                                offset += len(js)
                            await afp.fsync()
                except Exception as e:
                    print(f"Could not write to backlog file {self.config.backlog.storage}: {e}")
            await asyncio.sleep(10)

    def read_backlog_storage(self):
        if self.config.backlog.storage and os.path.exists(self.config.backlog.storage):
            try:
                readlines = 0
                with open(self.config.backlog.storage, 'r') as fp:
                    for line in fp.readlines():
                        js = json.loads(line)
                        readlines += 1
                        ppath = "/".join(js['topics'])
                        if js['private']:
                            ppath = '/private/' + ppath
                        payload = Payload(ppath, js['json'], js['timestamp'])
                        self.backlog.append(payload)
                        if self.config.backlog.queue_size < len(self.backlog):
                            self.backlog.pop(0)
            except Exception as e:
                print(f"Error while reading backlog: {e}")

            print(f"Read {readlines} objects from {self.config.backlog.storage}, applied {len(self.backlog)} to backlog.")

    async def server_loop(self, loop: asyncio.BaseEventLoop):
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
        self.read_backlog_storage()
        loop.create_task(self.write_backlog_storage())
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
    acl:    dict
    topics: typing.List[typing.List[str]]

    def __init__(self, server: Server, request: aiohttp.web.BaseRequest):
        self.connection: typing.Optional[aiohttp.web.StreamResponse] = None
        self.acl = {}
        self.server = server
        self.lock = asyncio.Lock()
        self.secure_topics = []

        # Set topics subscribed to
        self.topics = []
        for topic_batch in request.path.split(','):
            sub_to = [x for x in topic_batch.split('/') if x]
            self.topics.append(sub_to)

        # Is the client old and expecting zero-terminators?
        self.old_school = False
        for ua in self.server.config.oldschoolers:
            if ua in request.headers.get('User-Agent', ''):
                self.old_school = True
                break

    async def parse_acl(self, basic: str):
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
                    self.acl = acl
                    self.secure_topics = set(self.server.acl[u].get('topics', []) or [])
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
                self.acl = acl

        except binascii.Error as e:
            pass  # Bad Basic Auth params, bail quietly
        except AssertionError as e:
            print(e)
            print(f"ACL configuration error: ACL scheme for {u} contains errors, setting ACL to nothing.")
        except Exception as e:
            print(f"Basic unknown exception occurred: {e}")
        

    async def ping(self):
        """Generic ping-back to the client"""
        js = b"%s\n" % json.dumps({"stillalive": time.time()}).encode('utf-8')
        if self.old_school:
            js += b"\0"
        async with self.lock:
            await asyncio.wait_for(self.connection.write(js), timeout=PUBSUB_WRITE_TIMEOUT)


class Payload:
    """A payload (event) object sent by a registered publisher."""

    def __init__(self, path: str, data: dict, timestamp: typing.Optional[float] = None):
        self.json = data
        self.timestamp = timestamp or time.time()
        self.topics = [x for x in path.split('/') if x]
        self.private = False

        # Private payload?
        if self.topics and self.topics[0] == 'private':
            self.private = True
            del self.topics[0]  # Remove the private bit from topics now.

        self.json['pubsub_timestamp'] = self.timestamp
        self.json['pubsub_topics'] = self.topics
        self.json['pubsub_path'] = path

    async def publish(self, subscribers: typing.List[Subscriber]):
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
            for topic_batch in sub.topics:
                if all(el in self.topics for el in topic_batch):
                    try:
                        if sub.old_school:
                            async with sub.lock:
                                await asyncio.wait_for(sub.connection.write(ojs), timeout=PUBSUB_WRITE_TIMEOUT)
                        else:
                            async with sub.lock:
                                await asyncio.wait_for(sub.connection.write(js), timeout=PUBSUB_WRITE_TIMEOUT)
                    except Exception:
                        bad_subs.append(sub)
                    break
        return bad_subs


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", help="Configuration file to load (default: pypubsub.yaml)", default="pypubsub.yaml")
    parser.add_argument("--acl", help="ACL Configuration file to load (default: pypubsub_acl.yaml)",
                        default="pypubsub_acl.yaml")
    cliargs = parser.parse_args()
    pubsub_server = Server(cliargs)
    pubsub_server.run()
