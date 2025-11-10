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

""" This is the SQS component of PyPubSub """

import asyncio
import aiobotocore
import botocore.exceptions
import json
import typing

# Avoid circular import by only importing pypubsub for type checking
if typing.TYPE_CHECKING:
    import pypubsub

# Global to hold ID of all items seem across all queues, to dedup things.
ITEMS_SEEN: typing.List[str] = []


async def get_payloads(server: 'pypubsub.Server', config: dict):
    # Import at runtime to avoid circular import
    import pypubsub

    # Assume everything is configured in the client's .aws config
    session = aiobotocore.get_session()
    queue_name = config.get('queue', '???')
    while True:
        async with session.create_client('sqs',
                                         aws_secret_access_key=config.get('secret'),
                                         aws_access_key_id=config.get('key'),
                                         region_name=config.get('region', 'default')
                                         ) as client:
            try:
                response = await client.get_queue_url(QueueName=queue_name)
            except botocore.exceptions.ClientError as err:
                if err.response['Error']['Code'] == \
                        'AWS.SimpleQueueService.NonExistentQueue':
                    print(f"SQS item {queue_name} does not exist, bailing!")
                    return
                else:
                    raise
            queue_url = response['QueueUrl']
            print(f"Connected to SQS {queue_url}, reading stream...")
            while True:
                try:
                    response = await client.receive_message(
                        QueueUrl=queue_url,
                        WaitTimeSeconds=3,
                    )

                    if 'Messages' in response:
                        for msg in response['Messages']:
                            body = msg.get('Body', '{}')
                            mid = msg.get('MessageId', '')
                            try:
                                # If we already logged this one, but couldn't delete - skip payload construction,
                                # but do try to remove it again...
                                if mid not in ITEMS_SEEN:
                                    js = json.loads(body)
                                    path = js.get('pubsub_path', '/')  # Default to catch-all pubsub topic
                                    payload = pypubsub.Payload(path, js)
                                    server.pending_events.put_nowait(payload)
                                    backlog_size = server.config.backlog.queue_size
                                    if backlog_size > 0:
                                        server.backlog.append(payload)
                            except ValueError as e:
                                print(f"Could not parse payload from SQS: {e}")
                            # Do we delete messages or keep them?
                            if config.get('delete'):
                                try:
                                    await client.delete_message(
                                        QueueUrl=queue_url,
                                        ReceiptHandle=msg['ReceiptHandle']
                                    )
                                    if mid in ITEMS_SEEN:
                                        ITEMS_SEEN.remove(mid) # Remove if found and now deleted
                                except Exception as e:
                                    if mid not in ITEMS_SEEN:
                                        print(f"Could not remove item from SQS, marking as potential later duplicate!")
                                        print(e)
                                        ITEMS_SEEN.append(mid)
                            else:  # dedup nonetheless
                                ITEMS_SEEN.append(mid)
                except KeyboardInterrupt:
                    return
                except botocore.exceptions.ClientError as e:
                    print(f"Could not receive message(s) from SQS, bouncing connection:")
                    print(e)
                    break
            await asyncio.sleep(10)  # Sleep for 10 before bouncing SQS connection so as to not retry too often.
