# 0.7.2
- Addessed an issue with SQS not updating in real-time, only when backlog is requested.
- Added secure topics feature for locking publishing of certain pubsub topics to the ACL. 

# 0.7.1
- Use asyncio queues for modifying the list of events pending publishing to avoid potential race conditions.
- Minor tweak to the handling of backlog size   
- Improved type checks
- Updated client examples

# 0.7.0
- Allow for multiple subscriptions per stream

# 0.6.3
- Fixed an issue with payload delivery stalling due to client pipe timeouts

# 0.6.2
- Fixed a configuration issue with SQS storage

# 0.6.1
- Added FS-backed persistant backlog storage for persisting backlog through restarts
- Addressed issues with aiohttp pipe writes not being coroutine-safe

# 0.6.0
- Reworked configuration structure

# 0.5.1
- Fixed an issue where the SQS dedup list would try to remove non-existent elements
- Added support for AWS credential inside the config yaml

# 0.5.0
- Added SQS support for weaving in items from AWS SQS

# 0.4.6
- Changed content type to better reflect that this is a custom stream
- Switched to internal counter for number of requests served
- Added max payload size setting
- Fixed an issue where payloads with no topics would cause an indexing error
- Added a backlog for clients wishing to retrieve earlier payloads (if enabled)

# 0.4.5
- Better handling of errored subscriber connections

# 0.4.4
- Fixed a typo in the configuration retrieval for binary clients.

# 0.4.3
- Changed LDAP ACL structure to allow for multiple ACL definitions
  per LDAP group.

# 0.4.2
- Changed plugin structure

# 0.4.1
- Added CLI arguments for specifying configuration file paths.

# 0.4.0
- Various internal optimizations
- Better configuration vetting
- Added asynchronous LDAP ACL support (see README.md for details)

# 0.3.0
- Initial public release.
