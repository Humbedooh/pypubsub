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

""" This is the LDAP component of PyPubSub """

import ldap
import asyncio


class LDAPConnection:
    def __init__(self, yml: dict):
        self.uri = yml.get('uri')
        assert isinstance(self.uri, str), "LDAP URI must be a string."

        self.user = yml.get('user_dn')
        assert isinstance(self.user, str) or self.user is None, "LDAP user DN must be a string or absent."

        self.base = yml.get('base_scope')
        assert isinstance(self.base, str), "LDAP Base scope must be a string"

        self.patterns: list = yml.get('membership_patterns', [])
        assert isinstance(self.patterns, list), "LDAP membership patterns must be a list of pattern strings"

        self.acl = yml.get('acl')
        assert isinstance(self.acl, dict), "LDAP ACL must be a dictionary (hash) of ACLs"

        assert ldap.initialize(self.uri)
        print("==== LDAP configuration looks kosher, enabling LDAP authentication as fallback ====")

    async def get_groups(self, user: str, password: str):
        """Async fetching of groups an LDAP user belongs to"""
        bind_dn = self.user % user  # Interpolate user DN with username

        try:
            client = ldap.initialize(self.uri)
            client.set_option(ldap.OPT_REFERRALS, 0)
            client.set_option(ldap.OPT_TIMEOUT, 0)
            rv = client.simple_bind(bind_dn, password)
            while True:
                res = client.result(rv, timeout=0)
                if res and res != (None, None):
                    break
                await asyncio.sleep(0.25)

            groups = []
            for role in self.patterns:
                rv = client.search(self.base, ldap.SCOPE_SUBTREE, role % user, ['dn'])
                while True:
                    res = client.result(rv, all=0, timeout=0)
                    if res:
                        if res == (None, None):
                            await asyncio.sleep(0.25)
                        else:
                            if not res[1]:
                                break
                            for tuples in res[1]:
                                groups.append(tuples[0])
                    else:
                        break
            return groups

        except Exception as e:
            print(f"LDAP Exception for user {user}: {e}")
            return []

