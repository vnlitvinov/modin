# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

from rpyc.core import netref

_READONLY_ATTRS = {"__bases__", "__base__", "__mro__"}
orig_getattribute = netref.BaseNetref.__getattribute__


def __getattribute__(self, name):
    if name in _READONLY_ATTRS:
        try:
            cache = type.__getattribute__(type(self), "__readonly_cache__")
        except AttributeError:
            cache = {}
            type.__setattr__(type(self), "__readonly_cache__", cache)
        try:
            return cache[name]
        except KeyError:
            res = cache[name] = orig_getattribute(self, name)
            return res
    return orig_getattribute(self, name)


def patch():
    netref.BaseNetref.__getattribute__ = __getattribute__
