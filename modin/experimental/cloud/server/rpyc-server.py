#!/usr/bin/env python

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

"""
Run classic RPyC server with some patches applied.

Usage:
    rpyc-server.py [--port 12345] [--logfile path/to/file.log]
"""

import sys
import os
import argparse

import rpyc
from rpyc.utils.server import ThreadedServer
from rpyc.utils.classic import DEFAULT_SERVER_PORT
from rpyc.utils.authenticators import SSLAuthenticator
from rpyc.lib import setup_logger
from rpyc.core import SlaveService

sys.path.append(os.path.dirname(__file__))
from patches import patch


def run_server(port: int, logfile: str = None):
    host = "127.0.0.1"  # do not allow non-localhost connections for security

    setup_logger(bool(logfile), logfile)

    server = ThreadedServer(
        SlaveService,
        hostname=host,
        port=port,
        reuse_addr=True,
        ipv6=False,
        authenticator=None,
        registrar=None,
        auto_register=None,
        protocol_config={"sync_request_timeout": 2400},
    )
    server.start()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--port",
        type=int,
        default=DEFAULT_SERVER_PORT,
        required=False,
        help="Specify port to bind to",
    )
    parser.add_argument(
        "--logfile", type=str, default="", required=False, help="Where to store logs"
    )
    args = parser.parse_args()
    patch()
    run_server(args.port, args.logfile)


if __name__ == "__main__":
    main()
