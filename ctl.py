#!/usr/bin/env python3
"""Tiny helper to send one JSON command to a running run_node.py's control
channel and print the reply. See test_dht.md section 3 for the command
reference (store/get/find_node/dump_routing_table/dump_store/shutdown/ping).

Usage: python ctl.py <control_port> '<json command>'
Example: python ctl.py 9001 '{"cmd": "node_id"}'
"""
import json
import socket
import sys

port = int(sys.argv[1])
request = json.loads(sys.argv[2])

with socket.create_connection(("127.0.0.1", port), timeout=10) as sock:
    sock.sendall((json.dumps(request) + "\n").encode())
    print(sock.recv(65536).decode().strip())
