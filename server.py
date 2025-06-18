import logging
import threading
from typing import Tuple, Any

debugging = False

# Use this function for debugging
def debug(format, *args):
    if debugging:
        logging.info(format % args)

# Put or Append
class PutAppendArgs:
    # Add definitions here if needed
    def __init__(self, key, value, client_id=None, req_id=None):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.req_id = req_id

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key, client_id=None, req_id=None):
        self.key = key
        self.client_id = client_id
        self.req_id = req_id

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.kv = dict()
        self.client_req = dict()

    def Get(self, args: GetArgs):
        reply = GetReply(None)
        with self.mu:
            reply.value = self.kv.get(args.key, None)
        return reply

    def Put(self, args: PutAppendArgs):
        with self.mu:
            self.kv[args.key] = args.value

    def Append(self, args: PutAppendArgs):
        with self.mu:
            previous = self.kv[args.key]
            self.kv[args.key] += args.value
            return previous
