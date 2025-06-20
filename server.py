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
    def __init__(self, key, value, client_id=None, req_count=None, shard=0, server_id=0):
        self.key = key
        self.value = value
        self.client_id = client_id
        self.req_count = req_count
        self.shard = shard
        self.server_id = server_id

class PutAppendReply:
    # Add definitions here if needed
    def __init__(self, value, redirect=None):
        self.value = value
        self.redirect = redirect

class GetArgs:
    # Add definitions here if needed
    def __init__(self, key, client_id=None, req_count=None, shard=0, server_id=0):
        self.key = key
        self.client_id = client_id
        self.req_count = req_count
        self.shard = shard
        self.server_id = server_id

class GetReply:
    # Add definitions here if needed
    def __init__(self, value):
        self.value = value

class KVServer:
    def __init__(self, cfg):
        self.mu = threading.Lock()
        self.cfg = cfg
        self.kv = dict()
        self.client_req = {}
        self.total_servers = cfg.nservers

    def Get(self, args: GetArgs):
        reply = GetReply(None)
        with self.mu:
            reply.value = self.kv.get(args.key, None)
        return reply

    def Put(self, args: PutAppendArgs):
        reply = PutAppendReply(None)
        if args.shard != args.server_id:
            reply.redirect = args.shard
            return reply

        last_req = self.client_req.get(args.client_id)
        # print(last_req)
        if last_req is not None and last_req[0] >= args.req_count:
            print("Already completed PUT")

        self.kv[args.key] = args.value
        self.client_req[args.client_id] = (args.req_count, "")

    def Append(self, args: PutAppendArgs):
        reply = PutAppendReply(None)
        if args.shard != args.server_id:
            # redirect client to leader
            reply.redirect = args.shard
            return reply

        last_req = self.client_req.get(args.client_id)
        # print(last_req)
        if last_req is not None and last_req[0] >= args.req_count:
            print("Already completed APPEND")
            return last_req[1]

        prev = self.kv.get(args.key, "")
        self.kv[args.key] = prev + args.value
        self.client_req[args.client_id] = (args.req_count, prev)
        reply.value = prev
        return reply.value
