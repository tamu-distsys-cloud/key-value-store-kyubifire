import random
import threading
from typing import Any, List

from labrpc.labrpc import ClientEnd
from server import GetArgs, GetReply, PutAppendArgs, PutAppendReply

def nrand() -> int:
    return random.getrandbits(62)

class Clerk:
    def __init__(self, servers: List[ClientEnd], cfg):
        self.servers = servers
        self.cfg = cfg
        self.request_id = 0
        self.client_id = nrand()
        #self.nshards = cfg.nshards

    # def get_shard(self, key: str) -> int:
    #     return int(key) % self.nshards

    # Fetch the current value for a key.
    # Returns "" if the key does not exist.
    # Keeps trying forever in the face of all other errors.
    #
    # You can send an RPC with code like this:
    # reply = self.server[i].call("KVServer.Get", args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def get(self, key: str) -> str:
        self.request_id += 1
        args = GetArgs(key, self.client_id, self.request_id)
        # shard = self.get_shard(key)
        while True:
            for server in self.servers:
                try:
                    reply = server.call("KVServer.Get", args)
                    if reply is not None:
                        return reply.value
                    else:
                        return ""
                except TimeoutError:
                    continue

    # Shared by Put and Append.
    #
    # You can send an RPC with code like this:
    # reply = self.servers[i].call("KVServer."+op, args)
    # assuming that you are connecting to the i-th server.
    #
    # The types of args and reply (including whether they are pointers)
    # must match the declared types of the RPC handler function's
    # arguments in server.py.
    def put_append(self, key: str, value: str, op: str) -> str:
        self.request_id += 1
        args = PutAppendArgs(key, value, self.client_id, self.request_id)
        while True:
            for server in self.servers:
                try:
                    reply = server.call("KVServer." + op, args)
                    if op == "Append":
                        return reply
                    else:
                        return ""
                except TimeoutError:
                    continue

    def put(self, key: str, value: str):
        self.put_append(key, value, "Put")

    # Append value to key's value and return that value
    def append(self, key: str, value: str) -> str:
        return self.put_append(key, value, "Append")
