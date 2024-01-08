from dataclasses import dataclass
from typing import Any
import httpx
import socket
import json

# log_nodes_ip_list = ['192.168.0.2', '192.168.0.3']
log_nodes_ip_list = ["192.168.0.101:8080", "192.168.0.102:8000"]
keys_dict = {}
step_id = 0


def ip_to_int(ip: str):
    b = socket.inet_aton(ip)
    return int.from_bytes(b, byteorder="big")


def int_to_ip(ip: int):
    return socket.inet_ntoa(ip)


def hash_func(ip: str):
    # num = ip_to_int(ip)
    return hash(ip) % len(log_nodes_ip_list)


client = httpx.Client()


@dataclass
class ResType:
    status: int
    data: int
    message: str = " "


# ip: 数据库的地址
def read(ip: str, key: str, SSF_id: int) -> ResType:
    log_ip = log_nodes_ip_list[hash_func(ip)]
    global step_id
    # global client
    resp = client.post(
        f"http://{log_ip}/read",
        json={
            "db_address": ip,
            "key": key,
            "ssf_id": SSF_id,
            "step_id": step_id,
            "version": 1,
        },
    )
    # return orjson.loads(resp.content)
    return ResType(**json.loads(resp.content))


def write(ip: str, key: str, value: int | str, SSF_id: int) -> ResType:
    log_ip = log_nodes_ip_list[hash_func(ip)]
    global step_id
    step_id += 1
    resp = client.post(
        f"http://{log_ip}/write",
        json={
            "db_address": ip,
            "key": key,
            "value": value,
            "ssf_id": SSF_id,
            "step_id": step_id,
            "version": 1,
        },
    )
    return json.loads(resp.content)


def exit(ip: str, SSF_id: int):
    log_ip = log_nodes_ip_list[hash_func(ip)]
    client.post(
        f"http://{log_ip}/clear",
        json={
            "ssf_id": SSF_id,
        },
    )
    client.close()


if __name__ == "__main__":
    resp = read("192.168.0.1", "height", 1)
    print(resp)
    # resp = write("192.168.0.1", "height", 1, 1, 1)
    # print(resp)
