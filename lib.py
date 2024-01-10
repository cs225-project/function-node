from dataclasses import dataclass
from typing import Any
import httpx
import socket
import json

# log_nodes_ip_list = ['192.168.0.2', '192.168.0.3']
log_nodes_ip_list = ["192.168.0.101:8080", "192.168.0.102:8080"]
step_id_dict: dict[str, dict[str, int]] = {}
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


class RespException(Exception):
    pass


# ip: 数据库的地址
def read(ip: str, key: str, SSF_id: int, err_num: int = 3) -> ResType:
    step_id = 1
    log_ip = log_nodes_ip_list[hash_func(ip)]
    global step_id_dict
    if ip in step_id_dict:
        if key in step_id_dict[ip]:
            step_id = step_id_dict[ip][key]
        else:
            step_id_dict[ip][key] = 1
    else:
        step_id_dict[ip] = {key: 1}
    # global client

    for i in range(err_num):
        try:
            resp = client.post(
                f"http://{log_ip}/read",
                params={
                    "db_address": ip,
                    "key": key,
                    "ssf_id": SSF_id,
                    "step_id": step_id,
                    "version": 1,
                },
                timeout=None,
            )
            resp = ResType(**json.loads(resp.content))
            if resp.status != 1:
                raise RespException(resp.message)

            return resp

        except RespException as e:
            raise e

        except Exception as e:
            if i == err_num - 1:
                raise e


def write(
    ip: str, key: str, value: int | str, SSF_id: int, err_num: int = 3
) -> ResType:
    log_ip = log_nodes_ip_list[hash_func(ip)]

    step_id = 1
    global step_id_dict
    if ip in step_id_dict:
        if key in step_id_dict[ip]:
            step_id = step_id_dict[ip][key]
        else:
            step_id_dict[ip][key] = 1
    else:
        step_id_dict[ip] = {key: 1}

    for i in range(err_num):
        try:
            resp = client.post(
                f"http://{log_ip}/write",
                params={
                    "db_address": ip,
                    "key": key,
                    "value": value,
                    "ssf_id": SSF_id,
                    "step_id": step_id,
                    "version": 1,
                },
                timeout=None,
            )
            resp = ResType(**json.loads(resp.content))
            if resp.status != 1:
                raise RespException(resp.message)

            step_id_dict[ip][key] += 1
            return resp

        except RespException as e:
            raise e

        except Exception as e:
            if i == err_num - 1:
                raise e


def exit(ip: str, SSF_id: int, err_num: int = 3):
    log_ip = log_nodes_ip_list[hash_func(ip)]

    for i in range(err_num):
        try:
            resp = client.post(
                f"http://{log_ip}/clear",
                params={
                    "ssf_id": SSF_id,
                },
            )
            resp = ResType(**json.loads(resp.content))
            if resp.status != 1:
                raise RespException(resp.message)

            return

        except RespException as e:
            raise e

        except Exception as e:
            if i == err_num - 1:
                raise e

    # client.close()


def restart():
    global step_id_dict
    step_id_dict = {}


if __name__ == "__main__":
    # resp = write("192.168.0.103:6379", "height", 1)
    # print(resp)
    # resp = write("192.168.0.1", "height", 1, 1, 1)
    # print(resp)
    resp = client.post(
        f"http://192.168.0.101:8080/write",
        parasm={
            "db_address": "192.168.0.103:6379",
            "key": "a",
            "value": 1,
            "ssf_id": 1,
            "step_id": 1,
            "version": 1,
        },
    )
    exit("192.168.0.104:6379", 10)
    client.close()
