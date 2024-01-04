import httpx
import socket
import orjson

# log_nodes_ip_list = ['192.168.0.2', '192.168.0.3']
log_nodes_ip_list = ["127.0.0.1:8000", "127.0.0.1:8000"]
keys_dict = {}
step_id = 0


def ip_to_int(ip: str):
    b = socket.inet_aton(ip)
    return int.from_bytes(b, byteorder="big")


def int_to_ip(ip: int):
    return socket.inet_ntoa(ip)


def hash_func(ip: str):
    # num = ip_to_int(ip)
    return hash(ip) % 2


def read(ip: str, key: str, SSF_id: int):
    log_ip = log_nodes_ip_list[hash_func(ip)]
    global step_id
    step_id += 1
    resp = httpx.post(
        f"http://{log_ip}/read",
        json={
            "ip": ip,
            "key": key,
            "ssf_id": SSF_id,
            "step_id": step_id,
            "version": 1,
        },
    )

    return orjson.loads(resp.content)


def write(ip: str, key: str, value: any, SSF_id: int | None = None):
    log_ip = log_nodes_ip_list[hash_func(ip)]
    global step_id
    step_id += 1
    resp = httpx.post(
        f"http://{log_ip}/write",
        json={
            "ip": ip,
            "key": key,
            "value": value,
            "ssf_id": SSF_id,
            "step_id": step_id,
            "version": 1,
        },
    )
    return orjson.loads(resp.content)


if __name__ == "__main__":
    resp = read("192.168.0.1", "height", 1, 1)
    print(resp)
    resp = write("192.168.0.1", "height", 1, 1, 1)
    print(resp)
