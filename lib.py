from dataclasses import dataclass
import httpx
import json
from cohash import ConsistentHash

# log_nodes_ip_list = ['192.168.0.2', '192.168.0.3']


@dataclass
class ResType:
    status: int
    data: int
    message: str = " "


class RespException(Exception):
    pass


class LogClient:
    def __init__(self) -> None:
        self.log_nodes_ip_list = ["192.168.0.101:8080", "192.168.0.102:8080"]
        self.step_id_dict: dict[str, dict[str, int]] = {}
        self.con_hash = ConsistentHash(self.log_nodes_ip_list)
        self.client = httpx.Client()

    # ip: 数据库的地址
    def read(self, ip: str, key: str, SSF_id: int, err_num: int = 3) -> ResType:
        step_id = 1
        log_ip = self.con_hash.get_node(ip)
        if ip in self.step_id_dict:
            if key in self.step_id_dict[ip]:
                step_id = self.step_id_dict[ip][key]
            else:
                self.step_id_dict[ip][key] = 1
        else:
            self.step_id_dict[ip] = {key: 1}
        # global client

        for i in range(err_num):
            try:
                resp = self.client.post(
                    f"http://{log_ip}/read",
                    params={
                        "db_address": ip,
                        "key": key,
                        "ssf_id": SSF_id,
                        "step_id": step_id,
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
        self, ip: str, key: str, value: int | str, SSF_id: int, err_num: int = 3
    ) -> ResType:
        log_ip = self.con_hash.get_node(ip)

        step_id = 1
        if ip in self.step_id_dict:
            if key in self.step_id_dict[ip]:
                step_id = self.step_id_dict[ip][key]
            else:
                self.step_id_dict[ip][key] = 1
        else:
            self.step_id_dict[ip] = {key: 1}

        for i in range(err_num):
            try:
                resp = self.client.post(
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

                self.step_id_dict[ip][key] += 1
                return resp

            except RespException as e:
                raise e

            except Exception as e:
                if i == err_num - 1:
                    raise e

    def clear(self, ip: str, SSF_id: int, err_num: int = 3):
        log_ip = self.con_hash.get_node(ip)

        for i in range(err_num):
            try:
                resp = self.client.post(
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

    def restart(self):
        self.step_id_dict = {}

    def exit(self):
        self.client.close()


if __name__ == "__main__":
    # resp = write("192.168.0.103:6379", "height", 1)
    # print(resp)
    # resp = write("192.168.0.1", "height", 1, 1, 1)
    # print(resp)
    # resp = client.post(
    #     f"http://192.168.0.101:8080/write",
    #     parasm={
    #         "db_address": "192.168.0.103:6379",
    #         "key": "a",
    #         "value": 1,
    #         "ssf_id": 1,
    #         "step_id": 1,
    #         "version": 1,
    #     },
    # )
    # exit("192.168.0.104:6379", 10)
    # client.close()
    pass
