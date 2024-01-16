import lib
import socket
from multiprocessing import Process, Barrier
from multiprocessing.synchronize import Barrier as Barrier_
import random
from dataclasses import dataclass
import time


def chooseRW(i: int, total: int, ratio: float = 0.3):
    if i <= int(total * ratio):
        return "r"
    return "w"


def remove_dots_to_int(ip: str) -> int:
    parts = ip.split(".")
    result = "".join(part.replace(".", "") for part in parts)
    return int(result)


@dataclass
class CmdItem:
    op: str
    ip: str
    key: str
    val: int


@dataclass
class Result:
    key: str
    val: int


class FUNC(Process):
    def __init__(
        self, id: int, prefix: int, start_barrier: Barrier_, end_barrier: Barrier_
    ):
        self.id = id
        self.prefix = prefix
        self.start_barrier = start_barrier
        self.end_barrier = end_barrier
        self.ip_list: list[str] = ["192.168.0.103:6379"]
        # self.ip_list: list[str] = [
        #     "192.168.0.103:6379",
        #     "192.168.0.104:6379",
        #     "192.168.0.105:6379",
        #     "192.168.0.106:6379",
        # ]

        self.result: list[Result] = []
        self.retry_result: list[Result] = []
        self.flag = False  # 表示是否重试了

        self.key_list: list[str] = []
        self.cmd_list: list[CmdItem] = []
        super().__init__()

    def run(self):
        # 只能在run中获取id, 因为此事ident才有值
        self.ssf_id = int(str(self.prefix) + str(self.ident))
        self.gen_test_cmds()
        self.client = lib.LogClient()
        # self.start_barrier.wait()
        try:
            time1 = time.time()
            self.run_test()
            time2 = time.time()
            print(time2 - time1)
        except Exception as e:
            print(e)
            if not self.flag:
                self.flag = True
                self.client.restart()
                self.run_test()

        finally:
            self.exit_func()
            self.dump_result()

    def gen_test_cmds(self):
        key_list_temp = [chr(i) for i in range(ord("a"), ord("z") + 1)]

        # for i in range(len(key_list_temp)):
        #     for j in range(i + 1, len(key_list_temp)):
        #         self.key_list.append(key_list_temp[i] + key_list_temp[j])

        self.key_list = random.sample(key_list_temp, 10)
        # self.key_list = random.sample(self.key_list, 10)
        self.cmd_list = [
            CmdItem("w", random.choice(self.ip_list), k, 1) for k in self.key_list
        ]  # 先执行wrtie，保证一定有这些key

        total = 120
        temp: list[CmdItem] = []
        for cmd in self.cmd_list:
            for i in range(total):
                temp.append(
                    CmdItem(
                        chooseRW(i, 10 * total, 0.75),
                        cmd.ip,
                        cmd.key,
                        random.randint(1, 100),
                    )
                )

        random.shuffle(temp)
        self.cmd_list.extend(temp)
        self.cmd_list = self.cmd_list

    def run_test(self):
        # print(f"test len {len(self.cmd_list)}")
        for i, cmd in enumerate(self.cmd_list):
            # if (not self.flag) and (i == len(self.cmd_list) * 1 / 2):
            #     raise Exception()

            match cmd.op:
                case "w":
                    self.write_func(cmd.ip, cmd.key, cmd.val)
                case "r":
                    res = self.read_func(cmd.ip, cmd.key)
                    if not self.flag:
                        self.result.append(Result(cmd.key, res.data))
                    else:
                        self.retry_result.append(Result(cmd.key, res.data))

    def dump_result(self):
        print(f"dump result: {len(self.result)}")
        print(f"dump retry_result: {len(self.retry_result)}")
        with open(f"./{self.id}.result", "w") as f:
            for item in self.result:
                f.write(f"key: {item.key}, data:{item.val}" + "\n")

        if len(self.retry_result) != 0:
            with open(f"./{self.id}.retry_result", "w") as f:
                for item in self.retry_result:
                    f.write(f"key: {item.key}, data:{item.val}" + "\n")

    def read_func(self, ip: str, key: str):
        res = self.client.read(ip, key, self.ssf_id)
        return res

    def write_func(self, ip: str, key: str, value: int | str):
        res = self.client.write(ip, key, value, self.ssf_id)
        return res

    def clear_func(self):
        ip_set = set(map(lambda x: x.ip, self.cmd_list))
        for ip in ip_set:
            self.client.clear(ip, self.ssf_id)

    def exit_func(self):
        self.clear_func()
        self.client.exit()
        return


if __name__ == "__main__":
    start_barrier = Barrier(2)
    end_barrier = Barrier(2)
    res = remove_dots_to_int(socket.gethostbyname(socket.gethostname()))
    num = 2
    funcs_list: list[FUNC] = [
        FUNC(i, res, start_barrier, end_barrier) for i in range(num)
    ]

    for i in range(num):
        funcs_list[i].start()

    for i in range(num):
        funcs_list[i].join()
