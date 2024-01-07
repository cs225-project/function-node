import lib
import socket
from multiprocessing import Process, Barrier
from multiprocessing.synchronize import Barrier as Barrier_
import random
from dataclasses import dataclass


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
        self.ip_list: list[str] = ["192.168.0.1", "192.168.0.1"]
        self.ip1 = "192.168.0.1"
        self.ip2 = "192.168.0.1"

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
        self.start_barrier.wait()
        try:
            self.run_test()
        except Exception as e:
            if not self.flag:
                self.flag = True
                self.run_test()
            else:
                print(e)
        finally:
            self.exit_wrapper()
            self.dump_result()

    def gen_test_cmds(self):
        key_list_temp = [chr(i) for i in range(ord("a"), ord("z") + 1)]  # 26个英文字母
        self.key_list = random.sample(key_list_temp, 20)
        self.cmd_list = [
            CmdItem("w", random.choice(self.ip_list), k, 1) for k in self.key_list
        ]  # 先执行wrtie，保证一定有这些key

        temp: list[CmdItem] = []
        for cmd in self.cmd_list:
            for _ in range(10):
                temp.append(
                    CmdItem(
                        random.choice(["w", "r"]),
                        cmd.ip,
                        cmd.key,
                        random.randint(1, 100),
                    )
                )

        random.shuffle(temp)
        self.cmd_list.extend(temp)

    def run_test(self):
        for i, cmd in enumerate(self.cmd_list):
            if (not self.flag) and (i == len(self.cmd_list) * 1 / 2):
                raise Exception()

            match cmd.op:
                case "w":
                    self.write_wrapper(cmd.ip, cmd.key, cmd.val)
                case "r":
                    res = self.read_wrapper(cmd.ip, cmd.key)
                    if not self.flag:
                        self.result.append(Result(cmd.key, res.data))
                    else:
                        self.retry_result.append(Result(cmd.key, res.data))

    def dump_result(self):
        with open(f"./{self.id}.result", "w") as f:
            for item in self.result:
                f.write(f"key: {item.key}, data:{item.val}" + "\n")

        if len(self.retry_result) != 0:
            with open(f"./{self.id}.retry_result", "w") as f:
                for item in self.retry_result:
                    f.write(f"key: {item.key}, data:{item.val}" + "\n")

    def read_wrapper(self, ip: str, key: str):
        err_num = 0
        while err_num < 3:
            try:
                res = lib.read(ip, key, self.ssf_id)
                return res
            except:
                err_num += 1

        raise Exception()

    def write_wrapper(self, ip: str, key: str, value: int | str):
        err_num = 0
        while err_num < 3:
            try:
                res = lib.write(ip, key, value, self.ssf_id)
                return res
            except:
                err_num += 1

        raise Exception()

    def exit_wrapper(self):
        err_num = 0
        ip_set = set(map(lambda x: x.ip, self.cmd_list))
        while err_num < 3:
            try:
                for ip in ip_set:
                    lib.exit(ip, self.ssf_id)
                return
            except:
                err_num += 1

        raise Exception()


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
