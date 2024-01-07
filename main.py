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
    def __init__(self, prefix: int, start_barrier: Barrier_, end_barrier: Barrier_):
        self.prefix = prefix
        self.start_barrier = start_barrier
        self.end_barrier = end_barrier
        self.ip_list: list[str] = ["192.168.0.1", "192.168.0.1"]
        self.ip1 = "192.168.0.1"
        self.ip2 = "192.168.0.1"
        super().__init__()

    def run(self):
        self.result: list[Result] = []
        self.retry_result: list[Result] = []
        self.flag = False  # 表示是否重试了
        self.ssf_id = int(str(self.prefix) + str(self.ident))
        self.gen_test_cmds()
        self.start_barrier.wait()
        try:
            self.run_test()
        except Exception as e:
            print(e)
        finally:
            self.exit_wrapper()
            # self.dump_result()

    def gen_test_cmds(self):
        key_list_temp = [chr(i) for i in range(ord("a"), ord("z") + 1)]  # 26个英文字母
        self.key_list = random.sample(key_list_temp, 1)
        self.cmd_list = [
            CmdItem("w", random.choice(self.ip_list), k, 1) for k in self.key_list
        ]  # 先执行wrtie，保证一定有这些key

        temp: list[CmdItem] = []
        for cmd in self.cmd_list:
            for _ in range(1):
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
        print(self.cmd_list)
        # self.cmd_list = temp

    def run_test(self):
        for cmd in self.cmd_list:
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
        print(self.result)

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
        while err_num < 3:
            try:
                lib.exit(self.ip2, self.ssf_id)
                return
            except:
                err_num += 1

        raise Exception()


if __name__ == "__main__":
    start_barrier = Barrier(2)
    end_barrier = Barrier(2)
    res = remove_dots_to_int(socket.gethostbyname(socket.gethostname()))
    num = 2
    funcs_list: list[FUNC] = [FUNC(res, start_barrier, end_barrier) for _ in range(num)]

    for i in range(num):
        funcs_list[i].start()

    for i in range(num):
        funcs_list[i].join()
