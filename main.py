import lib
import socket
from multiprocessing import Process, Barrier
from multiprocessing.synchronize import Barrier as Barrier_


def remove_dots_to_int(ip: str) -> int:
    parts = ip.split(".")
    result = "".join(part.replace(".", "") for part in parts)
    return int(result)


class FUNC(Process):
    def __init__(self, prefix: int, start_barrier: Barrier_, end_barrier: Barrier_):
        self.prefix = prefix
        self.start_barrier = start_barrier
        self.end_barrier = end_barrier
        super().__init__()

    def run(self):
        self.ssf_id = int(str(self.prefix) + str(self.ident))
        self.start_barrier.wait()
        try:
            self.read_wrapper("b")
        except:
            print("false")
            return

    def read_wrapper(self, key: str):
        err_num = 0
        while err_num < 3:
            try:
                res = lib.read("192.168.0.1", key, self.ssf_id)
                return res
            except:
                err_num += 1

        raise Exception()

    def write_wrapper(self, key: str, value: any):
        err_num = 0
        while err_num < 3:
            try:
                lib.write("192.168.0.1", key, value, self.ssf_id)
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
