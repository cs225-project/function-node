import redis
import random
from dataclasses import dataclass
import time


@dataclass
class CmdItem:
    op: str
    ip: str
    key: str
    val: int


ip_list: list[str] = [
    "192.168.0.103:6379",
    "192.168.0.104:6379",
    "192.168.0.105:6379",
    "192.168.0.106:6379",
]


def chooseRW(i: int, total: int, ratio: float = 0.3):
    if i <= int(total * ratio):
        return "r"
    return "w"


key_list_temp = [chr(i) for i in range(ord("a"), ord("z") + 1)]
key_list = random.sample(key_list_temp, 10)

rcli = redis.Redis(host="192.168.0.103", port=6379)


cmd_list: list[CmdItem] = [
    CmdItem("w", random.choice(ip_list), k, 1) for k in key_list
]  # 先执行wrtie，保证一定有这些key

temp: list[CmdItem] = []
total = 90
for cmd in cmd_list:
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
cmd_list.extend(temp)


# 生成测试数据
time1 = time.time()
for i, cmd in enumerate(cmd_list):
    match cmd.op:
        case "w":
            version = rcli.incr(cmd.key)
            rcli.set(f"{cmd.key}:{version}", cmd.val)
        case "r":
            version = rcli.get(cmd.key)
            rcli.get(f"{cmd.key}:{version}")
time2 = time.time()
rcli.close()

print(time2 - time1)
