import hashlib
import bisect
import re


class ConsistentHash:
    def __init__(
        self, objects: list | dict, intereave_count: int = 40, hasher: int = None
    ):
        self.keys: list[str] = []
        self.key_node = {}
        self.nodes = []
        self.index = 0
        self.weights = {}
        self.total_weight = 0

        self.interleave_count = intereave_count
        self.hasher = hasher

        self.add_nodes(objects)

    def _ingest_objects(self, objects):
        if isinstance(objects, dict):
            self.nodes.extend(objects.keys())
            self.weights.update(objects.copy())
        elif isinstance(objects, list):
            self.nodes.extend(objects[:])
        elif isinstance(objects, str):
            self.nodes.extend(objects)
        elif objects is None:
            pass
        else:
            raise TypeError(
                "The arguments of nodes must be dict,\
                        list or string."
            )

    def add_nodes(self, nodes):
        self._ingest_objects(nodes)

        self._generate_ring(start=self.index)

        self.index = self.get_nodes_cnt()
        self.keys.sort()

    def _generate_ring(self, start=0):
        # Generates the ring.
        for node in self.nodes[start:]:
            for key in self._node_keys(node):
                self.key_node[key] = node
                self.keys.append(key)

    def del_nodes(self, nodes: list[str]):
        # Delete nodes from the ring.
        for node in nodes:
            if node not in self.nodes:
                continue

            for key in self._node_keys(node):
                self.keys.remove(key)
                del self.key_node[key]

            self.index -= 1
            self.nodes.remove(node)

    def _node_keys(self, node: str):
        """
        Generates the keys specific to a given node.
        """
        if node in self.weights:
            weight = self.weights.get(node)
        else:
            weight = 1

        factor = self.interleave_count * weight

        for j in range(factor):
            b_key = self._hash_digest("%s-%s" % (node, j))
            for i in range(4):
                yield self._hash_val(b_key, lambda x: x + i * 4)

    def get_node(self, string_key: str) -> str:
        pos = self.get_node_pos(string_key)
        return self.key_node[self.keys[pos]]

    def get_node_pos(self, string_key):
        if not self.key_node:
            return None
        key = self.gen_key(string_key)
        nodes = self.keys
        pos = bisect.bisect(nodes, key)
        if pos == len(nodes):
            return 0
        else:
            return pos

    def get_all_nodes(self):
        # Sorted with ascend
        return sorted(self.nodes, key=lambda node: list(map(int, re.split("\W", node))))

    def get_nodes_cnt(self):
        return len(self.nodes)

    def gen_key(self, key):
        b_key = self._hash_digest(key)
        return self._hash_val(b_key, lambda x: x)

    def _hash_val(self, b_key, entry_fn):
        return (
            (b_key[entry_fn(3)] << 24)
            | (b_key[entry_fn(2)] << 16)
            | (b_key[entry_fn(1)] << 8)
            | b_key[entry_fn(0)]
        )

    def _hash_digest(self, key):
        key = key.encode() if isinstance(key, str) else key

        if self.hasher is not None:
            res = [x if isinstance(x, int) else ord(x) for x in self.hasher(key)]
        else:
            m = hashlib.md5()
            m.update(key)
            res = [x if isinstance(x, int) else ord(x) for x in m.digest()]

        return res


if __name__ == "__main__":
    con_hash = ConsistentHash(["192.168.0.101:8080", "192.168.0.102:8080"])
    server = con_hash.get_node("192.168.0.")
    print(server)
