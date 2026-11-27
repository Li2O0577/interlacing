"""FIFO admission shared by the schedulers driving one tree of graphs."""
from threading import Lock


class RunSlots:
    def __init__(self, limit: int):
        self.limit = limit
        self._lock = Lock()
        self._owners: set[object] = set()
        self._queue: dict[object, None] = {}

    def take(self, ticket: object, owner: object) -> bool:
        with self._lock:
            self._queue[ticket] = None
            if len(self._owners) == self.limit or next(iter(self._queue)) is not ticket:
                return False
            del self._queue[ticket]
            self._owners.add(owner)
            return True

    def withdraw(self, ticket: object) -> None:
        with self._lock:
            self._queue.pop(ticket, None)

    def release(self, owner: object) -> bool:
        with self._lock:
            if owner not in self._owners:
                return False
            self._owners.remove(owner)
            return True
