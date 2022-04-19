from collections.abc import Iterator
import threading
from typing import Callable, Generic, TypeVar

T = TypeVar('T')
R = TypeVar('R')

class threadsafe(Generic[T], Iterator[T]):
    def __init__(self, f: Iterator[T]) -> None:
        self.inner = f
        self.lock = threading.Lock()
    
    def __next__(self) -> T:
        with self.lock:
            return self.inner.__next__()


def split_over_threads(thread_count: int, producer: threadsafe[T], *consumers: list[Callable[[T], R]]):
    from concurrent.futures import ThreadPoolExecutor
    from multiprocessing import Queue
    out = Queue()
    with ThreadPoolExecutor(thread_count) as executor:
        for o in producer:
            for c in consumers:
                res = executor.submit(c, o)
                res.add_done_callback(print)