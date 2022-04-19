from collections.abc import Iterator
import threading
from typing import Tuple, Generic, TypeVar
from concurrent.futures import ThreadPoolExecutor

from pandas import DataFrame

from synth_tools import check_candidate

T = TypeVar('T')
R = TypeVar('R')

class threadsafe(Generic[T], Iterator[T]):
    def __init__(self, f: Iterator[T]) -> None:
        self.inner = f
        self.lock = threading.Lock()
    
    def __next__(self) -> T:
        with self.lock:
            return self.inner.__next__()


def mp_check_candidate(result, target, signature, queue: list):
    if check_candidate(result, target):
        queue.append(signature)


def split_over_threads(thread_count: int, producer: threadsafe[Tuple[DataFrame, object]], target: DataFrame):
    out = list()
    with ThreadPoolExecutor(thread_count) as executor:
        for res, sig in producer:
            executor.submit(mp_check_candidate, res, target, sig, out)
        executor.shutdown(wait=True, cancel_futures=False)
    return out
