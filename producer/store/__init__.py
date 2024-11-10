from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional
from typing_extensions import override
from time import time
import json
import hashlib



def hash_dict(input: dict) -> bytes:
    serialized_input = json.dumps(input, sort_keys=True).encode()
    return hashlib.sha1(serialized_input).digest()

@dataclass
class StateEntry:
    input: dict
    result: dict
    updated_time: int = 0

    def __post_init__(self):
        self.updated_time= int(time())

class StateStore(ABC):
    '''
        State store stores temparary state, intermeidate results
    '''

    @abstractmethod
    def get(self, input: dict) -> Optional[dict]:
        raise NotImplementedError()

    @abstractmethod
    def set(self, input: dict, result: dict) -> None:
        raise NotImplementedError()

class MapStateStore(StateStore):

    def __init__(self):
        self._store: dict = {}

    @override
    def get(self, input: dict) -> Optional[dict]:
        entry = self._store.get(hash_dict(input))
        if entry is None:
            return None
        return entry.result

    @override
    def set(self, input: dict, result: dict) -> None:
        self._store[hash_dict(input)] = StateEntry(input, result)

def get_state_store() -> StateStore:
    return MapStateStore()
