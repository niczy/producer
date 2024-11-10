import asyncio
import dataclasses
import enum
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Awaitable, Generic, List, Optional, Type, TypeVar

import cattr
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf.message import Message

from producer import store
from producer.store import StateStore


logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RetryConfig:
    wait_for_external_service: bool = False
    always_retry_on_error: bool = False
    retry_times: Optional[int] = None
    retry_interval_sec: float = 5
    check_external_service_every_sec: float = 30

    def __post_init__(self):
        if not self.always_retry_on_error and self.retry_times is None:
            raise ValueError("Either 'always_retry' must be True, or 'retry_times' must be set.")


class WaitingExternalServiceException(Exception):
    pass


class ID(enum.Enum):
    # testing tasks
    Double = 'double'
    Triple = 'triple'
    Proto = 'proto'
    PlusOne = 'plus_one'
    Failing = 'failing'
    Cache = 'cache'


InputT = TypeVar('InputT')
OutputT = TypeVar('OutputT')


class ProducerTask(ABC, Generic[InputT, OutputT]):
    '''
    Cache intermediate result
    Retry on failure
    '''
    def __init__(
            self,
            id: ID,
            state_store: Optional[StateStore]=None) -> None:
        if not isinstance(id, ID):
            raise ValueError(f"Expected 'id' to be an instance of ID Enum, got {type(id)}")
        self.id = id
        if state_store is None:
            state_store = store.get_state_store()
        self._state_store = state_store

    async def execute(self, input: InputT, retry_config: Optional[RetryConfig] = None) -> OutputT:
        key = self._input_key(input)
        value = self._state_store.get(key)

        return_type = self._execute.__annotations__.get('return')
        if return_type is None:
            await self._attempt_execute(input, retry_config)
            return None # type: ignore

        if value is not None:
            try:
                logger.debug(f'Returning cached value for key: {input}')
                return self._deserialize(value, return_type)
            except Exception as e:
                logger.warning(f'failed to deserialize cache object, re-executing the task')

        ret = await self._attempt_execute(input, retry_config)
        if isinstance(ret, Exception):
            raise ret

        data = self._to_obj(ret)
        self._state_store.set(key, data)
        return ret

    async def _attempt_execute(self, input: InputT, retry_config: Optional[RetryConfig] = None) -> OutputT:
        _exec_cnt = 0

        if retry_config is None:
            _exec_cnt += 1
            return await self._execute(input)

        while True:
            try:
                _exec_cnt += 1
                return await self._execute(input)
            except WaitingExternalServiceException as e:
                if retry_config.wait_for_external_service:
                    await asyncio.sleep(retry_config.check_external_service_every_sec)
                else:
                    raise e
            except Exception as e:
                logger.exception(f"exception when execution_count={_exec_cnt}")
                assert retry_config.retry_interval_sec is not None
                await asyncio.sleep(retry_config.retry_interval_sec)
                if retry_config.always_retry_on_error:
                    continue
                assert retry_config.retry_times is not None
                if _exec_cnt >= retry_config.retry_times:
                    raise Exception(f'task failed after {_exec_cnt} retries') from e

    def _input_key(self, input: InputT) -> dict:
        obj = self._to_obj(input)
        return {
            "task_id": self.id.value,
            "input": obj
        }

    def _to_obj(self, obj: Any) -> dict:
        if dataclasses.is_dataclass(obj):
            serialized_obj = cattr.unstructure(obj)
            return serialized_obj

        if isinstance(obj, Message):
            return {"value": obj.SerializeToString().hex()}

        raise Exception('Unsupported data type, the data needs to either be a dataclass or a protobuf message')

    def _deserialize(self, obj: dict, clz: Type[OutputT]) -> OutputT:
        if dataclasses.is_dataclass(clz):
            return cattr.structure(obj, clz)

        if issubclass(clz, Message):
            ret = clz()
            ret.ParseFromString(bytes.fromhex(obj["value"])) # type: ignore
            return ret

        raise Exception(f'Unsupported data type {str(clz)}, the data needs to either be a dataclass or a protobuf message')

    @abstractmethod
    async def _execute(self, input: InputT) -> OutputT:
        raise NotImplementedError


async def execute_parallel_tasks(
        task: ProducerTask[InputT, OutputT],
        inputs: List[InputT],
        batch_size: int=32,
        batch_sleep_secs: int=0,
        retry_config: Optional[RetryConfig]=None,
        return_exception: bool=False) -> List[OutputT]:
    futures: List[Awaitable[OutputT]] = []
    output = []
    for input in inputs:
        futures.append(task.execute(input, retry_config=retry_config))
        if len(futures) >= batch_size:
            output = await _reduce(futures, output, return_exception)
            logger.debug(f'output is {output}')
            futures = []
            if batch_sleep_secs > 0:
                logger.info(f'rest for {batch_sleep_secs} secs before new batch')
                time.sleep(batch_sleep_secs)

    if futures:
        output = await _reduce(futures, output, return_exception)
        logger.debug(f'output is {output}')

    return output


async def _reduce(
        futures: List[Awaitable[Any]],
        output: List[Any], return_exception: bool=False) -> List[Any]:
    results = await asyncio.gather(*futures, return_exceptions=return_exception)
    output.extend(results)
    return output


@dataclass
class IntValue():
    value: int


def sum_reducer(a: Optional[IntValue], b: IntValue) -> IntValue:
    if a is None:
        return b
    else:
        return IntValue(a.value + b.value)
