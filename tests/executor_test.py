import asyncio
import time
from typing_extensions import override
from typing import List, Any
from dataclasses import dataclass
import logging

import pytest
from producer import (
    ProducerTask,
    IntValue,
    execute_parallel_tasks,
    RetryConfig,
)
from producer.store import MapStateStore 
from producer import ID, WaitingExternalServiceException 


logger = logging.getLogger(__name__)


class PlusOneTask(ProducerTask):
    def __init__(self, *args, **kwargs):
        super().__init__(ID.PlusOne, *args, **kwargs)
        self._exec_cnt = 0

    @override
    async def _execute(self, input: IntValue) -> IntValue:
        self._exec_cnt += 1
        ret = IntValue(input.value)
        ret.value += 1
        return ret


class DoubleTask(ProducerTask):
    def __init__(self, *args, **kwargs):
        super().__init__(ID.Double, *args, **kwargs)

    @override
    async def _execute(self, input: IntValue) -> IntValue:
        ret = IntValue(input.value)
        ret.value *= 2
        return ret

class TripleTask(ProducerTask):
    def __init__(self, *args, **kwargs):
        super().__init__(ID.Triple, *args, **kwargs)

    @override
    async def _execute(self, input: IntValue) -> IntValue:
        ret = IntValue(input.value)
        ret.value *= 3
        return ret

def test_tasks_with_same_input():
    state = MapStateStore()
    input = IntValue(2)

    result: IntValue = asyncio.run(DoubleTask(state_store=state).execute(input))
    assert result.value == 4

    result: IntValue = asyncio.run(TripleTask(state_store=state).execute(input))
    assert result.value == 6

def test_executor():
    state = MapStateStore()
    input = IntValue(2)
    task = PlusOneTask(state)
    assert task._exec_cnt == 0

    output = asyncio.run(task.execute(input))
    assert isinstance(output, IntValue)
    assert output.value == 3
    assert task._exec_cnt == 1

    output = asyncio.run(task.execute(input))
    assert isinstance(output, IntValue)
    assert output.value == 3
    assert task._exec_cnt == 1

def test_parallel_execute():
    state = MapStateStore()
    input1 = IntValue(2)
    plus_one = PlusOneTask(state)

    output: List[IntValue] = asyncio.run(
        execute_parallel_tasks(plus_one, [input1, input1]))
    assert len(output) == 2
    assert plus_one._exec_cnt == 1
    assert output[0].value == 3
    assert output[1].value == 3

    input3 = IntValue(3)
    input4 = IntValue(4)
    plus_one = PlusOneTask(state)

    output: List[IntValue] = asyncio.run(
        execute_parallel_tasks(plus_one, [input3, input4]))
    assert len(output) == 2
    assert plus_one._exec_cnt == 1 + 1

async def test_retry_logic():
    class FailingTask(ProducerTask):
        def __init__(self, *args, **kwargs):
            super().__init__(ID.Failing, *args, **kwargs)
            self._exec_cnt = 0

        async def _execute(self, input: Any) -> Any:
            self._exec_cnt += 1
            raise Exception("Task failed")

    state = MapStateStore()
    input = IntValue(2)
    task = FailingTask(state)
    retry_config = RetryConfig(always_retry_on_error=False, retry_times=3, retry_interval_sec=0.1)

    start_time = time.time()
    with pytest.raises(Exception) as exc_info:
        await task.execute(input, retry_config=retry_config)
    end_time = time.time()

    logger.debug(f'exc_info {exc_info}')
    assert "Task failed" in str(exc_info.value.__cause__)
    assert task._exec_cnt == 3  # Initial + 3 retries
    assert 0.3 <= (end_time - start_time) < 0.5  # Should have retried 3 times with a 0.1 sec interval

async def test_retry_logic_waiting_external_service_no():
    class FailingTask(ProducerTask):
        def __init__(self, *args, **kwargs):
            super().__init__(ID.Failing, *args, **kwargs)
            self._exec_cnt = 0

        async def _execute(self, input: Any) -> Any:
            self._exec_cnt += 1
            raise WaitingExternalServiceException("still running")

    state = MapStateStore()
    input = IntValue(2)
    task = FailingTask(state)
    retry_config = RetryConfig(always_retry_on_error=False, retry_times=3, retry_interval_sec=0.1)

    with pytest.raises(Exception) as exc_info:
        await task.execute(input, retry_config=retry_config)

    logger.debug(f'exc_info {exc_info}')
    assert "still running" in str(exc_info)
    assert task._exec_cnt == 1  # no retry when wait_for_external_service no

async def test_retry_logic_waiting_external_service_yes():
    class FailingTask(ProducerTask):
        def __init__(self, *args, **kwargs):
            super().__init__(ID.Failing, *args, **kwargs)
            self._exec_cnt = 0

        async def _execute(self, input: Any) -> Any:
            self._exec_cnt += 1
            raise WaitingExternalServiceException("still running")

    state = MapStateStore()
    input = IntValue(2)
    task = FailingTask(state)
    retry_config = RetryConfig(wait_for_external_service=True, always_retry_on_error=False, retry_times=3, retry_interval_sec=0.1)

    try:
        await asyncio.wait_for(task.execute(input, retry_config=retry_config), timeout=3)
        assert False, 'should never return'
    except asyncio.TimeoutError:
        logger.debug("Task execution timed out after 3 seconds")
        pass


@dataclass
class ExecuteResult:
    result_id: str

class CacheTask(ProducerTask):
    def __init__(self, *args, **kwargs):
        super().__init__(ID.Cache, *args, **kwargs)

    @override
    async def _execute(self, input: IntValue) -> ExecuteResult:
        return ExecuteResult(result_id="export_id")
 
def test_cache_task():
    state = MapStateStore()
    task = CacheTask(state_store=state)
    input = IntValue(2)
    result: ExecuteResult = asyncio.run(task.execute(input))
    logger.debug(f'result {result}')
    assert result.result_id== "export_id"

    result = asyncio.run(task.execute(input))
    logger.debug(f'result2 {result}')
    assert result.result_id == "export_id"

    # try to mess up the cache.
    key = task._input_key(input)
    task._state_store.set(key, {})

    result = asyncio.run(task.execute(input))
    assert result.result_id == "export_id"


def test_parallel_retry():
    class FailingTask(ProducerTask):
        def __init__(self, *args, **kwargs):
            super().__init__(ID.Failing, *args, **kwargs)
            self._exec_cnt = 0

        async def _execute(self, input: Any) -> Any:
            self._exec_cnt += 1
            raise Exception("task failed")

    retry_config = RetryConfig(retry_times=3, retry_interval_sec=0.1)
    assert retry_config.retry_times is not None

    state = MapStateStore()
    input1 = IntValue(1)
    input3 = IntValue(3)
    inputs = [input1, input3]
    task = FailingTask(state)

    output: List[IntValue] = asyncio.run(
        execute_parallel_tasks(task, inputs,
                               retry_config=retry_config, return_exception=True))
    assert len(output) == len(inputs)
    assert task._exec_cnt == len(inputs) * retry_config.retry_times