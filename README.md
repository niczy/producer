# Producer Task Framework

A framework for executing tasks with caching, retries, and parallel execution capabilities.

## Features

- Task caching using configurable state stores
- Configurable retry policies
- Parallel task execution with batching
- Support for dataclasses and protobuf messages
- Type-safe task definitions

## Examples

### Basic Task Definition and Execution

Here's an example of defining and executing a basic task using the framework:
```python
from producer import ProducerTask, IntValue

class PlusOneTask(ProducerTask):
    def __init__(self, *args, **kwargs):
        super().__init__(ID.PlusOne, *args, **kwargs)

    async def _execute(self, input: IntValue) -> IntValue:
        ret = IntValue(input.value)
        ret.value += 1
        return ret

# Example execution
state = MapStateStore()
input = IntValue(2)
task = PlusOneTask(state_store=state)
output = asyncio.run(task.execute(input))
print(output.value)  # Output: 3
```