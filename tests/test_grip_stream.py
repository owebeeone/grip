import asyncio
import pytest
import random
import time
from collections import defaultdict
from typing import TypeVar, Any, Set, Dict, Callable, Awaitable

from grip.grip_stream import (
    GripStream, GripStreamMux, AttemptedSendOnInactive, StreamAlreadyAddedError
)

DType = TypeVar('DType')


@pytest.mark.asyncio
async def test_mux_immediate_deactivation():
    """Test that the multiplexer can be activated and immediately deactivated."""
    mux = GripStreamMux()
    assert mux.active is False
    mux.activate()
    assert mux.active is True
    
    # Receiver loop starts in the background via activate()

    # Give the loop a tiny bit of time to ensure it's running before deactivate
    await asyncio.sleep(0.01)

    mux.deactivate()
    assert mux.active is False

    # Wait for the internal receiver loop to stop cleanly
    await mux.wait_until_stopped(timeout=1.0)

    # Verify internal task state if needed (optional, implementation detail)
    assert mux._receiver_task is not None
    assert mux._receiver_task.done()
    assert mux._receiver_task.exception() is None


@pytest.mark.asyncio
async def test_single_sender_single_stream_async_processor():
    """Test one sender sending data to one stream with async processor using overload."""
    mux = GripStreamMux[int]()
    received_data = []

    async def processor(data: int):
        await asyncio.sleep(random.uniform(0, 0.001))
        received_data.append(data)

    # Use the overloaded method - type checker knows it's async
    stream = mux.create_stream(processor=processor)

    mux.activate()

    async def sender_task():
        for i in range(5):
            stream.send(i)
            await asyncio.sleep(random.uniform(0.01, 0.05))

    sender = asyncio.create_task(sender_task())
    await sender

    await asyncio.sleep(0.1)

    mux.deactivate()
    success, exc = await mux.wait_until_stopped(timeout=1.0)
    assert success is True
    assert exc is None

    assert received_data == [0, 1, 2, 3, 4]
    assert mux._receiver_task is not None
    assert mux._receiver_task.done()
    assert mux._receiver_task.exception() is None


@pytest.mark.asyncio
async def test_single_sender_single_stream_sync_processor():
    """Test one sender sending data to one stream with sync processor using overload."""
    mux = GripStreamMux[int]()
    received_data = []

    # Synchronous processor
    def processor(data: int):
        # Simulate some CPU work
        _ = [x*x for x in range(10)] 
        received_data.append(data)

    # Use the overloaded method - type checker knows it's sync
    stream = mux.create_stream(processor=processor)

    mux.activate()

    async def sender_task():
        for i in range(5):
            stream.send(i)
            await asyncio.sleep(random.uniform(0.01, 0.05))

    sender = asyncio.create_task(sender_task())
    await sender

    await asyncio.sleep(0.1) 

    mux.deactivate()
    success, exc = await mux.wait_until_stopped(timeout=1.0)
    assert success is True
    assert exc is None

    assert received_data == [0, 1, 2, 3, 4]
    assert mux._receiver_task is not None
    assert mux._receiver_task.done()
    assert mux._receiver_task.exception() is None


@pytest.mark.asyncio
async def test_multi_sender_multi_stream(num_streams: int = 13, num_senders: int = 15, duration_seconds: int = 2):
    """Test multiple senders/streams with async processor and overloaded factory."""
    mux = GripStreamMux[tuple[int, int]]()

    received_data_per_stream: Dict[int, Set[int]] = defaultdict(set)
    stream_counters: Dict[int, int] = defaultdict(int)

    # Factory for processors - returns async for even, sync for odd stream_id
    def create_processor(stream_id: int) -> Callable[..., Any]: # Return Any for implementation ease
        # Common logic for checking duplicates
        def check_and_add(data: tuple[int, int]):
            seq_num, sender_id = data
            assert seq_num not in received_data_per_stream[stream_id], \
                f"Stream {stream_id} received duplicate seq num {seq_num} from sender {sender_id}"
            received_data_per_stream[stream_id].add(seq_num)
            
        if stream_id % 2 == 0:
            # Even stream_id: Use an async processor
            async def async_processor(data: tuple[int, int]):
                # Simulate tiny async work
                await asyncio.sleep(0.00001) 
                check_and_add(data)
            return async_processor
        else:
            # Odd stream_id: Use a sync processor
            def sync_processor(data: tuple[int, int]):
                # Simulate tiny sync work
                _ = [x*x for x in range(5)]
                check_and_add(data)
            return sync_processor

    streams: list[GripStream[tuple[int, int]]] = []
    mux.activate()
    for i in range(num_streams):
        # Use the overloaded method
        stream = mux.create_stream(processor=create_processor(i))
        streams.append(stream)

    async def sender_task(sender_id: int):
        """Sends data until the mux becomes inactive. Returns count of sent items."""
        sent_count = 0
        while mux.active:
            target_stream_index = random.randrange(num_streams)
            target_stream = streams[target_stream_index]

            # This counter access is not thread-safe but sufficient for testing
            # the receiver side's ability to handle the sequence correctly.
            data_to_send = stream_counters[target_stream_index]
            stream_counters[target_stream_index] += 1

            try:
                # Send a tuple (sequence_number, sender_id)
                target_stream.send((data_to_send, sender_id))
                sent_count += 1 # Increment count on successful send
            except AttemptedSendOnInactive:
                # This can happen if mux deactivates between check and send
                # print(f"Sender {sender_id} attempted send on inactive mux. Stopping.")
                break # Exit loop if mux is inactive
            except Exception as e:
                print(f"Sender {sender_id} encountered unexpected error: {e}")
                # Decide if we should break or continue based on error type
                break

            await asyncio.sleep(random.uniform(0, 0.0001)) # Shorter sleep
        return sent_count # Return the total count for this sender

    sender_tasks = [asyncio.create_task(sender_task(i)) for i in range(num_senders)]

    # Let senders run for the specified duration
    await asyncio.sleep(duration_seconds)

    # Now deactivate the multiplexer - this should signal senders to stop
    mux.deactivate()

    # Wait for the receiver task to finish processing queued items and stop
    # Increase timeout slightly to accommodate remaining items in queue + shutdown
    success, exc = await mux.wait_until_stopped(timeout=duration_seconds + 1.5)
    assert success is True
    assert exc is None

    # Wait for sender tasks to cleanly exit their loops (they check mux.active)
    # Use gather to potentially catch sender exceptions
    results = await asyncio.gather(*sender_tasks, return_exceptions=True)
    
    sender_counts: Dict[int, int | str] = {}
    for i, result in enumerate(results):
        if isinstance(result, Exception):
             print(f"Sender task {i} finished with exception: {result}")
             sender_counts[i] = f"Exception: {result}"
        else:
             # Result should be the integer count returned by sender_task
             sender_counts[i] = result

    # Verify internal receiver task state (optional)
    assert mux._receiver_task is not None
    assert mux._receiver_task.done()
    assert mux._receiver_task.exception() is None

    # Verification
    total_received_count = sum(len(v) for v in received_data_per_stream.values())
    print(f"Total items received across all streams: {total_received_count}")
    print("Received sequence counts per stream:", 
          sorted(list({k: len(v) for k, v in received_data_per_stream.items()}.items())))
    print("Sent counts per sender task:", 
          sorted(list(sender_counts.items())))

    # Optional: Verify total sent approx equals total received 
    # (might differ slightly due to race conditions during shutdown)
    total_sent_count = sum(count for count in sender_counts.values() if isinstance(count, int))
    print(f"Total items successfully sent by senders: {total_sent_count}")

    for stream_id, received_set in received_data_per_stream.items():
        if not received_set:
            print(f"Warning: Stream {stream_id} received no data.")
            continue

        # received_set now directly contains the sequence numbers (integers)
        max_received = max(received_set)
        expected_set = set(range(max_received + 1))
        
        missing = expected_set - received_set
        unexpected = received_set - expected_set

        assert received_set == expected_set, (
            f"Stream {stream_id}: Sequence mismatch! "
            f"Expected {len(expected_set)} items (0 to {max_received}). "
            f"Received {len(received_set)} items. "
            f"Missing: {sorted(list(missing)) if missing else 'None'}. "
            f"Unexpected: {sorted(list(unexpected)) if unexpected else 'None'}."
        )

# Keep or remove the main block as needed
if __name__ == "__main__":
    asyncio.run(test_multi_sender_multi_stream())

