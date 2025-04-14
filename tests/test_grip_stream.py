import asyncio
import pytest
import random
import time
# Need imports for threading and executor
import threading # Added
from concurrent.futures import ThreadPoolExecutor # Added
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
            await stream.send(i)
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
            await stream.send(i)
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
async def test_multi_sender_multi_stream(
    num_streams: int = 5,
    num_senders: int = 10,
    duration_seconds: float = 2.0,
    num_threads: int = 0 # New parameter: 0 means use asyncio tasks
):
    """
    Test multiple senders/streams with optional threading for senders.
    num_threads=0: Senders run as asyncio tasks (default).
    num_threads>0: Senders run in specified number of OS threads.
    """
    mux = GripStreamMux[tuple[int, int]]()

    received_data_per_stream: Dict[int, Set[int]] = defaultdict(set)
    stream_counters: Dict[int, int] = defaultdict(int)

    # --- Locks: Use appropriate type based on execution mode ---
    if num_threads > 0:
        # Locks for thread-based senders
        stream_locks: Dict[int, threading.Lock] = {
            i: threading.Lock() for i in range(num_streams)
        }
        stop_event = threading.Event() # Event to signal threads to stop
    else:
        # Locks for asyncio task-based senders
        stream_locks: Dict[int, asyncio.Lock] = {
            i: asyncio.Lock() for i in range(num_streams)
        }
        stop_event = None # Not needed for asyncio tasks checking mux.active

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
                check_and_add(data)
            return async_processor
        else:
            # Odd stream_id: Use a sync processor
            def sync_processor(data: tuple[int, int]):

                check_and_add(data)
            return sync_processor

    streams: list[GripStream[tuple[int, int]]] = []
    mux.activate()
    event_loop = asyncio.get_running_loop() # Get loop for threadsafe calls

    for i in range(num_streams):
        # Use the overloaded method
        stream = mux.create_stream(processor=create_processor(i))
        streams.append(stream)

    # --- Sender Logic: Separate functions for tasks vs threads ---

    async def sender_task_async(sender_id: int):
        """Sender logic for asyncio tasks."""
        sent_count = 0
        while mux.active: # Check mux status directly
            target_stream_index = random.randrange(num_streams)
            target_stream = streams[target_stream_index]
            target_lock = stream_locks[target_stream_index] # asyncio.Lock

            # --- Atomic read and increment using the stream's lock ---
            async with target_lock:
                data_to_send = stream_counters[target_stream_index]
                stream_counters[target_stream_index] += 1
            # --- End of atomic section ---

            try:
                # SEND IS NOW ASYNC
                await target_stream.send((data_to_send, sender_id))
                sent_count += 1
            except AttemptedSendOnInactive:
                break
            except Exception as e:
                print(f"Async Sender {sender_id} error: {e}")
                break
            # Yield control briefly, sleep is less critical now as send is async
            await asyncio.sleep(0)
        return sent_count # Return the total count for this sender

    def sender_worker_thread(sender_id: int):
        """Sender logic for worker threads."""
        sent_count = 0
        while not stop_event.is_set(): # Check threading stop event
            target_stream_index = random.randrange(num_streams)
            target_stream = streams[target_stream_index]
            target_lock = stream_locks[target_stream_index] # threading.Lock

            # --- Atomic read and increment using the stream's lock ---
            with target_lock: # Use standard with for threading.Lock
                data_to_send = stream_counters[target_stream_index]
                stream_counters[target_stream_index] += 1
            # --- End of atomic section ---

            try:
                # Schedule the async send coroutine onto the event loop from the thread
                future = asyncio.run_coroutine_threadsafe(
                    target_stream.send((data_to_send, sender_id)),
                    event_loop
                )
                # Optional: Wait for the send to complete on the event loop
                # Can add a timeout here if needed
                future.result(timeout=1.0) # Waits for completion, raises if send failed
                sent_count += 1
            # Catch exceptions from run_coroutine_threadsafe or future.result
            except AttemptedSendOnInactive: # This might be raised by future.result()
                 break # Mux likely deactivated
            except Exception as e:
                # Handles QueueFull in send, TimeoutError from future.result, etc.
                # Check stop_event again in case the error is due to shutdown race
                if stop_event.is_set():
                     break
                print(f"Thread Sender {sender_id} error: {e}")
                # Decide whether to break or continue based on error
                break # Safer to break on unexpected errors
            time.sleep(0) # Yield within the thread, less efficient than asyncio.sleep(0)
        return sent_count

    # --- Task/Thread Creation and Execution ---
    sender_futures = []
    executor = None

    if num_threads > 0:
        print(f"Running {num_senders} senders in {num_threads} threads...")
        executor = ThreadPoolExecutor(max_workers=num_threads)
        for i in range(num_senders):
            # Submit the thread worker function to the executor
            future = event_loop.run_in_executor(
                executor, sender_worker_thread, i
            )
            sender_futures.append(future)
    else:
        print(f"Running {num_senders} senders as asyncio tasks...")
        for i in range(num_senders):
            # Create asyncio tasks
            task = asyncio.create_task(sender_task_async(i))
            sender_futures.append(task)

    # Let senders run for the specified duration
    await asyncio.sleep(duration_seconds)

    # --- Shutdown ---
    print("Deactivating mux...")
    if stop_event:
        print("Signalling threads to stop...")
        stop_event.set() # Signal threads first
    mux.deactivate()

    print("Waiting for receiver loop to drain and stop...")
    success, exc = await mux.wait_until_stopped(timeout=duration_seconds + 10.0) # Longer timeout for drain
    if not success:
         print(f"Receiver loop failed to stop cleanly: {exc}")
    assert success is True
    assert exc is None
    print("Receiver loop stopped.")

    print("Waiting for sender tasks/threads to finish...")
    # Use asyncio.gather for tasks, wait for executor futures for threads
    if executor:
        # Wait for futures submitted via run_in_executor
        results = await asyncio.gather(*sender_futures, return_exceptions=True)
        print("Shutting down thread pool executor...")
        executor.shutdown(wait=True) # Wait for threads to finish
        print("Executor shut down.")
    else:
        # Wait for asyncio tasks
        results = await asyncio.gather(*sender_futures, return_exceptions=True)

    # --- Verification (remains largely the same) ---
    print("Processing results...")
    sender_counts: Dict[int, int | str] = {}
    for i, result in enumerate(results):
         if isinstance(result, Exception):
             print(f"Sender task/thread {i} finished with exception: {result}")
             sender_counts[i] = f"Exception: {result}"
         else:
             sender_counts[i] = result

    assert mux._receiver_task is not None
    assert mux._receiver_task.done()
    assert mux._receiver_task.exception() is None

    total_received_count = sum(len(v) for v in received_data_per_stream.values())
    print(f"\n--- Results ---")
    print(f"Total items received across all streams: {total_received_count}")
    print("Received sequence counts per stream:",
          sorted(list({k: len(v) for k, v in received_data_per_stream.items()}.items())))
    print("Sent counts per sender task/thread:",
          sorted(list(sender_counts.items())))

    total_sent_count = sum(count for count in sender_counts.values() if isinstance(count, int))
    print(f"Total items reported sent by senders: {total_sent_count}")
    print(f"Queue backup metric (items processed in batches > 1): {mux.queue_backup_metric}")

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

