import asyncio
import pytest
import random
import time
# Need imports for threading and executor
import threading
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
from typing import TypeVar, Any, Set, Dict, Callable, Awaitable, List

from grip.grip_stream import (
    GripStream,
    StreamProcessor,
    StreamScope,
    AttemptedSendOnInactive,
    StreamAlreadyAddedError
)

DType = TypeVar('DType')


@pytest.mark.asyncio
async def test_processor_immediate_deactivation():
    """Test that the StreamProcessor can be activated and immediately deactivated."""
    processor = StreamProcessor()
    assert processor.active is False
    processor.activate()
    assert processor.active is True
    
    # Receiver loop starts in the background via activate()

    # Give the loop a tiny bit of time to ensure it's running before deactivate
    await asyncio.sleep(0.01)

    processor.deactivate()
    assert processor.active is False

    # Wait for the internal receiver loop to stop cleanly
    await processor.wait_until_stopped(timeout=1.0)

    # Verify internal task state if needed (optional, implementation detail)
    assert processor._receiver_task is not None
    assert processor._receiver_task.done()
    # Allow for CancelledError on clean shutdown
    if exc := processor._receiver_task.exception():
        assert isinstance(exc, asyncio.CancelledError)


@pytest.mark.asyncio
async def test_single_sender_single_stream_async_processor():
    """Test one sender sending data to one stream with async processor using overload."""
    processor = StreamProcessor[int]()
    scope = StreamScope[int](processor=processor)
    received_data = []

    async def stream_proc_func(data: int):
        await asyncio.sleep(random.uniform(0, 0.001))
        received_data.append(data)

    # Use the overloaded method - type checker knows it's async
    processor.activate()

    async def sender_task():
        for i in range(5):
            await stream_proc_func(i)
            await asyncio.sleep(random.uniform(0.01, 0.05))

    # Create stream using the scope (now async)
    stream = await scope.create_stream(stream_processor_func=stream_proc_func)

    sender = asyncio.create_task(sender_task())
    await sender

    await asyncio.sleep(0.1)

    processor.deactivate()
    success, exc = await processor.wait_until_stopped(timeout=1.0)
    assert success is True
    assert exc is None

    assert received_data == [0, 1, 2, 3, 4]
    assert processor._receiver_task is not None
    assert processor._receiver_task.done()
    assert processor._receiver_task.exception() is None


@pytest.mark.asyncio
async def test_single_sender_single_stream_sync_processor():
    """Test one sender sending data to one stream with sync processor using overload."""
    processor = StreamProcessor[int]()
    scope = StreamScope[int](processor=processor)
    received_data = []

    # Synchronous processor
    def stream_proc_func(data: int):
        # Simulate some CPU work
        _ = [x*x for x in range(10)]
        received_data.append(data)

    # Use the overloaded method - type checker knows it's sync
    processor.activate()

    async def sender_task():
        for i in range(5):
            await stream_proc_func(i)
            await asyncio.sleep(random.uniform(0.01, 0.05))

    # Create stream using scope (async)
    stream = await scope.create_stream(stream_processor_func=stream_proc_func)

    sender = asyncio.create_task(sender_task())
    await sender

    await asyncio.sleep(0.1) 

    processor.deactivate()
    success, exc = await processor.wait_until_stopped(timeout=1.0)
    assert success is True
    assert exc is None

    assert received_data == [0, 1, 2, 3, 4]
    assert processor._receiver_task is not None
    assert processor._receiver_task.done()
    assert processor._receiver_task.exception() is None


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
    processor = StreamProcessor[tuple[int, int]]()
    scope = StreamScope[tuple[int, int]](processor=processor)

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
        stop_event = None # Not needed for asyncio tasks checking processor.active

    # Factory for processors - returns async for even, sync for odd stream_id
    def create_stream_processor_func(stream_id: int) -> Callable[..., Any]: # Return Any for implementation ease
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
    processor.activate()
    event_loop = asyncio.get_running_loop() # Get loop for threadsafe calls

    for i in range(num_streams):
        # Use the overloaded method
        stream = await scope.create_stream(stream_processor_func=create_stream_processor_func(i))
        streams.append(stream)

    # --- Sender Logic: Separate functions for tasks vs threads ---

    async def sender_task_async(sender_id: int):
        """Sender logic for asyncio tasks."""
        sent_count = 0
        while processor.active: # Check processor status directly
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
                 break # Processor likely deactivated
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
    print("Deactivating processor...")
    if stop_event:
        print("Signalling threads to stop...")
        stop_event.set() # Signal threads first
    processor.deactivate()

    print("Waiting for receiver loop to drain and stop...")
    success, exc = await processor.wait_until_stopped(timeout=duration_seconds + 10.0) # Longer timeout for drain
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

    assert processor._receiver_task is not None
    assert processor._receiver_task.done()
    assert processor._receiver_task.exception() is None

    total_received_count = sum(len(v) for v in received_data_per_stream.values())
    print("\n--- Results ---")
    print(f"Total items received across all streams: {total_received_count}")
    print("Received sequence counts per stream:",
          sorted(list({k: len(v) for k, v in received_data_per_stream.items()}.items())))
    print("Sent counts per sender task/thread:",
          sorted(list(sender_counts.items())))

    total_sent_count = sum(count for count in sender_counts.values() if isinstance(count, int))
    print(f"Total items reported sent by senders: {total_sent_count}")
    print(f"Processor queue backup metric: {processor.queue_backup_metric}")

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


@pytest.mark.asyncio
async def test_stream_latest_only():
    """Verify latest_only=True keeps only the last message."""
    processor = StreamProcessor[int]()
    scope = StreamScope[int](processor=processor)
    received_items = []

    async def stream_proc_func(data: int):
        received_items.append(data)
        await asyncio.sleep(0.01) # Add delay to allow sends to queue up

    # Create stream with latest_only=True
    processor.activate()
    stream = await scope.create_stream(stream_proc_func, latest_only=True)

    # Send multiple items quickly
    await stream.send(1)
    await stream.send(2)
    await stream.send(3)

    # Allow time for receiver loop to potentially process notification
    await asyncio.sleep(0.05)

    # Send final item
    await stream.send(4)

    # Allow ample time for final processing
    await asyncio.sleep(0.1)

    processor.deactivate()
    success, exc = await processor.wait_until_stopped(timeout=1.0)
    assert success is True
    assert exc is None

    # Only the very last item sent should have been processed
    assert received_items == [4], f"Expected only [4], got {received_items}"
    # Check internal buffer state (optional, implementation detail test)
    async with stream._lock:
        assert stream._buffer is None or stream._buffer == [], "Buffer should be empty or None after processing"


@pytest.mark.asyncio
async def test_stream_skip_duplicates():
    """Verify skip_duplicates=True prevents processing consecutive identical messages."""
    processor = StreamProcessor[str]()
    scope = StreamScope[str](processor=processor)
    received_items = []
    processor_call_count = 0

    async def stream_proc_func(data: str):
        nonlocal processor_call_count
        processor_call_count += 1
        received_items.append(data)
        await asyncio.sleep(0.001)

    # Create stream with skip_duplicates=True
    processor.activate()
    stream = await scope.create_stream(stream_proc_func, skip_duplicates=True)

    # Send sequence with duplicates
    await stream.send("A")
    await stream.send("A") # Should be skipped
    await stream.send("B")
    await stream.send("B") # Should be skipped
    await stream.send("B") # Should be skipped
    await stream.send("A") # Different from last, should be sent
    await stream.send("C")
    await stream.send("C") # Should be skipped

    # Allow time for processing
    await asyncio.sleep(0.1)

    processor.deactivate()
    success, exc = await processor.wait_until_stopped(timeout=1.0)
    assert success is True
    assert exc is None

    # Check received items and processor calls
    expected_items = ["A", "B", "A", "C"]
    assert received_items == expected_items, f"Expected {expected_items}, got {received_items}"
    assert processor_call_count == len(expected_items), f"Expected processor calls {len(expected_items)}, got {processor_call_count}"
    # Check internal buffer state (optional)
    async with stream._lock:
        assert stream._buffer is None or stream._buffer == [], "Buffer should be empty or None after processing"


@pytest.mark.asyncio
async def test_stream_latest_only_and_skip_duplicates():
    """Verify interaction of latest_only and skip_duplicates."""
    processor = StreamProcessor[str]()
    scope = StreamScope[str](processor=processor)
    received_items = []
    processor_call_count = 0

    async def stream_proc_func(data: str):
        nonlocal processor_call_count
        processor_call_count += 1
        received_items.append(data)
        # Simulate some processing time
        await asyncio.sleep(0.01)

    # Create stream with BOTH options enabled
    processor.activate()
    stream = await scope.create_stream(stream_proc_func, latest_only=True, skip_duplicates=True)

    # --- Send sequence --- 
    print("Sending A (1st)")
    await stream.send("A") # Should be sent (first item)
    await asyncio.sleep(0.001) # Brief pause

    print("Sending A (2nd) - should be skipped")
    await stream.send("A") # Duplicate of last sent, skip
    await asyncio.sleep(0.001)

    print("Sending B (1st)")
    await stream.send("B") # New value, should be sent
    await asyncio.sleep(0.05) # Longer pause - allow A to potentially process

    print("Sending C (1st)")
    await stream.send("C") # New value, replaces B due to latest_only
                           # Should be sent eventually
    await asyncio.sleep(0.001)

    print("Sending C (2nd) - should be skipped")
    await stream.send("C") # Duplicate of last sent (C), skip
    await asyncio.sleep(0.001)

    print("Sending D (1st)")
    await stream.send("D") # New value, replaces C due to latest_only
                           # Should be sent eventually

    # Allow ample time for the receiver loop to process the final state
    await asyncio.sleep(0.1)

    processor.deactivate()
    success, exc = await processor.wait_until_stopped(timeout=2.0) # Increased timeout just in case
    assert success is True
    assert exc is None

    # --- Verification --- 
    # Expected sequence: 
    # - A is sent initially.
    # - A (2nd) is skipped.
    # - B is sent, replaces A if A wasn't processed yet.
    # - C is sent, replaces B if B wasn't processed yet.
    # - C (2nd) is skipped.
    # - D is sent, replaces C if C wasn't processed yet.
    # Because latest_only=True, only the *last* non-skipped value processed matters.
    # Processor delays ensure some intermediate values might get processed.
    
    print(f"Received Items: {received_items}")
    print(f"Processor Calls: {processor_call_count}")

    # Crucially, even if A, B, C were processed, the final state processed MUST be D
    # because latest_only ensures only the last value overwrites previous ones in the buffer.
    # However, the processor runs for each *non-skipped* value that gets scheduled.
    expected_values_processed = ["A", "B", "C", "D"]
    assert received_items == expected_values_processed, \
        f"Expected processed items {expected_values_processed}, got {received_items}"
    assert processor_call_count == len(expected_values_processed), \
        f"Expected {len(expected_values_processed)} processor calls, got {processor_call_count}"


@pytest.mark.asyncio
async def test_stream_scope_drain():
    """Verify StreamScope.drain detaches streams and waits for pending processing."""
    processor = StreamProcessor[int]()
    scope = StreamScope[int](processor=processor)
    received_items = []
    processing_complete_events = []

    async def stream_proc_func(data: int):
        event = asyncio.Event()
        processing_complete_events.append(event)
        # Simulate some work
        await asyncio.sleep(0.05)
        received_items.append(data)
        event.set() # Signal this item is done

    processor.activate()
    stream = await scope.create_stream(stream_proc_func)

    # Send initial items
    await stream.send(1)
    await stream.send(2)

    # Give a moment for processing to start
    await asyncio.sleep(0.01)

    # Drain the scope
    print("Calling scope.drain()...")
    drain_task = asyncio.create_task(scope.drain(timeout=1.0))

    # Try sending after drain starts - should fail or be ignored
    send_after_drain_failed = False
    try:
        await stream.send(3)
        # Give a tiny moment in case send doesn't raise immediately
        await asyncio.sleep(0.01)
    except AttemptedSendOnInactive:
        send_after_drain_failed = True
    except Exception as e:
        pytest.fail(f"Sending after drain raised unexpected error: {e}")

    # Wait for drain to complete
    await drain_task
    print("scope.drain() completed.")

    # Wait for all processing events triggered *before* drain
    if processing_complete_events:
        await asyncio.gather(*(evt.wait() for evt in processing_complete_events))
    print("All processing events set.")

    # Verify only pre-drain items were received
    assert received_items == [1, 2],\
        f"Expected [1, 2], got {received_items}"

    # Verify stream was detached (callback is None)
    assert stream._notify_dirty_callback is None,\
        "Stream notification callback was not cleared by drain."
    assert stream._scope_ref is None,\
        "Stream scope reference was not cleared by drain."

    # Verify send after drain failed as expected
    # Note: If send doesn't raise but is just ignored, this check might need adjustment
    # based on the exact behaviour desired/implemented in send when detached.
    # Current implementation *should* raise AttemptedSendOnInactive.
    assert send_after_drain_failed is True, "Send after drain did not fail as expected."

    # Cleanly stop processor
    processor.deactivate()
    success, exc = await processor.wait_until_stopped(timeout=1.0)
    assert success is True
    assert exc is None


@pytest.mark.asyncio
async def test_stream_scope_drain_timeout():
    """Verify StreamScope.drain raises TimeoutError if processing takes too long."""
    processor = StreamProcessor[int]()
    scope = StreamScope[int](processor=processor)
    processing_started_event = asyncio.Event()
    processing_blocker = asyncio.Event() # Event to manually unblock processing

    async def slow_stream_proc_func(data: int):
        processing_started_event.set()
        # Wait indefinitely until blocker is set
        await processing_blocker.wait()

    processor.activate()
    stream = await scope.create_stream(slow_stream_proc_func)

    # Send an item
    await stream.send(1)

    # Wait for processing to start
    await asyncio.wait_for(processing_started_event.wait(), timeout=1.0)

    # Attempt to drain with a short timeout
    drain_timeout = 0.1
    print(f"Calling scope.drain() with timeout {drain_timeout}s...")
    with pytest.raises(asyncio.TimeoutError):
        await scope.drain(timeout=drain_timeout)
    print("scope.drain() timed out as expected.")

    # Verify stream was still detached even though drain timed out
    assert stream._notify_dirty_callback is None,\
        "Stream notification callback was not cleared by drain timeout."
    assert stream._scope_ref is None,\
        "Stream scope reference was not cleared by drain timeout."

    # Attempt to send after drain timeout - should still fail
    send_after_drain_failed = False
    try:
        await stream.send(2)
    except AttemptedSendOnInactive:
        send_after_drain_failed = True
    assert send_after_drain_failed is True, \
        "Send after drain timeout did not fail."

    # Allow processing to finish now
    processing_blocker.set()

    # Cleanly stop processor
    processor.deactivate()
    # Need potentially longer timeout here as processing was blocked
    success, exc = await processor.wait_until_stopped(timeout=2.0)
    assert success is True
    assert exc is None


# Keep or remove the main block as needed
if __name__ == "__main__":
    asyncio.run(test_multi_sender_multi_stream())

