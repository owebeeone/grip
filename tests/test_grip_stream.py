import asyncio
import pytest
import random
import time
from collections import defaultdict

from grip.grip_stream import GripStream, GripStreamMux


@pytest.mark.asyncio
async def test_mux_immediate_deactivation():
    """Test that the multiplexer can be activated and immediately deactivated."""
    mux = GripStreamMux()
    mux.activate()
    assert mux.active is True

    receiver_task = asyncio.create_task(mux.receive_loop())

    # Give the loop a chance to start if needed, though it should check active status first
    await asyncio.sleep(0.01) 

    mux.deactivate()
    assert mux.active is False

    # The receiver loop should terminate quickly after deactivation
    await asyncio.wait_for(receiver_task, timeout=1.0)

    # Ensure it actually terminated and didn't timeout
    assert receiver_task.done()
    # Check for exceptions
    assert receiver_task.exception() is None


@pytest.mark.asyncio
async def test_single_sender_single_stream():
    """Test one sender sending data to one stream."""
    mux = GripStreamMux()
    received_data = []

    def processor(data):
        received_data.append(data)
        return data

    stream = GripStream(processor=processor)
    
    mux.activate()
    mux.add_stream(stream)
    
    receiver_task = asyncio.create_task(mux.receive_loop())

    async def sender_task():
        for i in range(5):
            stream.send(i)
            await asyncio.sleep(0.01) # Allow context switching

    sender = asyncio.create_task(sender_task())
    await sender

    # Allow receiver time to process last items
    await asyncio.sleep(0.1)

    mux.deactivate()
    await asyncio.wait_for(receiver_task, timeout=1.0)

    assert received_data == [0, 1, 2, 3, 4]
    assert receiver_task.done()
    assert receiver_task.exception() is None


@pytest.mark.asyncio
async def test_multi_sender_multi_stream(num_streams=3, num_senders=5, duration_seconds=2):
    """Test multiple senders sending data to multiple streams randomly."""
    mux = GripStreamMux[tuple[str, int]]()
    
    # Dictionary to store received data per stream (using sets to check for duplicates)
    received_data_per_stream = defaultdict(set)
    # Dictionary to store the expected next sequence number per stream
    stream_counters = defaultdict(int)

    def create_processor(stream_id):
        def processor(data: list[(str, int)]):
            for d in data:
                assert d[0] not in received_data_per_stream[stream_id], \
                    f"Stream {stream_id} received duplicate data {d}"
                received_data_per_stream[stream_id].add(d)
        return processor

    streams: list[GripStream[tuple[str, int]]] = []
    for i in range(num_streams):
        stream = GripStream[tuple[str, int]](
            processor=create_processor(i))
        streams.append(stream)

    mux.activate()
    
    receiver_task = asyncio.create_task(mux.receive_loop())
    
    for stream in streams:
        mux.add_stream(stream)


    async def sender_task(sender_id: str):
        # Loop while the multiplexer is active
        while mux.active:
            # Select a random stream
            target_stream_index = random.randrange(num_streams)
            target_stream = streams[target_stream_index]
            
            # Atomically get and increment the counter for the chosen stream
            # Note: This simple increment isn't strictly atomic across async tasks 
            # without a lock, but for testing stream integrity (no duplicates, 
            # no gaps per stream), it's sufficient as the processor checks that.
            data_to_send = stream_counters[target_stream_index]
            stream_counters[target_stream_index] += 1
            
            try:
                target_stream.send((data_to_send, sender_id))
                # print(f"Sender {sender_id} sent {data_to_send} to stream {target_stream_index}")
            except Exception as e:
                # Catch potential exceptions if send occurs exactly as mux deactivates
                print(f"Sender {sender_id} exception during send: {e}")
                pass # Ignore send errors during shutdown
                
            # Wait for a random time before sending again
            await asyncio.sleep(random.uniform(0, 0.1))
            
        # print(f"Sender {sender_id} finished.") # Optional: for debugging

    sender_tasks = [asyncio.create_task(sender_task(i)) for i in range(num_senders)]

    # Let senders run for the specified duration
    await asyncio.sleep(duration_seconds)

    # Now deactivate the multiplexer - this should signal senders to stop
    mux.deactivate()

    # Wait for the receiver task to finish (it exits when mux is inactive)
    await asyncio.wait_for(receiver_task, timeout=duration_seconds + 1.0) # Adjust timeout
    
    # Also wait for sender tasks to cleanly exit their loops
    await asyncio.gather(*sender_tasks)

    assert receiver_task.done()
    assert receiver_task.exception() is None

    # Verification
    print("Received data counts per stream:", {k: len(v) for k, v in received_data_per_stream.items()})
    for stream_id, received_set in received_data_per_stream.items():
        if not received_set:
            # It's possible a stream received no data if duration is short and random sleeps are long
            print(f"Warning: Stream {stream_id} received no data.")
            continue 
        
        set_of_first_values = set(d[0] for d in received_set)
        max_received = max(set_of_first_values)
        expected_set = set(range(max_received + 1))
        assert set_of_first_values == expected_set, f"Stream {stream_id}: Missing or unexpected data. Expected sequence up to {max_received}. Received {len(received_set)} items."

if __name__ == "__main__":
    asyncio.run(test_multi_sender_multi_stream())

