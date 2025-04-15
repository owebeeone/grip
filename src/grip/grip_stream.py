"""
Basic stream multiplexing implementation using asyncio.Queue.

The GripStream class represents a conceptual stream associated with a processor.
The GripStreamMux class multiplexes data from multiple GripStreams onto a single
asyncio.Queue and processes it in a central loop.

Producers call `stream.send(data)`, which puts the data onto the central queue.
The multiplexer's `receive_loop` consumes data from the queue and routes it
to the appropriate stream's processor.

Activation starts the receiver loop. Deactivation stops it.

Example:

mux = GripStreamMux[str]()
mux.activate() # Start receiver loop in background

received = []
async def processor1(data: str):
    await asyncio.sleep(0.01) # Can perform async operations
    received.append(data)

stream1 = mux.create_stream(processor1)

async def sender():
    stream1.send("hello")
    stream1.send("world")

asyncio.create_task(sender())
await asyncio.sleep(0.1) # Allow time for processing
mux.deactivate()
await mux.wait_until_stopped() # Ensure receiver loop finishes
print(received) # Output: ['hello', 'world']

"""

import asyncio
from collections.abc import Callable, Awaitable
from typing import Generic, TypeVar, Optional, overload, Any, List
import inspect
from datatrees import datatree
from datatrees.datatrees import dtfield
from grip.grip_base import GripBaseException


class AttemptedSendOnInactive(GripBaseException):
    """
    An exception raised when an attempt is made to send data on an inactive multiplexer.
    """

    pass


# AttemptedAddStreamToInactive might not be strictly needed if adding is allowed while inactive,
# but kept for potential future use or stricter policy.
class AttemptedAddStreamToInactive(GripBaseException):
    """
    An exception raised when an attempt is made to add a stream to an inactive multiplexer.
    """

    pass


class StreamAlreadyAddedError(GripBaseException):
    """
    An exception raised when attempting to add a stream that is already associated with a multiplexer.
    """

    pass


DType = TypeVar("DType")
_SENTINEL = object()  # Sentinel value to signal queue shutdown


@datatree
class GripStream(Generic[DType]):
    """
    Represents a stream of data directed towards a specific processor,
    managed by a GripStreamMux. Buffers data internally and notifies mux.
    Processor MUST be awaitable.

    Args:
        processor: The awaitable function to process incoming data.
        latest_only: If True, only keep the latest message, discarding previous ones
                     in the buffer upon receiving a new message.
        skip_duplicates: If True, do not store or notify if the incoming message
                         is the same as the last message already in the buffer.
    """

    processor: Callable[[DType], Awaitable[None]]
    latest_only: bool = dtfield(default=False)
    skip_duplicates: bool = dtfield(default=False)

    mux: Optional["GripStreamMux"] = dtfield(default=None, repr=False)
    # Internal buffer for this stream
    _buffer: Optional[List[DType]] = dtfield(default=None, init=False, repr=False)
    # Lock to protect access to the buffer during send/receive operations
    _lock: asyncio.Lock = dtfield(default_factory=asyncio.Lock, init=False, repr=False)
    # Internal state to track the last value accepted by send()
    _last_value_sent: Optional[DType] = dtfield(default=None, init=False, repr=False) 
    # Flag to track if _last_value_sent has been set at least once
    _has_sent_value: bool = dtfield(default=False, init=False, repr=False) 

    async def send(self, data: DType) -> None:
        """
        Add data to the stream's internal buffer according to options.
        Skips if skip_duplicates is True and data matches the last sent value.
        Notify the multiplexer if the buffer transitions from empty to non-empty.
        Raises AttemptedSendOnInactive if the multiplexer is not active.
        """
        if self.mux is None or not self.mux.active:
            raise AttemptedSendOnInactive("Multiplexer is not active or stream not added")

        needs_notification = False
        update_buffer_and_notify = True # Assume we proceed unless skipped

        async with self._lock:
            # 1. Check for duplicates against the *last accepted* value for sending
            if self.skip_duplicates \
                and self._has_sent_value \
                and self._last_value_sent == data:
                update_buffer_and_notify = False # Skip update and notification
            
            if update_buffer_and_notify:
                # Mark that we have now sent a value
                self._has_sent_value = True 
                # Store the latest value intended for processing
                self._last_value_sent = data 

                buffer_was_none = self._buffer is None

                # 2. Update buffer logic
                if self.latest_only:
                    # Always store only the latest value
                    self._buffer = [data]
                else:
                    # Standard buffering: create if None, else append
                    if buffer_was_none:
                        self._buffer = [data]
                    else:
                        # Buffer is guaranteed to be a list here
                        self._buffer.append(data)

                # 3. Determine if notification is needed (only if buffer was initially None)
                needs_notification = buffer_was_none
            
            # else: If update_buffer_and_notify is False, buffer remains unchanged

        # --- End Lock ---

        if needs_notification:
            try:
                self.mux.queue.put_nowait(self)
            except asyncio.QueueFull:
                print(f"WARNING: Mux queue full when trying to notify for stream {self}. Data might be delayed/lost.")


@datatree
class GripStreamMux(Generic[DType]):
    """
    Multiplexes streams using notifications. Receives stream objects, 
    processes their internal buffers.
    """

    receiver_loop_error_handler: Callable[[Exception], None] = dtfield(default=lambda msg: ())
    # Queue now holds GripStream instances needing processing
    queue: asyncio.Queue[GripStream[DType] | object] = dtfield(
        default_factory=asyncio.Queue
    )
    streams: list[GripStream[DType]] = dtfield(default_factory=list)
    active: bool = dtfield(default=False)
    _receiver_task: Optional[asyncio.Task] = dtfield(default=None, repr=False)
    queue_backup_metric: int = 0

    def activate(self):
        """
        Activate the multiplexer and start the receiver loop.
        Idempotent: Does nothing if already active.
        """
        if self.active and self._receiver_task and not self._receiver_task.done():
            return  # Already active

        self.active = True
        # Reset queue if reactivating after deactivation? Or assume continuation?
        # For simplicity, let's assume a fresh start on activate if previously stopped.
        if self._receiver_task and self._receiver_task.done():
            self.queue = asyncio.Queue()

        self._receiver_task = asyncio.create_task(
            self._receive_loop(), name=f"GripStreamMux-Receiver-{id(self)}"
        )

    def deactivate(self):
        """
        Signal the multiplexer to stop processing and shut down the receiver loop.
        Puts a sentinel on the queue to wake the receiver loop for shutdown.
        """
        self.active = False
        if self._receiver_task and not self._receiver_task.done():
            # Put sentinel to ensure the receiver loop wakes up and exits
            self.queue.put_nowait(_SENTINEL)

    async def wait_until_stopped(
        self, timeout: Optional[float] = None
    ) -> tuple[bool, Exception | None]:
        """
        Wait until the receiver loop task has finished.
        Useful for ensuring clean shutdown in tests or applications.
        Raises asyncio.TimeoutError if the timeout is exceeded.
        Returns a tuple of (success, exception).
        """
        if self._receiver_task and not self._receiver_task.done():
            try:
                await asyncio.wait_for(self._receiver_task, timeout=timeout)
            except asyncio.TimeoutError as e:
                return False, e
            except Exception as e:
                return False, e
        return True, None

    def _add_stream(self, stream: GripStream[DType]) -> None:
        """
        Add an existing GripStream instance to the multiplexer.
        Prefer using `create_stream` factory method.
        """
        if stream.mux is not None:
            # Use the more specific exception
            raise StreamAlreadyAddedError("Stream is already added to a multiplexer")
        if stream not in self.streams:
            stream.mux = self
            self.streams.append(stream)

    # Update overloads and implementation for create_stream
    @overload
    def create_stream(
        self,
        processor: Callable[[DType], Awaitable[None]],
        *, # Make options keyword-only
        latest_only: bool = False,
        skip_duplicates: bool = False
    ) -> GripStream[DType]:
        ...

    @overload
    def create_stream(
        self,
        processor: Callable[[DType], None],
        *, # Make options keyword-only
        latest_only: bool = False,
        skip_duplicates: bool = False
    ) -> GripStream[DType]:
        ...

    # Single implementation handling both cases and new options
    def create_stream(
        self,
        processor: Callable[..., Any],
        *,
        latest_only: bool = False,
        skip_duplicates: bool = False
    ) -> GripStream[DType]:
        """
        Factory method to create a new GripStream with options, add it to this mux,
        and set its processor. Supports both sync and async processors.

        Args:
            processor: The sync or async processor function.
            latest_only: Pass-through to GripStream.
            skip_duplicates: Pass-through to GripStream.

        Returns:
            The created stream.
        """
        final_processor: Callable[[DType], Awaitable[None]]

        if inspect.iscoroutinefunction(processor):
            final_processor = processor
        else:
            async def async_wrapper(data: DType):
                processor(data)
                await asyncio.sleep(0)
            final_processor = async_wrapper

        # Pass options to GripStream constructor
        new_stream = GripStream[DType](
            processor=final_processor,
            latest_only=latest_only,
            skip_duplicates=skip_duplicates
        )
        self._add_stream(new_stream)
        return new_stream

    async def _receive_loop(self):
        """
        Internal task that receives ready GripStream instances from the queue,
        retrieves and clears their buffers, and processes the buffered data.
        Continues draining the queue after deactivation until empty.
        """
        should_stop = False
        try:
            while not should_stop:
                buffer_to_process: Optional[List[DType]] = None
                source_stream: Optional[GripStream[DType]] = None
                try:
                    # Get the stream instance that has data ready
                    # Use a small timeout to prevent blocking forever if something 
                    # unexpected happens during shutdown and the queue never empties 
                    # (though it should).
                    item = await asyncio.wait_for(self.queue.get(), timeout=1.0) 

                    if item is _SENTINEL:
                        # Sentinel received. Stop accepting new work (already done by 
                        # deactivate) but continue processing items already in the queue.
                        self.active = False # Ensure flag is false
                        self.queue.task_done()
                        # If the queue is now empty, we can prepare to stop.
                        if self.queue.empty():
                           should_stop = True
                        continue # Continue loop to check queue again or exit

                    # Item should be a GripStream instance
                    if not isinstance(item, GripStream):
                        print(f"WARNING: Invalid item received in mux queue: {item}")
                        self.queue.task_done()
                        continue
                        
                    source_stream = item
                    
                    # Atomically get and clear the buffer
                    async with source_stream._lock:
                        buffer_to_process = source_stream._buffer
                        source_stream._buffer = None # Mark buffer as empty
                    
                    # Process the retrieved buffer if it wasn't empty
                    if buffer_to_process:
                        try:
                            if len(buffer_to_process) > 1:
                                self.queue_backup_metric += len(buffer_to_process) - 1
                            for data_item in buffer_to_process:
                                await source_stream.processor(data_item) 
                        except Exception as e:
                            self.receiver_loop_error_handler(e)
                    
                    self.queue.task_done() 

                except asyncio.TimeoutError:
                    # Timeout waiting for queue item. If mux is inactive, 
                    # assume queue is drained or stuck, prepare to stop.
                    if not self.active:
                        # print("Receiver loop timeout while inactive, stopping.")
                        should_stop = True
                    # If active, maybe just log and continue?
                    # else: print("Receiver loop timeout while active.")
                    continue
                except asyncio.CancelledError as e:
                    self.receiver_loop_error_handler(e)
                    should_stop = True # Exit loop if cancelled
                    # Do not call task_done() here, cancellation handles it
                    raise # Re-raise CancelledError
                except Exception as e:
                     self.receiver_loop_error_handler(e)
                     # Avoid tight loop on unexpected errors
                     await asyncio.sleep(0.1) 
                
                # Check exit condition after processing or handling exceptions/sentinel
                if not self.active and self.queue.empty():
                    should_stop = True
                    
        finally:
            # Ensure active is false upon any exit (normal, exception, cancellation)
            self.active = False
            # print("Receiver loop finished.")
