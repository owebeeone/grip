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
    """

    processor: Callable[[DType], Awaitable[None]]
    mux: Optional["GripStreamMux"] = dtfield(default=None, repr=False)
    # Internal buffer for this stream
    _buffer: Optional[List[DType]] = dtfield(default=None, init=False, repr=False)
    # Lock to protect access to the buffer during send/receive operations
    _lock: asyncio.Lock = dtfield(default_factory=asyncio.Lock, init=False, repr=False)

    async def send(self, data: DType) -> None:
        """
        Add data to the stream's internal buffer.
        If the buffer was previously empty (None), notify the multiplexer.
        Raises AttemptedSendOnInactive if the multiplexer is not active.
        """
        if self.mux is None or not self.mux.active:
            raise AttemptedSendOnInactive("Multiplexer is not active or stream not added")
        
        needs_notification = False
        async with self._lock:
            if self._buffer is None:
                self._buffer = []
                needs_notification = True
            self._buffer.append(data)
            
        if needs_notification:
            # Put self onto the queue to signal data is ready
            # Use put_nowait as send is often called from sync contexts 
            # or tasks that shouldn't block on the queue put.
            # Handle QueueFull exception if necessary, though less likely with 
            # single item notifications per stream burst.
            try:
                self.mux.queue.put_nowait(self) 
            except asyncio.QueueFull:
                # This case is less likely now, but could happen if many streams 
                # become ready simultaneously and the receiver is slow. 
                # How to handle? Log? Drop? Retry? 
                # For now, let's log and potentially drop/re-raise.
                print(f"WARNING: Mux queue full when trying to notify for stream {self}. Data might be delayed/lost.")
                # Optionally re-raise or implement retry logic
                # raise # Or: await self.mux.queue.put(self) # If send itself becomes async


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

    # Overload signature for async processors
    @overload
    def create_stream(self, processor: Callable[[DType], Awaitable[None]]) -> GripStream[DType]:
        ...

    # Overload signature for sync processors
    @overload
    def create_stream(self, processor: Callable[[DType], None]) -> GripStream[DType]:
        ...

    # Single implementation handling both cases
    def create_stream(self, processor: Callable[..., Any]) -> GripStream[DType]:
        """
        Factory method to create a new GripStream, add it to this mux,
        and set its processor. Supports both sync and async processors.
        Returns the created stream.
        """
        final_processor: Callable[[DType], Awaitable[None]]

        if inspect.iscoroutinefunction(processor):
            # If the provided processor is already async, use it directly
            final_processor = processor
        else:
            # If it's sync, wrap it in an async function
            async def async_wrapper(data: DType):
                processor(data)
                # Yield control briefly to avoid tight loop
                await asyncio.sleep(0)
            final_processor = async_wrapper

        new_stream = GripStream[DType](processor=final_processor)
        self._add_stream(new_stream) # Sets mux reference and adds to list
        return new_stream

    async def _receive_loop(self):
        """
        Internal task that receives ready GripStream instances from the queue,
        retrieves and clears their buffers, and processes the buffered data.
        """
        while self.active:
            buffer_to_process: Optional[List[DType]] = None
            source_stream: Optional[GripStream[DType]] = None
            try:
                # Get the stream instance that has data ready
                item = await self.queue.get()

                if item is _SENTINEL:
                    self.queue.task_done()
                    break 

                if not self.active:
                    self.queue.task_done() 
                    break

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
                        #     print(f"WARNING: Processing {len(buffer_to_process)} items from stream {source_stream}")
                        # Process items one by one from the retrieved buffer
                        for data_item in buffer_to_process:
                            await source_stream.processor(data_item) 
                    except Exception as e:
                        self.receiver_loop_error_handler(e)
                        # Continue processing remaining items in batch? Or stop?
                        # Current: Continues loop to get next stream notification.
                
                self.queue.task_done() 

            except asyncio.CancelledError as e:
                self.receiver_loop_error_handler(e)
                break 
            except Exception as e:
                 self.receiver_loop_error_handler(e)
                 await asyncio.sleep(0.1) 
