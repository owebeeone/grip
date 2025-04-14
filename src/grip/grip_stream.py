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
def processor1(data: str):
    received.append(data)

stream1 = GripStream(processor1)
mux.add_stream(stream1)

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
from collections.abc import Callable
from typing import Generic, TypeVar, Optional
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
    managed by a GripStreamMux.
    """

    processor: Callable[[DType], None]
    mux: Optional["GripStreamMux"] = dtfield(default=None, repr=False)

    def send(self, data: DType) -> None:
        """
        Send data to the stream via its multiplexer's queue.
        Raises AttemptedSendOnInactive if the multiplexer is not active.
        """
        if self.mux is None or not self.mux.active:
            raise AttemptedSendOnInactive("Multiplexer is not active or stream not added")
        # The mux queue handles the async aspect
        self.mux.queue.put_nowait((self, data))


@datatree
class GripStreamMux(Generic[DType]):
    """
    Multiplexes data from multiple GripStreams using an asyncio.Queue.
    Routes incoming data to the appropriate stream's processor.
    """

    receiver_loop_error_handler: Callable[[Exception], None] = dtfield(
        default=lambda msg: ())
    # The central queue for receiving data from all streams
    queue: asyncio.Queue[tuple[GripStream[DType], DType] | object] = dtfield(
        default_factory=asyncio.Queue
    )
    # Keep track of streams added, maybe useful, though processors are stored implicitly
    streams: list[GripStream[DType]] = dtfield(default_factory=list)
    active: bool = dtfield(default=False)
    _receiver_task: Optional[asyncio.Task] = dtfield(default=None, repr=False)

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

    def add_stream(self, stream: GripStream[DType]) -> None:
        """
        Add a stream to the multiplexer.
        """
        if stream.mux is not None:
            # Use the more specific exception
            raise StreamAlreadyAddedError("Stream is already added to a multiplexer")
        if stream not in self.streams:
            stream.mux = self
            self.streams.append(stream)
        # No need to bump futures anymore

    async def _receive_loop(self):
        """
        Internal task that continuously receives data from the queue
        and calls the appropriate stream processor.
        Exits when a sentinel value is received or the mux becomes inactive.
        """
        while self.active:
            try:
                item = await self.queue.get()

                if item is _SENTINEL:
                    # print(f"Mux {id(self)} receiver loop received sentinel, exiting.")
                    self.queue.task_done()
                    break

                # Ensure mux didn't become inactive while waiting
                if not self.active:
                    # print(f"Mux {id(self)} became inactive while waiting, exiting loop.")
                    self.queue.task_done()  # Mark item done even if not processed
                    break

                source_stream, data = item

                try:
                    # Call the processor associated with the source stream
                    # NOTE: This processes items ONE BY ONE.
                    # If batching is desired, logic needs modification here.
                    # See previous discussion on batching alternatives.
                    source_stream.processor(data)
                except Exception as e:
                    # Handle processor errors gracefully
                    self.receiver_loop_error_handler(e)

                self.queue.task_done()  # Signal that this item is processed

            except asyncio.CancelledError as e:
                self.receiver_loop_error_handler(e)
            except Exception as e:
                # Catch potential errors during queue get or processing
                self.receiver_loop_error_handler(e)
                await asyncio.sleep(0.1)  # Avoid tight loop on persistent errors

