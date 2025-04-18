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
from typing import Generic, TypeVar, Optional, overload, Any, List, Set, Tuple
import inspect
from datatrees import datatree
from datatrees.datatrees import dtfield
from grip.grip_base import GripBaseException
from typing import TYPE_CHECKING  # Added for forward references


class AttemptedSendOnInactive(GripBaseException):
    """
    An exception raised when an attempt is made to send data on an inactive multiplexer.
    """


# AttemptedAddStreamToInactive might not be strictly needed if adding is allowed while inactive,
# but kept for potential future use or stricter policy.
class AttemptedAddStreamToInactive(GripBaseException):
    """
    An exception raised when an attempt is made to add a stream to an inactive multiplexer.
    """


class StreamAlreadyAddedError(GripBaseException):
    """
    An exception raised when attempting to add a stream that is already associated with a multiplexer.
    """


class UnexpectedMessageReceived(GripBaseException):
    """
    An exception raised when an unexpected message is received in the mux queue.
    """


DType = TypeVar("DType")
_SENTINEL = object()  # Sentinel value to signal queue shutdown
_DIRTY_STREAMS = object()

if TYPE_CHECKING:
    # Add forward references for renamed/new classes
    class StreamProcessor(Generic[DType]):
        pass

    # Add StreamScope back
    class StreamScope(Generic[DType]):
        pass


@datatree(eq=False)
class GripStream(Generic[DType]):
    """
    Represents a stream of data directed towards a specific processor (via a StreamScope).
    Buffers data internally and notifies via callback.
    Processor MUST be awaitable.
    """

    # Callback now points to StreamScope._notify_processor_of_dirty_stream
    _notify_dirty_callback: Optional[Callable[["GripStream[DType]"], Awaitable[None]]] = dtfield(
        default=None, init=False, repr=False
    )
    _scope_ref: Optional["StreamScope[DType]"] = dtfield(default=None, init=False, repr=False)

    stream_processor_func: Callable[[DType], Awaitable[None]] = dtfield(default=None, repr=False)
    latest_only: bool = dtfield(default=False)
    skip_duplicates: bool = dtfield(default=False)

    _buffer: Optional[List[DType]] = dtfield(default=None, init=False, repr=False)
    _lock: asyncio.Lock = dtfield(default_factory=asyncio.Lock, init=False, repr=False)
    _last_value_sent: Optional[DType] = dtfield(default=None, init=False, repr=False)
    _has_sent_value: bool = dtfield(default=False, init=False, repr=False)

    async def send(self, data: DType) -> None:
        notify_callback = self._notify_dirty_callback
        # No longer need scope ref here, callback handles it
        if notify_callback is None:
            raise AttemptedSendOnInactive("Stream is not attached to an active processor/scope.")

        needs_notification = False
        update_buffer_and_notify = True
        async with self._lock:
            if self.skip_duplicates and self._has_sent_value and self._last_value_sent == data:
                update_buffer_and_notify = False
            if update_buffer_and_notify:
                self._has_sent_value = True
                self._last_value_sent = data
                buffer_was_none = self._buffer is None
                if self.latest_only:
                    self._buffer = [data]
                else:
                    if buffer_was_none:
                        self._buffer = [data]
                    elif self._buffer is not None:
                        self._buffer.append(data)
                needs_notification = buffer_was_none
        if needs_notification:
            # Call the scope's notification method, passing self
            await notify_callback(self)


@datatree
class StreamProcessor(Generic[DType]):
    """
    An asynchronous processing engine for GripStreams.
    Manages a dedicated task and queue to execute stream processor callbacks
    asynchronously. Stream management is delegated to StreamScope.
    """

    receiver_loop_error_handler: Callable[[Exception], None] = dtfield(
        default=lambda msg: print(f"StreamProcessor Error: {msg!r}")
    )
    queue: asyncio.Queue[object] = dtfield(default_factory=asyncio.Queue)
    active: bool = dtfield(default=False)
    _receiver_task: Optional[asyncio.Task] = dtfield(default=None, init=False, repr=False)
    queue_backup_metric: int = dtfield(default=0, init=False)
    _lock: asyncio.Lock = dtfield(default_factory=asyncio.Lock, init=False, repr=False)
    # Dirty set now stores tuples
    _dirty_streams: Optional[Set[Tuple[GripStream[DType], "StreamScope[DType]"]]] = dtfield(
        default=None, init=False, repr=False
    )

    def activate(self):
        if self.active and self._receiver_task and not self._receiver_task.done():
            return
        self.active = True
        if self._receiver_task and self._receiver_task.done():
            self.queue = asyncio.Queue()
            self._dirty_streams = None
            self.queue_backup_metric = 0
        self._receiver_task = asyncio.create_task(
            self._receive_loop(), name=f"StreamProcessor-Receiver-{id(self)}"
        )

    def deactivate(self):
        self.active = False
        if self._receiver_task and not self._receiver_task.done():
            self.queue.put_nowait(_SENTINEL)

    async def wait_until_stopped(
        self, timeout: Optional[float] = None
    ) -> Tuple[bool, Optional[Exception]]:
        if not self._receiver_task:
            return True, None
        if self._receiver_task.done():
            try:
                exc = self._receiver_task.exception()
                return True, exc
            except asyncio.CancelledError as e:
                return True, e
            except Exception as e:
                return False, e
        try:
            await asyncio.wait_for(asyncio.shield(self._receiver_task), timeout=timeout)
            exc = self._receiver_task.exception()
            return True, exc
        except asyncio.TimeoutError as e:
            print(f"Processor {id(self)} wait_until_stopped timed out after {timeout}s.")
            return False, e
        except asyncio.CancelledError as e:
            print(f"Processor {id(self)} wait_until_stopped cancelled.")
            return False, e
        except Exception as e:
            print(f"Processor {id(self)} wait_until_stopped encountered unexpected error: {e}")
            return False, e

    async def _add_dirty_stream(
        self, stream: GripStream[DType], scope: "StreamScope[DType]"
    ) -> None:
        # This method is called by StreamScope after it updates its count
        if not self.active:
            print(
                f"Warning: _add_dirty_stream called on inactive processor {id(self)} by scope {id(scope)}."
            )
            return
        put_marker = False
        async with self._lock:
            if self._dirty_streams is None:
                self._dirty_streams = set()
                put_marker = True
            self._dirty_streams.add((stream, scope))
        if put_marker:
            try:
                self.queue.put_nowait(_DIRTY_STREAMS)
            except asyncio.QueueFull:
                print("WARNING: StreamProcessor queue full when trying to notify _DIRTY_STREAMS.")

    async def _receive_loop(self):
        should_stop = False
        try:
            while not should_stop:
                streams_to_process_now: Optional[
                    Set[Tuple[GripStream[DType], StreamScope[DType]]]
                ] = None
                try:
                    msg = await self.queue.get()
                    if msg is _SENTINEL:
                        self.active = False
                        self.queue.task_done()
                        async with self._lock:
                            if self._dirty_streams:
                                streams_to_process_now = self._dirty_streams
                                self._dirty_streams = None
                        if self.queue.empty() and not streams_to_process_now:
                            should_stop = True
                    elif msg is _DIRTY_STREAMS:
                        self.queue.task_done()
                        async with self._lock:
                            if self._dirty_streams is not None:
                                streams_to_process_now = self._dirty_streams
                                self._dirty_streams = None
                    else:
                        self.receiver_loop_error_handler(
                            UnexpectedMessageReceived(f"Invalid message in queue: {msg!r}")
                        )
                        self.queue.task_done()
                        continue

                    if not streams_to_process_now:
                        if not self.active and self.queue.empty():
                            should_stop = True
                        continue

                    for source_stream, source_scope in streams_to_process_now:
                        if not isinstance(source_stream, GripStream) or not isinstance(
                            source_scope, StreamScope
                        ):
                            self.receiver_loop_error_handler(
                                TypeError(
                                    "Invalid item in dirty set tuple: "
                                    f"{(source_stream, source_scope)}"
                                )
                            )
                            continue

                        buffer_to_process: Optional[List[DType]] = None
                        processed_item_count = 0
                        try:
                            # Lock stream, get buffer, clear buffer
                            async with source_stream._lock:
                                buffer_to_process = source_stream._buffer
                                source_stream._buffer = None
                            # Process buffer outside lock
                            if buffer_to_process:
                                try:
                                    batch_size = len(buffer_to_process)
                                    if batch_size > 1:
                                        self.queue_backup_metric += batch_size - 1
                                    for data_item in buffer_to_process:
                                        await source_stream.stream_processor_func(data_item)
                                        processed_item_count += 1  # Increment only if processor ran
                                except Exception as e:
                                    self.receiver_loop_error_handler(e)
                        finally:
                            # Acknowledge processing in the scope if items were processed
                            if processed_item_count > 0:
                                try:
                                    await source_scope._acknowledge_processing(source_stream)
                                except Exception as ack_err:
                                    # Log error during acknowledgement
                                    self.receiver_loop_error_handler(
                                        RuntimeError(
                                            "Error acknowledging scope "
                                            f"{id(source_scope)} for stream "
                                            f"{id(source_stream)}: {ack_err!r}"
                                        )
                                    )

                    # Check stop condition after processing batch
                    if not self.active and self.queue.empty():
                        async with self._lock:
                            if self._dirty_streams is None:
                                should_stop = True
                except asyncio.CancelledError:
                    should_stop = True
                    raise
                except Exception as loop_err:
                    self.receiver_loop_error_handler(loop_err)
                    self.queue.task_done()
                    await asyncio.sleep(0.1)
        finally:
            self.active = False


# --- StreamScope Class (Implement Drain Logic) ---


@datatree(eq=False)
class StreamScope(Generic[DType]):
    """
    Manages a logical group of GripStreams and associates them
    with a specific StreamProcessor, including a drain mechanism.
    """

    processor: "StreamProcessor[DType]"
    _streams: Set[GripStream[DType]] = dtfield(
        default_factory=set, init=False, repr=False)
    _lock: asyncio.Lock = dtfield(
        default_factory=asyncio.Lock, init=False, repr=False
    )  # Protects _streams and count/event
    _pending_processing_count: int = dtfield(default=0, init=False, repr=False)
    _drain_event: asyncio.Event = dtfield(
        default_factory=asyncio.Event, init=False, repr=False)

    def __post_init__(self):
        """Initializes the drain event to indicate initially drained state."""
        self._drain_event.set()  # Start drained

    # --- Stream Creation / Management moved here ---
    @overload
    async def create_stream(
        self,
        stream_processor_func: Callable[[DType], Awaitable[None]],
        *,
        latest_only: bool = False,
        skip_duplicates: bool = False,
    ) -> GripStream[DType]: ...

    @overload
    async def create_stream(
        self,
        stream_processor_func: Callable[[DType], None],
        *,
        latest_only: bool = False,
        skip_duplicates: bool = False,
    ) -> GripStream[DType]: ...

    async def create_stream(
        self,
        stream_processor_func: Callable[..., Any],
        *,
        latest_only: bool = False,
        skip_duplicates: bool = False,
    ) -> GripStream[DType]:
        """
        Factory method to create a new GripStream, associate it with this scope,
        and configure it to notify this scope's processor. Supports sync/async processors.
        This method is ASYNCHRONOUS.
        """
        if not self.processor.active:
            raise AttemptedSendOnInactive(
                f"Cannot create stream: StreamProcessor {id(self.processor)} is not active."
            )

        final_processor_func: Callable[[DType], Awaitable[None]]
        if inspect.iscoroutinefunction(stream_processor_func):
            final_processor_func = stream_processor_func
        else:

            async def async_wrapper(data: DType):
                try:
                    stream_processor_func(data)
                except Exception as e:
                    print(f"Error in sync processor wrapper: {e}")
                    raise
                await asyncio.sleep(0)

            final_processor_func = async_wrapper

        new_stream = GripStream[DType](
            stream_processor_func=final_processor_func,
            latest_only=latest_only,
            skip_duplicates=skip_duplicates,
        )
        # Add asynchronously, handling lock
        await self.add_stream(new_stream)
        return new_stream

    # add_stream sets the NEW notification callback
    async def add_stream(self, stream: GripStream[DType]):
        if not self.processor.active:
            raise AttemptedSendOnInactive(
                f"Cannot add stream: StreamProcessor {id(self.processor)} is not active."
            )
        if stream._notify_dirty_callback is not None:
            raise StreamAlreadyAddedError(
                f"Stream {id(stream)} already has a notification callback."
            )

        async with self._lock:
            if stream._notify_dirty_callback is not None:
                raise StreamAlreadyAddedError(
                    f"Stream {id(stream)} already has a notification callback (race condition)."
                )
            if stream in self._streams:
                return  # Already added

            stream._scope_ref = self
            # Set callback to the scope's method which handles count/event
            stream._notify_dirty_callback = self._notify_processor_of_dirty_stream
            self._streams.add(stream)

    # remove_stream clears the callback
    async def remove_stream(self, stream: GripStream[DType]):
        """
        Removes a stream from this scope, detaching it from the processor.
        Asynchronous to handle locking safely.
        """
        async with self._lock:
            if stream in self._streams:
                stream._scope_ref = None
                stream._notify_dirty_callback = None  # Clear scope callback
                self._streams.remove(stream)
                # Note: Processor might still process one last notification
                # if removal happens just after send(). The check in receive_loop handles this.

    # --- Drain Methods Implemented ---

    async def _notify_processor_of_dirty_stream(self, stream: GripStream[DType]):
        """Internal callback from GripStream.send(). Increments count and notifies processor."""
        should_notify_processor = False
        async with self._lock:
            # Check if stream is still genuinely part of this scope (handles remove race)
            if stream in self._streams:
                # Increment pending count and clear drain event *only if processor active*
                if self.processor.active:
                    self._pending_processing_count += 1
                    self._drain_event.clear()
                    should_notify_processor = True
                # else: If processor inactive, don't increment or notify
            # else: If stream removed, don't increment or notify

        # Notify processor outside scope lock
        if should_notify_processor:
            try:
                # Pass self (the scope) to the processor registration method
                await self.processor._add_dirty_stream(stream, self)
            except AttemptedSendOnInactive:
                # Processor became inactive between check and call. Revert count.
                print(
                    f"Warning: Processor {id(self.processor)} deactivated during dirty registration for stream {id(stream)}."
                )
                async with self._lock:
                    if stream in self._streams:  # Check again before decrementing
                        self._pending_processing_count -= 1
                        if self._pending_processing_count == 0:
                            self._drain_event.set()
            except Exception as reg_err:
                print(
                    f"ERROR: Failed registering dirty stream {id(stream)} with processor: {reg_err!r}"
                )
                # Revert count if registration fails
                async with self._lock:
                    if stream in self._streams:  # Check again
                        self._pending_processing_count -= 1
                        if self._pending_processing_count == 0:
                            self._drain_event.set()

    async def _acknowledge_processing(self, stream: GripStream[DType]):
        """
        Internal callback from StreamProcessor after processing. Decrements count.
        """
        async with self._lock:
            # Check if stream still part of scope when acknowledgement arrives
            if stream in self._streams:
                if self._pending_processing_count > 0:
                    self._pending_processing_count -= 1
                    if self._pending_processing_count == 0:
                        self._drain_event.set()
                else:
                    # This indicates a logic error (e.g., ack without notification)
                    print(
                        f"WARNING: Scope {id(self)} received unexpected processing acknowledgement for stream {id(stream)}; count already zero."
                    )
            # else: If stream was removed after processing started but before ack, ignore.

    async def drain(self, timeout: Optional[float] = None):
        """
        Deactivates all streams currently managed by this scope by removing
        their notification callbacks, preventing further sends through this scope.
        Then, waits until the pending processing count for items sent *before*
        deactivation reaches zero.
        """
        # Deactivate streams under lock
        async with self._lock:
            streams_to_deactivate = set(self._streams)
            for stream in streams_to_deactivate:
                stream._notify_dirty_callback = None  # Prevent future sends via this scope
                stream._scope_ref = None
            self._streams.clear()  # Clear the scope's own list

        # Now wait for pending operations (initiated before deactivation) to complete
        if self._pending_processing_count == 0 and self._drain_event.is_set():
            return
        try:
            await asyncio.wait_for(self._drain_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            # Check count again under lock for accurate reporting (though lock is now less critical for count)
            # We don't re-acquire the scope lock here for count check, as new items aren't added.
            # However, the processor might still be decrementing the count via _acknowledge_processing.
            # Reading it without lock is slightly racy but acceptable for error message.
            pending_count = self._pending_processing_count
            print(
                f"WARNING: Drain timed out for scope {id(self)} "
                f"after {timeout}s. Still pending: {pending_count}"
            )
            raise

    def __eq__(self, other: Any) -> bool:
        return self is other

    def __hash__(self) -> int:
        return id(self)
