"""
Basic stream multiplexing implementation.

The GripStream class is a stream of data from a collection of GripKeys.
The GripStreamMux class is a multiplexer for a collection of GripStreams.

The GripStreamChunk is a chunk of data from a GripStream, it can contain
multiple collections of GripKeys/data maps.

You can first activate the multiplexer, then add streams to it. When data
is sent to a stream, it will be added to the chunk and the processing
function will be called with the GripKey/data map sent.

Deactivating the multiplexer will stop the processing loop and close the
streams.

It can be reactivated later to start processing again.

Here's a simple example:

globally:
    grip_stream_mux = GripStreamMux()
    grip_stream_mux.activate()

consumer task:
       for K=1..Kmax
    def streamK_processor(data: dict[GripKey, Any]) -> dict[GripKey, Any]:
        '''
        Process data from stream K
        '''
        print(data)
        return data

        grip_stream = GripStream(task1_processor)
        grip_stream_mux.add_stream(grip_stream)

producer tasks:
    for K=1..Kmax
    def sender_taskK():
        while grip_stream_mux.active:
            asyncio.sleep(random.uniform(0.1, 0.5))
            random_stream = random.choice(grip_stream_mux.streams)
            random_stream.send(GripKey("keyK"), "dataK")

# complete the example
grip_stream.deactivate()

"""
import asyncio
from collections.abc import Callable
from typing import Generic, TypeVar
from datatrees import datatree
from datatrees.datatrees import dtfield
from grip.grip_base import GripBaseException


class AttemptedSendOnInactive(GripBaseException):
    """
    An exception raised when an attempt is made to send data on an inactive stream.
    """
    pass

class AttemptedAddStreamToInactive(GripBaseException):
    """
    An exception raised when an attempt is made to add a stream to an inactive multiplexer.
    """
    pass

DType = TypeVar('DType')

@datatree(frozen=True)
class GripStreamChunk(Generic[DType]):
    """
    A stream of data from a GripKey.
    """
    stream: 'GripStream' = dtfield(default=None)
    future: asyncio.Future['GripStreamChunk'] = dtfield(
        default_factory=asyncio.Future)
    data_stream: list[DType] = dtfield(default_factory=list)

    def send(self, data: DType) -> None:
        """
        Send data to the stream.
        """
        self.data_stream.append(data)
        if not self.future.done():
            self.future.set_result(self)
            
    def consume(self, data: list[DType]) -> None:
        """
        Called by the receiver to consume data from the stream.
        """
        self.stream.new_chunk()
        
        while self.data_stream:
            data = self.data_stream.pop(0)
            self.stream.processor(data)


@datatree
class GripStream(Generic[DType]):
    """
    A stream of data from a GripKey.
    """
    processor: Callable[[DType], None]
    current_chunk: GripStreamChunk[DType] = None
    mux: 'GripStreamMux' = dtfield(default=None)
    
    def activate(self) -> None:
        """
        Activate the stream.
        """
        self.current_chunk = GripStreamChunk(self)

    def send(self, data: DType) -> None:
        """
        Send data to the stream.
        """
        if self.mux is not None and self.mux.active:
            self.current_chunk.send(data)
        else:
            raise AttemptedSendOnInactive("Stream is not active")
        
    def new_chunk(self) -> None:
        """
        Called by the receiver to indicate a new chunk of data is available.
        """
        self.current_chunk = GripStreamChunk(self)
        
    def deactivate(self) -> None:
        """
        Deactivate the stream.
        """
        if self.current_chunk is not None:
            self.current_chunk.future.cancel()
            self.current_chunk = None   
        
  
@datatree
class GripStreamMux(Generic[DType]):
    """
    A multiplexer for a collection of GripStreams.
    """
    streams: list[GripStream[DType]] = dtfield(default_factory=list)
    active_future: asyncio.Future[None] = dtfield(default_factory=asyncio.Future)
    
    active: bool = dtfield(default=False)
    
    def activate(self):
        """
        Activate the multiplexer.
        """
        for stream in self.streams:
            stream.activate()
        self.active_future = asyncio.Future()
        self.active = True
        
    def deactivate(self):
        """
        Deactivate the multiplexer.
        """
        self.active = False
        for stream in self.streams:
            stream.deactivate()
        active_future = self.active_future
        if active_future is not None:
            self.active_future = None
            active_future.set_result(None)

    def add_stream(self, stream: GripStream) -> None:
        """
        Add a stream to the multiplexer.
        """
        if not self.active:
            raise AttemptedAddStreamToInactive("Multiplexer is not active")
        stream.mux = self
        self.streams.append(stream)
        stream.activate()
        self._bump_active_future()
        
    def _bump_active_future(self):
        active_future = self.active_future
        self.active_future = asyncio.Future()
        if active_future is not None:
            active_future.set_result(None)
          
    async def receive_loop(self):
        """
        Continuously receive data from the multiplexer and process it.
        Terminates when the multiplexer is deactivated.
        """
        while True:
            active_future = self.active_future
            if not self.active or active_future is None or active_future.done():
                break
            current_chunks: list[GripStreamChunk] = [
                stream.current_chunk for stream in self.streams]
            
            current_futures = [active_future] + [
                chunk.future for chunk in current_chunks]
            done, pending = await asyncio.wait(
                current_futures, return_when=asyncio.FIRST_COMPLETED)
            
            for chunk in current_chunks:
                if chunk.future in done:
                    chunk.stream.processor(chunk.data_stream)



