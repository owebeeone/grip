from __future__ import annotations
from abc import ABC, abstractmethod
import asyncio
import weakref
from typing import Any, AsyncIterator, Awaitable, ClassVar, Generic, TypeVar
from datatrees import datatree, dtfield

T = TypeVar("T")


@datatree
class Drip(ABC, Generic[T]):
    """
    A reactive value stream associated with a specific GripKey and context.
    Delegates data flow to its current DripFeeder.
    """
    _drip_id: int = dtfield(init=False, repr=False)
    _drip_id_counter: ClassVar[int] = 0

    def __post_init__(self):
        cls = self.__class__
        self._drip_id = cls._drip_id_counter
        cls._drip_id_counter += 1

    @abstractmethod
    def snapshot(self) -> T:
        """
        Get the current value of the Drip.
        """
        pass

    @abstractmethod
    def __aiter__(self) -> AsyncIterator[T]:
        """
        Get an async iterator over the Drip.
        """
        pass

    @abstractmethod
    def attach(self, feeder: "DripFeeder[T]"):
        """Attach this Drip to a new DripFeeder."""
        pass
    
    @abstractmethod
    def on_change(self) -> Awaitable[None]:
        """
        Wait for the next change from the DripFeeder.
        """
        pass
    
class DripFeedObserver(ABC):
    """
    A class that observes a DripFeeder and can be notified when it changes.
    """
    @abstractmethod
    def on_feeder_change(self, feeder: "DripFeeder[T]") -> None:
        pass

@datatree
class DripImpl(Drip[T]):
    _feeder: DripFeeder[T] = dtfield(default=None)
    _cached_value: T = dtfield(default=None, init=False)
    _event: asyncio.Event = dtfield(default_factory=asyncio.Event, init=False, repr=False)
    _waiters: list[asyncio.Future] = dtfield(default_factory=list, init=False, repr=False)
    _version: int = dtfield(default=0, init=False, repr=False)
    _last_seen_version: int = dtfield(default=0, init=False, repr=False)
    _controller: DripFeedObserver = dtfield(default=None, init=False)

    def _notify(self):
        self._version += 1
        self._cached_value = self._feeder.snapshot()
        self._event.set()
        while self._waiters:
            fut = self._waiters.pop()
            if not fut.done():
                fut.set_result(self._cached_value)

    def detach(self):
        if self._feeder:
            self._feeder.detach(self)
            self._feeder = None

    async def __aiter__(self) -> AsyncIterator[T]:
        yield self.snapshot()  # Initial value
        while True:
            self._event.clear()  # Reset the event
            await self._event.wait()  # Wait for the event to be set
            yield self.snapshot()  # Yield the new value

    def attach(self, feeder: "DripFeeder[T]"):
        if self._feeder:  # In theory this is never None
            self._feeder.detach(self)
        feeder.attach(self)
        self._cached_value = feeder.snapshot()
        self._feeder = feeder
        if self._controller is not None:
            self._controller.on_feeder_change(feeder)
        self._notify()

    def on_change(self) -> Awaitable[T]:
        if self._last_seen_version != self._version:
            self._last_seen_version = self._version
            return asyncio.sleep(0, result=self.snapshot())

        fut = asyncio.get_event_loop().create_future()
        self._waiters.append(fut)
        return fut
    
    def snapshot(self) -> T:
        return self._cached_value


@datatree
class DripFeeder(Generic[T]):
    """
    A producer of Drips. Can push values to all bound Drips.
    Supports dynamically swapping in new sources for a context.
    """

    _latest: T
    _drips: dict[int, weakref.ref[DripImpl]] = dtfield(default_factory=dict, repr=False)
    _waiters: list[asyncio.Future] = dtfield(default_factory=list, repr=False)
    _cleanup_counter: int = dtfield(default=0, init=False, repr=False)
    
    CLEANUP_INTERVAL: ClassVar[int] = 10  # cleanup every 10 pushes

    def snapshot(self) -> T:
        return self._latest

    def push(self, value: T):
        self._latest = value
        dead_keys = []
        for key, dripref in self._drips.items():
            drip = dripref()
            if drip:
                drip._notify()
            else:
                dead_keys.append(key)
        for key in dead_keys:
            self._drips.pop(key)
            
        self._waiters = [w for w in self._waiters if not w.done()]
        for fut in self._waiters:
            fut.set_result(None)
        self._waiters.clear()

    def create_drip(self) -> DripImpl[T]:
        drip = DripImpl[T](self)
        drip.attach(self)
        return drip

    def attach(self, drip: DripImpl[T]):
        self._drips[drip._drip_id] = weakref.ref(drip)

    def detach(self, drip: DripImpl[T]):
        self._drips.pop(drip._drip_id, None)
        self._cleanup_counter += 1
        if self._cleanup_counter >= self.CLEANUP_INTERVAL:
            self._cleanup_dead_drips()
    
    def _cleanup_dead_drips(self):
        dead_keys = []
        for key, dripref in self._drips.items():
            drip = dripref()
            if drip is None:
                dead_keys.append(key)
        for key in dead_keys:
            self._drips.pop(key)


@datatree
class ConstantDripFeeder(DripFeeder[T]):
    """
    A drip feeder that never changes — emits a single static value.
    Useful for default values or simple static key mappings.
    """

    def __init__(self, value: T):
        super().__init__(value)

    def push(self, value: T):
        raise RuntimeError("Cannot push to ConstantDripFeeder")


@datatree
class PushDripFeeder(DripFeeder[T]):
    """
    A manually controlled drip feeder for tests or externally driven updates.
    Allows values to be pushed into the stream.
    """


@datatree(eq=False)
class DripController(DripFeedObserver, Generic[T]):
    """
    DripController maintains a set of DripImpl instances tied to a shared source.

    It acts as a manager for distributing data from a single DripFeeder to multiple Drips.
    When the source (a DripFeeder) is updated via `update_source()`, all currently tracked
    Drips are re-attached to the new feeder and will receive updates from it.

    If an external actor manually attaches a different feeder to a Drip, the controller
    can be notified to remove that Drip from its internal tracking to avoid conflicts.
    """
    parent_feeder: DripFeeder[T] 
    _controlled_drips: dict[int, weakref.ReferenceType[DripImpl[T]]] = dtfield(default_factory=dict, init=False)
    _controller_id: int = dtfield(self_default=id, init=False)
    _controller_id_counter: ClassVar[int] = 0

    def __post_init__(self):
        cls = self.__class__
        self._controller_id = cls._controller_id_counter
        cls._controller_id_counter += 1

    def create_drip(self) -> DripImpl[T]:
        feeder: DripFeeder[T]  = self.parent_feeder
        if feeder is None:
            raise RuntimeError("DripController must have an initial source drip before creating drips.")
        drip = feeder.create_drip()
        self._controlled_drips[drip._drip_id] = weakref.ref(drip)
        return drip

    def on_feeder_change(self, new_feeder: DripFeeder[T]):
        self.parent_feeder = new_feeder
        dead_keys = []
        for key, ref in self._controlled_drips.items():
            drip = ref()
            if drip is not None:
                drip.attach(new_feeder)
            else:
                dead_keys.append(key)
        for key in dead_keys:
            self._controlled_drips.pop(key)

    def remove_if_detached(self, drip: DripImpl[T]):
        # Called optionally by DripImpl when external attach happens
        if drip._drip_id in self._controlled_drips:
            self._controlled_drips.pop(drip._drip_id)
            
    def __len__(self):
        return len(self._controlled_drips)
            

def get_or_set_drip_controller(drip: DripImpl[T]) -> DripController[T]:
    """
    Get or set the DripController for a DripImpl.
    """
    if drip._controller is None:
        drip._controller = DripController(drip._feeder)
    return drip._controller

def new_controlled_drip(drip: DripImpl[T]) -> DripImpl[T]:
    """
    Create a new DripImpl that is managed by a DripController.
    """
    controller = get_or_set_drip_controller(drip)
    return controller.create_drip()
