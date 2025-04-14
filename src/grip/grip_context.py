from dataclasses import InitVar
# from datatrees import datatree, dtfield # Commented out dependency
from typing import Any
import weakref
from grip.dripfeeder import Drip, DripFeeder, DripImpl, new_controlled_drip
from grip.grip_core import GripRegistry, GripRegistry
import asyncio
import pytest
# from grip.dripfeeder import PushDripFeeder, ConstantDripFeeder # Commented out dependency
import sys


@datatree
class GripContextSnapshot:
    """A snapshot of a GripContext at a point in time."""
    values: dict[GripRegistry, Any]
    source: 'GripContext'


@datatree
class GripContext:
    """
    A context is a hierarchical mapping of GripKeys to DripFeeders.
    Contexts can inherit from a parent and override keys.
    """
    grip: GripRegistry
    # keys_requested: InitVar[list[GripKey] | None] = dtfield(default=None) # Commented out
    keys_requested: InitVar[list[GripRegistry] | None] = None # Replaced with simple default
    # parent: 'GripContext | None' = dtfield(default=None) # Commented out
    parent: 'GripContext | None' = None # Replaced with simple default
    # values: dict[GripKey, DripImpl] = dtfield(default_factory=dict) # Commented out
    values: dict[GripRegistry, DripImpl] = {} # Replaced with simple default
    # _children: list[weakref.ReferenceType['GripContext']] = dtfield(default_factory=list, repr=False) # Commented out
    _children: list[weakref.ReferenceType['GripContext']] = [] # Replaced with simple default
    
    def __post_init__(self, keys_requested: list[GripRegistry] | None):
        """Post-initialization hook.

        If keys were requested during initialization (typically when creating a child
        context) and a parent exists, pre-populate the context by getting those keys
        from the parent.
        """
        if self.parent and keys_requested:
            # Get the Drips from the parent hierarchy. parent.get() already returns
            # new controlled drips suitable for this context.
            parent_drips = self.parent.get(*keys_requested)
            self.values.update(parent_drips)

    def child(self, *keys_to_request: GripRegistry) -> 'GripContext': # Use varargs for keys
        """Create a child context, optionally requesting initial keys from the parent."""
        # Pass keys to the child's __post_init__ via keys_requested
        child = GripContext(grip=self.grip, parent=self, keys_requested=list(keys_to_request))
        self._children.append(weakref.ref(child))
        return child

    def set(self, spec: dict[GripRegistry, DripImpl]):
        """Associate a GripKey with a DripImpl in this context, potentially overriding parent values."""
        # TODO: Add logic to check if the underlying feeder is actually changing
        # to avoid unnecessary notifications.
        changed = False
        for k, new_drip in spec.items():
            if k not in self.values or self.values[k]._feeder is not new_drip._feeder:
                self.values[k] = new_drip
                changed = True
            # Else: Setting the same drip or a different drip with the same feeder,
            # no effective change for stream notification purposes.

        if changed:
             # TODO: Notify streams about the change
             pass

    def _find_inherited_feeder(self, key: GripRegistry) -> DripFeeder | None:
        """Search parent hierarchy for the DripFeeder associated with a key."""
        current_context = self.parent
        while current_context:
            # Directly access values, don't call get() to avoid new drip creation
            found_drip = current_context.values.get(key)
            if found_drip:
                # Assuming values store DripImpl which have _feeder
                return found_drip._feeder # Return the feeder of the first found drip
            current_context = current_context.parent
        return None # Not found in hierarchy

    def clear(self, key: GripRegistry, *args):
        """Revert specified keys to their inherited behavior.

        Removes any local override for the key(s) from this context's values.
        The original DripImpl instance (if one existed locally) is then re-attached
        to the DripFeeder it would inherit from the parent context, or detached
        if no parent provides the key.
        """
        keys_to_clear = {key} | set(args)
        changed = False
        for k in keys_to_clear:
            if k in self.values:
                # Get the instance before deleting the reference from values
                local_drip_instance = self.values[k]
                # Remove the local override definition
                del self.values[k]
                changed = True

                # Find the feeder this key would now inherit
                parent_feeder = self._find_inherited_feeder(k)

                if parent_feeder:
                    # Attach the existing instance to the inherited feeder
                    local_drip_instance.attach(parent_feeder)
                else:
                    # No parent feeder, detach the instance
                    local_drip_instance.detach()
            # If k was not in self.values, it was already inheriting or undefined,
            # so no change is needed.

        if changed:
            # TODO: Notify streams if the effective source for any key changed
            pass # Placeholder for notification

    def get(self, key: GripRegistry, *args) -> dict[GripRegistry, DripImpl]:
        """
        Retrieve controlled Drips for the specified keys, resolving through parent contexts.
        Returns new DripImpl instances managed by controllers, sharing the original feeder.
        Returns only the Drips for which a source was found.
        """
        keys_to_get = {key} | set(args)
        result: dict[GripRegistry, DripImpl] = {}
        keys_remaining = set(keys_to_get)

        current_context: GripContext | None = self
        while current_context and keys_remaining:
            found_locally: dict[GripRegistry, DripImpl] = {}
            keys_to_check_locally = set(keys_remaining)

            for k in keys_to_check_locally:
                # .get() from dict might return None
                original_drip = current_context.values.get(k)
                if original_drip:
                    # We now assume original_drip is DripImpl due to type hints
                    # If not, it's a programming error elsewhere (e.g., in set)
                    controlled_drip = new_controlled_drip(original_drip)
                    found_locally[k] = controlled_drip
                    keys_remaining.remove(k)

            result.update(found_locally)
            current_context = current_context.parent # Move up the hierarchy

        return result

    def snapshot(self, key: GripRegistry, *args) -> GripContextSnapshot:
        """Collect a snapshot of current values by calling snapshot on each relevant Drip."""
        keys_to_snap = {key} | set(args)
        relevant_drips = self.get(*keys_to_snap)
        snapshot_values = {k: d.snapshot() for k, d in relevant_drips.items()}
        return GripContextSnapshot(values=snapshot_values, source=self)

    def _all_keys(self) -> set[GripRegistry]: # Changed to set for efficiency
        keys = set(self.values.keys()) # Use keys() view
        if self.parent:
            keys.update(self.parent._all_keys())
        return keys

    def stream(self, key: GripRegistry, *args): # -> AsyncIterator[GripContextSnapshot]: # TODO: Fix signature
        """Yields updated snapshots whenever any relevant DripFeeder emits a new value or the context changes."""
        # TODO: Implement stream method using asyncio
        # 1. Get initial drips using self.get()
        # 2. Yield initial snapshot
        # 3. Create asyncio tasks to wait on drip.on_change() for all relevant drips
        # 4. Create asyncio task to wait on context changes (requires event mechanism)
        # 5. Use asyncio.wait(..., return_when=asyncio.FIRST_COMPLETED)
        # 6. On wake, determine cause (drip change or context change)
        # 7. Re-evaluate relevant drips, yield new snapshot, loop
        raise NotImplementedError("stream() method not yet implemented")
        yield # Placeholder to make it a generator type

# @datatree
class DripContextSnapshot:
    _grip: GripRegistry
    _source: 'GripContext'
    _values: dict[GripRegistry, Any]
    _asyncio_running: bool = False

    # def __dt_init__(self, grip: Grip, source: 'GripContext'):
    #     self._grip = grip
    #     self._source = source

    # @property
    # def values(self) -> dict[GripKey, Any]:
    #     return self._values

    # @dtfield
    # def keys(self) -> list[GripKey]:
    #     return list(self._values.keys())
    # @dtfield
    # def source(self) -> 'GripContext':
    #     return self._source
    # @dtfield
    # def grip(self) -> Grip:
    #     return self._grip

    def __getitem__(self, key: GripRegistry) -> Any:
        return self._values.get(key)
