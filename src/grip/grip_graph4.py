from grip.grip_dgraph import (
    DGraphNodeKey,
    DGraphNodeKind,
    DGraphNodeConstraintsData, # If needed later for constraints
    DGraphNodeConstraints,     # Base for our constraints class
    DGraphGroup,
    DGraphNode,
    DGraph,
    DGraphWrap,
)
from dataclasses import dataclass
from typing import Any, Set

# --- Key Definitions specific to GripGraph4 ---

@dataclass(frozen=True, order=True)
class GroupKey(DGraphNodeKey):
    """Key representing a GROUP node within GripGraph4."""
    # key_data will typically be the group's unique identifier (e.g., a string name or int)
    key_data: Any
    # __slots__ = ['key_data'] # Optional optimization

    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.GROUP

@dataclass(frozen=True, order=True)
class ProducerKey(DGraphNodeKey):
    """Key representing a PRODUCER node within GripGraph4."""
    # key_data could be a tuple (group_key_data, producer_name) or similar
    key_data: Any
    # __slots__ = ['key_data'] # Optional optimization

    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.PRODUCER

@dataclass(frozen=True, order=True)
class ConsumerKey(DGraphNodeKey):
    """Key representing a CONSUMER node within GripGraph4."""
    # key_data could be a tuple (group_key_data, consumer_name) or similar
    key_data: Any
    # __slots__ = ['key_data'] # Optional optimization

    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.CONSUMER

@dataclass(frozen=True, order=True)
class QueryKey(DGraphNodeKey):
    """Key representing a QUERY node within GripGraph4."""
    # key_data would be the specific query identifier
    key_data: Any
    # __slots__ = ['key_data'] # Optional optimization

    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.QUERY


# --- Constraint Logic ---

# Define a custom exception for constraint violations
class ConstraintViolationError(ValueError):
    pass

# --- Type-Specific Constraint Implementations ---

class BaseGripConstraints(DGraphNodeConstraints):
    """Base class for Grip constraints using allowed kind sets."""
    # Subclasses should override these sets to define allowed connections
    ALLOWED_TARGET_KINDS: Set[DGraphNodeKind] = set()
    ALLOWED_SOURCE_KINDS: Set[DGraphNodeKind] = set()

    def check_can_connect_to(self, source_node: 'DGraphNode', target_node: 'DGraphNode'):
        """Checks if source (self type) can connect TO target based on ALLOWED_TARGET_KINDS."""
        if target_node.kind not in self.ALLOWED_TARGET_KINDS:
            allowed_names = {k.name for k in self.ALLOWED_TARGET_KINDS}
            raise ConstraintViolationError(
                f"{source_node.kind.name} ({source_node.internal_id}) cannot connect TO {target_node.kind.name} ({target_node.internal_id}). "
                f"Allowed targets: {allowed_names or 'None'}"
            )
        # print(f"OK (Base check): {source_node.kind.name} -> {target_node.kind.name}")

    def check_can_receive_connection_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'):
        """Checks if target (self type) can receive connection FROM source based on ALLOWED_SOURCE_KINDS."""
        if source_node.kind not in self.ALLOWED_SOURCE_KINDS:
            allowed_names = {k.name for k in self.ALLOWED_SOURCE_KINDS}
            raise ConstraintViolationError(
                f"{target_node.kind.name} ({target_node.internal_id}) cannot receive connection FROM {source_node.kind.name} ({source_node.internal_id}). "
                f"Allowed sources: {allowed_names or 'None'}"
            )
        # print(f"OK (Base check): {source_node.kind.name} <- {target_node.kind.name}")

    # --- Default post hooks and disconnect checks remain the same ---
    def post_connect_to(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def post_receive_connection_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def post_disconnect_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def post_remove_connection_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def check_can_disconnect_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def check_can_remove_connection_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass


class ConsumerConstraints(BaseGripConstraints):
    """Constraints for CONSUMER nodes (Dependency Model: Consumer -> Producer/Query)."""
    # Consumer connects TO Producer or Query
    ALLOWED_TARGET_KINDS = {DGraphNodeKind.PRODUCER, DGraphNodeKind.QUERY}
    # Consumer receives connections FROM No one
    ALLOWED_SOURCE_KINDS = set() # Empty
    # No need to override check methods anymore

# Singleton instance
_consumer_constraints = ConsumerConstraints()

class ProducerConstraints(BaseGripConstraints):
    """Constraints for PRODUCER nodes (Dependency Model: Receives from Consumer/Query)."""
    # Producer connects TO No one
    ALLOWED_TARGET_KINDS = {DGraphNodeKind.GROUP}
    # Producer receives connections FROM Consumer or Query
    ALLOWED_SOURCE_KINDS = {DGraphNodeKind.CONSUMER, DGraphNodeKind.QUERY}
    # No need to override check methods anymore

# Singleton instance
_producer_constraints = ProducerConstraints()

class GroupConstraints(BaseGripConstraints):
    """Constraints for GROUP nodes (Dependency Model: Group -> Group)."""
    # Group connects TO Group
    ALLOWED_TARGET_KINDS = {DGraphNodeKind.GROUP, DGraphNodeKind.QUERY}
    # Group receives connections FROM Group
    ALLOWED_SOURCE_KINDS = {DGraphNodeKind.GROUP, DGraphNodeKind.CONSUMER, DGraphNodeKind.PRODUCER}

# Singleton instance
_group_constraints = GroupConstraints()

class QueryConstraints(BaseGripConstraints):
    """Constraints for QUERY nodes (Dependency Model: Query -> Producer, Receives from Consumer)."""
    # Query connects TO Producer
    ALLOWED_TARGET_KINDS = {DGraphNodeKind.PRODUCER}
    # Query receives connections FROM Consumer
    ALLOWED_SOURCE_KINDS = {DGraphNodeKind.CONSUMER}
    # No need to override check methods anymore

# Singleton instance
_query_constraints = QueryConstraints()


# --- Update Key Definitions to include constraints ---

@dataclass(frozen=True, order=True)
class GroupKey(DGraphNodeKey):
    """Key representing a GROUP node within GripGraph4."""
    key_data: Any
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.GROUP
    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _group_constraints

@dataclass(frozen=True, order=True)
class ProducerKey(DGraphNodeKey):
    """Key representing a PRODUCER node within GripGraph4."""
    key_data: Any
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.PRODUCER
    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _producer_constraints

@dataclass(frozen=True, order=True)
class ConsumerKey(DGraphNodeKey):
    """Key representing a CONSUMER node within GripGraph4."""
    key_data: Any
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.CONSUMER
    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _consumer_constraints

@dataclass(frozen=True, order=True)
class QueryKey(DGraphNodeKey):
    """Key representing a QUERY node within GripGraph4."""
    key_data: Any
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.QUERY
    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _query_constraints


# --- Constraint Logic ---

# Define a custom exception for constraint violations
# class ConstraintViolationError(ValueError):
#     pass

# class GripGraphConstraints(DGraphNodeConstraints):
    # ... remove entire class definition ...



# <<< Start defining GripGraph4 wrapper class here >>>




