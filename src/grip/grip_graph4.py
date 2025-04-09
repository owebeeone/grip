from grip.grip_dgraph import (
    ApplicationKeyBase,
    DGraphNodeKey,
    DGraphNodeKind,
    DGraphNodeConstraintsData,  # If needed later for constraints
    DGraphNodeConstraints,  # Base for our constraints class
    DGraphGroup,
    DGraphNode,
    DGraph,
    DGraphWrap,
)
from dataclasses import dataclass
from typing import Any, Set, Optional


# Define a custom exception for constraint violations
class ConstraintViolationError(ValueError):
    pass


# --- Type-Specific Constraint Implementations ---


class BaseGripConstraints(DGraphNodeConstraints):
    """Base class for Grip constraints using allowed kind sets."""

    # Subclasses should override these sets to define allowed connections
    ALLOWED_TARGET_KINDS: Set[DGraphNodeKind] = set()
    ALLOWED_SOURCE_KINDS: Set[DGraphNodeKind] = set()

    def check_can_connect_to(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        """Checks if source (self type) can connect TO target based on ALLOWED_TARGET_KINDS."""
        if target_node.kind not in self.ALLOWED_TARGET_KINDS:
            allowed_names = {k.name for k in self.ALLOWED_TARGET_KINDS}
            raise ConstraintViolationError(
                f"{source_node.kind.name} ({source_node.internal_id}) cannot connect TO {target_node.kind.name} ({target_node.internal_id}). "
                f"Allowed targets: {allowed_names or 'None'}"
            )
        # print(f"OK (Base check): {source_node.kind.name} -> {target_node.kind.name}")

    def check_can_receive_connection_from(
        self, source_node: "DGraphNode", target_node: "DGraphNode"
    ):
        """Checks if target (self type) can receive connection FROM source based on ALLOWED_SOURCE_KINDS."""
        if source_node.kind not in self.ALLOWED_SOURCE_KINDS:
            allowed_names = {k.name for k in self.ALLOWED_SOURCE_KINDS}
            raise ConstraintViolationError(
                f"{target_node.kind.name} ({target_node.internal_id}) cannot receive connection FROM {source_node.kind.name} ({source_node.internal_id}). "
                f"Allowed sources: {allowed_names or 'None'}"
            )
        # print(f"OK (Base check): {source_node.kind.name} <- {target_node.kind.name}")

    def check_add_node(self, graph: "DGraph", node_to_add: "DGraphNode"):
        # Base implementation might check nothing, or enforce app_key presence/absence
        # The subclasses now handle the specific presence/absence checks.
        pass  # Keep base simple or remove if all subclasses override

    def post_add_node(self, graph: "DGraph", added_node: "DGraphNode"):
        """Base implementation does nothing."""
        pass

    # --- Default post hooks and disconnect checks remain the same ---
    def post_connect_to(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        pass

    def post_receive_connection_from(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        pass

    def post_disconnect_from(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        pass

    def post_remove_connection_from(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        pass

    def check_can_disconnect_from(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        pass

    def check_can_remove_connection_from(
        self, source_node: "DGraphNode", target_node: "DGraphNode"
    ):
        pass


class ConsumerConstraints(BaseGripConstraints):
    """Constraints for CONSUMER nodes (Dependency Model: Consumer -> Producer/Query)."""

    # Consumer connects TO Producer or Query
    ALLOWED_TARGET_KINDS = {DGraphNodeKind.PRODUCER, DGraphNodeKind.QUERY}
    # Consumer receives connections FROM No one
    ALLOWED_SOURCE_KINDS = set()  # Empty
    # No need to override check methods anymore

    def check_add_node(self, graph: "DGraph", node_to_add: "DGraphNode"):
        """Check: Consumer nodes must have an application key."""
        if node_to_add.application_key is None:
            raise ConstraintViolationError("Consumer nodes must have an application key")
        # Uniqueness check within context will be added later if needed

    def post_add_node(self, graph: "DGraph", added_node: "DGraphNode"):
        """Post-add: Update the parent context index."""
        app_key = added_node.application_key
        kind = added_node.kind
        if app_key is None:
            return  # Should have been caught by check_add_node

        parent_context_id: Optional[int] = None
        # TODO: Robustly determine parent context ID from added_node or app_key
        if hasattr(app_key, "context_id"):
            parent_context_id = app_key.context_id

        if parent_context_id is not None:
            parent_node = graph.nodes.get(parent_context_id)
            if parent_node and parent_node.context_resource_index is not None:
                index_key = (app_key, kind)
                # Check again before write? Or assume check_add_node was sufficient?
                # Let's assume check_add_node handled uniqueness check.
                parent_node.context_resource_index[index_key] = added_node.internal_id
                graph._mark_dirty(parent_context_id)  # Mark parent dirty
            # else: Log warning?


# Singleton instance
_consumer_constraints = ConsumerConstraints()


class ProducerConstraints(BaseGripConstraints):
    """Constraints for PRODUCER nodes (Dependency Model: Receives from Consumer/Query)."""

    # Producer connects TO No one
    ALLOWED_TARGET_KINDS = {DGraphNodeKind.GROUP}
    # Producer receives connections FROM Consumer or Query
    ALLOWED_SOURCE_KINDS = {DGraphNodeKind.CONSUMER, DGraphNodeKind.QUERY}
    # No need to override check methods anymore

    def check_add_node(self, graph: "DGraph", node_to_add: "DGraphNode"):
        """Check: Producer nodes must have an application key."""
        if node_to_add.application_key is None:
            raise ConstraintViolationError("Producer nodes must have an application key")
        # Uniqueness check within context will be added later if needed

    def post_add_node(self, graph: "DGraph", added_node: "DGraphNode"):
        """Post-add: Update the parent context index."""
        app_key = added_node.application_key
        kind = added_node.kind
        if app_key is None:
            return  # Should have been caught by check_add_node

        parent_context_id: Optional[int] = None
        # TODO: Robustly determine parent context ID from added_node or app_key
        if hasattr(app_key, "context_id"):
            parent_context_id = app_key.context_id

        if parent_context_id is not None:
            parent_node = graph.nodes.get(parent_context_id)
            if parent_node and parent_node.context_resource_index is not None:
                index_key = (app_key, kind)
                parent_node.context_resource_index[index_key] = added_node.internal_id
                graph._mark_dirty(parent_context_id)  # Mark parent dirty
            # else: Log warning?


# Singleton instance
_producer_constraints = ProducerConstraints()


class GroupConstraints(BaseGripConstraints):
    """Constraints for GROUP nodes (Dependency Model: Group -> Group)."""

    # Group connects TO Group
    ALLOWED_TARGET_KINDS = {DGraphNodeKind.GROUP, DGraphNodeKind.QUERY}
    # Group receives connections FROM Group
    ALLOWED_SOURCE_KINDS = {DGraphNodeKind.GROUP, DGraphNodeKind.CONSUMER, DGraphNodeKind.PRODUCER}

    def check_add_node(self, graph: "DGraph", node_to_add: "DGraphNode"):
        """Check: Group nodes must NOT have an application key."""
        if node_to_add.application_key is not None:
            raise ConstraintViolationError("Context/Group nodes must not have an application key")

    def post_add_node(self, graph: "DGraph", added_node: "DGraphNode"):
        """Post-add: Initialize the context index for the Group."""
        if added_node.context_resource_index is None:
            added_node.context_resource_index = {}


# Singleton instance
_group_constraints = GroupConstraints()


class QueryConstraints(BaseGripConstraints):
    """Constraints for QUERY nodes (Dependency Model: Query -> Producer, Receives from Consumer)."""

    # Query connects TO Producer
    ALLOWED_TARGET_KINDS = {DGraphNodeKind.PRODUCER}
    # Query receives connections FROM Consumer
    ALLOWED_SOURCE_KINDS = {DGraphNodeKind.CONSUMER}
    # No need to override check methods anymore

    def check_add_node(self, graph: "DGraph", node_to_add: "DGraphNode"):
        """Check: Query nodes must NOT have an application key."""
        if node_to_add.application_key is not None:
            raise ConstraintViolationError("Query nodes must not have an application key")

    def post_add_node(self, graph: "DGraph", added_node: "DGraphNode"):
        """Post-add: Initialize the context index for the Query."""
        if added_node.context_resource_index is None:
            added_node.context_resource_index = {}


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
    app_key: ApplicationKeyBase

    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.PRODUCER

    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _producer_constraints

    @property
    def application_key(self) -> Optional[ApplicationKeyBase]:
        return self.app_key


@dataclass(frozen=True, order=True)
class ConsumerKey(DGraphNodeKey):
    """Key representing a CONSUMER node within GripGraph4."""

    key_data: Any
    app_key: ApplicationKeyBase

    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.CONSUMER

    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _consumer_constraints

    @property
    def application_key(self) -> Optional[ApplicationKeyBase]:
        return self.app_key


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
