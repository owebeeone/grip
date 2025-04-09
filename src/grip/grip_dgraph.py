from __future__ import annotations
import weakref
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, Set, List, Type, Iterable, Tuple
from enum import Enum, auto
from collections import deque  # For BFS algorithm
import copy  # Import the copy module
from abc import ABC, abstractmethod  # For abstract base class

# --- Global Verbosity Flag ---
VERBOSE = False  # Set to True for detailed debug prints


# --- Base class for Application-Specific Keys/Data ---
class ApplicationKeyBase(ABC):
    """Abstract base class for application-specific key/data objects
    associated with a DGraphNode via its DGraphNodeKey.
    Ensures these objects are not deepcopied with the graph structure.
    """

    # Add common abstract methods/properties if needed by constraints or other generic logic
    # ...

    def __deepcopy__(self, memo: Dict[int, Any]) -> "ApplicationKeyBase":
        """Application keys are treated as immutable references; do not copy."""
        return self


# --- Node Kind Enum ---


class DGraphNodeKind(Enum):
    """Enumeration for the different types of nodes in the graph."""

    GROUP = auto()
    PRODUCER = auto()
    CONSUMER = auto()
    QUERY = auto()


class DGraphNodeConstraintsData(ABC):
    """Abstract base class for data used to implement constraints on DGraph nodes."""

    pass


class DGraphNodeBase(ABC):
    """Abstract base class for DGraph nodes."""

    # kind: DGraphNodeKind
    constraint_data: DGraphNodeConstraintsData


class DGraphNodeConstraints(ABC):
    """Abstract base class for constraints on DGraph nodes.
    An instance specific to a node type is provided by the DGraphNodeKey.
    Methods should raise ConstraintViolationError (or a subclass) if a
    constraint is violated.
    """

    @abstractmethod
    def check_can_connect_to(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        """Check if 'source_node' (of the type this constraint belongs to)
        is allowed to connect TO 'target_node'."""
        pass

    @abstractmethod
    def check_can_receive_connection_from(
        self, source_node: "DGraphNode", target_node: "DGraphNode"
    ):
        """Check if 'target_node' (of the type this constraint belongs to)
        is allowed to receive a connection FROM 'source_node'."""
        pass

    @abstractmethod
    def post_connect_to(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        """Called on source_node's constraints after a successful connection TO target_node."""
        pass

    @abstractmethod
    def post_receive_connection_from(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        """Called on target_node's constraints after a successful connection FROM source_node."""
        pass

    @abstractmethod
    def check_can_disconnect_from(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        """Check if 'source_node' (of the type this constraint belongs to)
        is allowed to disconnect FROM 'target_node'."""
        pass

    @abstractmethod
    def check_can_remove_connection_from(
        self, source_node: "DGraphNode", target_node: "DGraphNode"
    ):
        """Check if 'target_node' (of the type this constraint belongs to)
        is allowed to have its connection FROM 'source_node' removed."""
        pass

    @abstractmethod
    def post_disconnect_from(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        """Called on source_node's constraints after successfully disconnecting FROM target_node."""
        pass

    @abstractmethod
    def post_remove_connection_from(self, source_node: "DGraphNode", target_node: "DGraphNode"):
        """Called on target_node's constraints after successfully removing the connection FROM source_node."""
        pass

    @abstractmethod
    def check_add_node(self, graph: 'DGraph', node_to_add: 'DGraphNode'):
        """Check if the given node can be added to the graph.
           Called by DGraph.add_node before adding the node to the main dictionary.
           The node_to_add object contains the application_key already assigned.
           Implementations should raise ConstraintViolationError if the node cannot be added
           (e.g., due to application key uniqueness violation).
        """
        pass

    def __deepcopy__(self, memo: Dict[int, Any]) -> "DGraphNodeConstraints":
        """Constraints are typically stateless singletons, so just return self."""
        return self


# --- Graph Group (ID/Key Management) ---


@dataclass
class DGraphGroup:
    """
    Manages unique internal integer IDs for nodes and keys within potentially
    multiple related DGraph instances. Handles weak references for keys to
    allow garbage collection.
    """

    _node_id_counter: int = field(default=0, init=False, repr=False)
    _key_id_counter: int = field(default=0, init=False, repr=False)
    # Mappings for Keys
    # External Key object -> internal key ID (strong ref needed here for lookup)
    _key_to_internal_id: Dict[DGraphNodeKey, int] = field(
        default_factory=dict, init=False, repr=False
    )
    # Internal key ID -> External Key object (weak ref to allow GC)
    _internal_id_to_key: weakref.WeakValueDictionary[int, DGraphNodeKey] = field(
        default_factory=weakref.WeakValueDictionary, init=False, repr=False
    )

    def generate_node_id(self) -> int:
        """Generates a new unique internal ID for a graph node."""
        new_id = self._node_id_counter
        self._node_id_counter += 1
        return new_id

    def register_key(self, key: DGraphNodeKey) -> int:
        """
        Registers a key, assigning it an internal ID if it's new (lazy allocation).
        Returns the internal ID for the key.
        """
        # isinstance check works with ABCs and subclasses
        if not isinstance(key, DGraphNodeKey):
            raise TypeError("Key must be an instance of DGraphNodeKey or its subclasses")

        if key in self._key_to_internal_id:
            # Key already registered, return existing ID
            key_id = self._key_to_internal_id[key]
            # Ensure weak ref is still alive or re-establish it
            if key_id not in self._internal_id_to_key:
                self._internal_id_to_key[key_id] = key
            return key_id
        else:
            # Assign a new ID
            new_id = self._key_id_counter
            self._key_id_counter += 1
            self._key_to_internal_id[key] = new_id
            self._internal_id_to_key[new_id] = key
            # print(f"Registered key {key} with ID {new_id}") # Debugging
            return new_id

    def get_key_from_id(self, key_internal_id: int) -> Optional[DGraphNodeKey]:
        """Retrieves the Key object from its internal ID using the weak reference."""
        if key_internal_id == -1:
            return None
        key = self._internal_id_to_key.get(key_internal_id)
        # print(f"Retrieving key for ID {key_internal_id}: {key}") # Debugging
        return key

    def get_id_from_key(self, key: DGraphNodeKey) -> Optional[int]:
        """Retrieves the internal ID for a given key."""
        return self._key_to_internal_id.get(key)

    def reap_graph_keys(self, graph: DGraph) -> Set[int]:
        """
        Checks for garbage-collected keys (expired weak references) and REMOVES
        nodes in the provided graph that reference those dead keys.

        This method iterates through the known keys. If a key's weak reference
        is found to be dead, it finds all nodes in the specific graph using
        that key's ID and removes them using graph.remove_node().
        It also cleans up the internal `_key_to_internal_id` map for the dead keys.

        Args:
            graph: The DGraph instance whose nodes should be checked and potentially removed.

        Returns:
            A set of internal key IDs that were reaped because their corresponding
            key objects were garbage collected.
        """
        dead_key_ids: Set[int] = set()
        keys_to_remove_from_strong_map: List[DGraphNodeKey] = []

        # Phase 1: Identify dead keys and mark for removal from strong map
        for key, key_id in list(self._key_to_internal_id.items()):
            if self._internal_id_to_key.get(key_id) is None:
                dead_key_ids.add(key_id)
                keys_to_remove_from_strong_map.append(key)

        if not dead_key_ids:
            return set()  # Nothing to do

        # Phase 2: Identify nodes in the specific graph to remove using the graph's index
        node_ids_to_remove: Set[int] = set()
        for key_id in dead_key_ids:
            node_id = graph._key_id_to_node_id.get(key_id)  # Use the index
            if node_id is not None:
                # Check if the node actually still exists in the graph
                if node_id in graph.nodes:
                    # Verify the node's key_id indeed matches (sanity check)
                    if graph.nodes[node_id].key_internal_id == key_id:
                        node_ids_to_remove.add(node_id)
                else:
                    # Node already removed, clean up index just in case
                    # This case might occur if remove_node was called between phases
                    if key_id in graph._key_id_to_node_id:
                        del graph._key_id_to_node_id[key_id]

        # Phase 3: Remove the identified nodes from the graph
        # This loop is safe because we're iterating over a separate set
        for node_id in node_ids_to_remove:
            # print(f"Removing node {node_id} due to dead key") # Debugging
            graph.remove_node(
                node_id
            )  # remove_node handles links, dirty state, and _key_id_to_node_id cleanup

        # Phase 4: Clean up the strong reference map in this group
        for key in keys_to_remove_from_strong_map:
            # print(f"Removing key {key} from strong map") # Debugging
            if key in self._key_to_internal_id:  # Check if not already removed by cascade
                del self._key_to_internal_id[key]

        # Note: The WeakValueDictionary (_internal_id_to_key) cleans itself up automatically.

        return dead_key_ids


# --- Graph Node Definition ---


@dataclass(eq=False)  # Equality should be based on ID via DGraph, not fields
class DGraphNode(DGraphNodeBase):
    """Represents a node within the DGraph."""

    graph: DGraph = field(repr=False, compare=False)  # Reference to the parent graph
    internal_id: int
    kind: DGraphNodeKind  # Kind is now determined by the key used to create it
    # Field to store the associated application-level key/data object
    application_key: Optional[ApplicationKeyBase] = field(compare=False)
    key_internal_id: int = -1  # -1 indicates no key associated (or key was reaped)
    forward_links: Set[int] = field(default_factory=set, compare=False)  # Set of target node IDs
    back_links: Set[int] = field(default_factory=set, compare=False)  # Set of source node IDs

    # __slots__ = ['graph', 'internal_id', 'kind', 'key_internal_id', 'forward_links', 'back_links'] # Optional memory optimization

    def __post_init__(self):
        # Ensure ID is non-negative
        if self.internal_id < 0:
            raise ValueError("internal_id must be non-negative")
        # Kind validation could happen here if needed, based on key_internal_id

    def get_key(self) -> Optional[DGraphNodeKey]:
        """Retrieves the associated key object via the graph's group."""
        # Check if key_internal_id is valid before attempting lookup
        if self.key_internal_id == -1:
            return None
        # Ensure graph and group references are valid before using them
        # Check graph reference first, as group is accessed via graph
        if self.graph and hasattr(self.graph, "group") and self.graph.group:
            return self.graph.group.get_key_from_id(self.key_internal_id)
        # This might happen if the node exists but its parent graph is being torn down?
        # Or if the node was somehow created without a valid graph reference.
        # print(f"Warning: Node {self.internal_id} has no valid graph reference for key lookup.")
        return None

    def _add_forward_link(self, target_node_id: int):
        """Internal method to add forward link."""
        if target_node_id != self.internal_id:  # Prevent self-loops
            # FIX: Check membership *before* adding, only mark dirty if new
            if target_node_id not in self.forward_links:
                self.forward_links.add(target_node_id)
                self.graph._mark_dirty(self.internal_id)  # Mark dirty only if changed

    def _remove_forward_link(self, target_node_id: int):
        """Internal method to remove forward link."""
        if target_node_id in self.forward_links:
            self.forward_links.remove(target_node_id)
            self.graph._mark_dirty(self.internal_id)

    def _add_back_link(self, source_node_id: int):
        """Internal method to add backward link."""
        if source_node_id != self.internal_id:
            # FIX: Check membership *before* adding, only mark dirty if new
            if source_node_id not in self.back_links:
                self.back_links.add(source_node_id)
                self.graph._mark_dirty(self.internal_id)  # Mark dirty only if changed

    def _remove_back_link(self, source_node_id: int):
        """Internal method to remove backward link."""
        if source_node_id in self.back_links:
            self.back_links.remove(source_node_id)
            self.graph._mark_dirty(self.internal_id)

    def __repr__(self):
        key_info = f", key_id={self.key_internal_id}" if self.key_internal_id != -1 else ""
        app_key_info = (
            f", app_key={type(self.application_key).__name__}" if self.application_key else ""
        )
        return (
            f"DGraphNode(id={self.internal_id}, kind={self.kind.name}{key_info}{app_key_info}, "
            f"fwd={sorted(list(self.forward_links))}, bck={sorted(list(self.back_links))})"
        )

    def __hash__(self):
        return hash(self.internal_id)

    def __eq__(self, other):
        if isinstance(other, DGraphNode):
            # Nodes are considered equal if they have the same ID *and* belong to the same graph instance
            # This is important for deepcopy where nodes might be recreated.
            return self.internal_id == other.internal_id and self.graph is other.graph
        return NotImplemented

    # Note: No __deepcopy__ needed here if DGraph.__deepcopy__ handles memoization correctly before copying nodes.
    # deepcopy will call __init__ on the copied nodes, and the graph reference
    # should point to the new graph instance found in the memo dictionary.


# --- Directed Graph Definition ---


@dataclass
class DGraph:
    """Represents the directed graph, managing nodes and their relationships."""

    # DGraphGroup must now be provided during initialization
    group: DGraphGroup
    nodes: Dict[int, DGraphNode] = field(default_factory=dict, repr=False)
    dirty_node_ids: Set[int] = field(default_factory=set, repr=False)
    # Index for efficient lookup of node ID by key ID
    _key_id_to_node_id: Dict[int, int] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        # Rebuild only key_id index if needed
        if self.nodes and not self._key_id_to_node_id:
             print("Warning: Rebuilding key_id index from existing nodes...")
             for node_id, node in self.nodes.items():
                 if node.key_internal_id != -1:
                     if node.key_internal_id not in self._key_id_to_node_id:
                         self._key_id_to_node_id[node.key_internal_id] = node_id
                     else:
                         print(f"Warning: Key ID {node.key_internal_id} collision during index rebuild.")

    def _mark_dirty(self, node_id: int):
        """Marks a node as dirty (modified)."""
        if node_id in self.nodes:  # Avoid marking non-existent nodes dirty
            self.dirty_node_ids.add(node_id)

    def clear_dirty(self):
        """Clears the set of dirty node IDs."""
        self.dirty_node_ids.clear()

    def get_node(self, node_id: int) -> Optional[DGraphNode]:
        """Retrieves a node by its internal ID."""
        return self.nodes.get(node_id)

    def get_node_by_key(self, key: DGraphNodeKey) -> Optional[DGraphNode]:
        """
        Retrieves a node efficiently using its key.
        If the key is registered in the group but the node doesn't exist
        in this specific graph, it automatically adds the node to this graph.
        """
        key_id = self.group.get_id_from_key(key)
        if key_id is None:
            # Key isn't even registered in the group, so no node can exist.
            # Or should we register and add here? Let's stick to only adding
            # if key_id exists but node_id doesn't for now.
            return None

        node_id = self._key_id_to_node_id.get(key_id)
        if node_id is None:
            # Key is registered in the group, but not indexed in this graph.
            # Try to get the key object itself (it must exist if key_id is valid).
            key_object = self.group.get_key_from_id(key_id)
            if key_object:
                # Automatically add the node to *this* graph instance
                # print(f"Auto-adding node for existing key {key_object} (key_id={key_id})") # Debugging
                # The add_node method now derives the kind from the key.
                new_node = self.add_node(key=key_object)
                return new_node
            else:
                # This case is unexpected: key_id exists but weak ref is dead?
                # Could happen if reap_keys hasn't run recently.
                # Or if get_id_from_key succeeded but get_key_from_id failed concurrently.
                print(
                    f"Warning: Key object for key_id {key_id} not found via weak ref, cannot auto-add node."
                )
                return None  # Cannot add node without the key object to get its kind.

        # Node ID found in the index, retrieve the node object
        return self.nodes.get(node_id)

    def add_node(self, key: DGraphNodeKey) -> DGraphNode:
        """
        Adds a new node, performs DGraphNodeKey uniqueness check, and calls
        the constraint's check_add_node before finalizing.
        """
        app_key = key.get_application_key()
        kind = key.kind

        # --- DGraphNodeKey Uniqueness Check (via DGraphGroup internal ID) ---
        check_key_id = self.group.get_id_from_key(key)
        if check_key_id is not None:
            existing_node_id_by_keyid = self._key_id_to_node_id.get(check_key_id)
            if existing_node_id_by_keyid is not None and existing_node_id_by_keyid in self.nodes:
                 raise ValueError(f"DGraphNodeKey {key} (internal id {check_key_id}) is already associated with node {existing_node_id_by_keyid} in this graph.")
            elif existing_node_id_by_keyid is not None: # Index inconsistent
                 if check_key_id in self._key_id_to_node_id:
                    del self._key_id_to_node_id[check_key_id]

        # --- Create Node Object --- 
        key_id = self.group.register_key(key)
        node_id = self.group.generate_node_id()
        node = DGraphNode(
            graph=self,
            internal_id=node_id,
            kind=kind,
            key_internal_id=key_id,
            application_key=app_key,
        )

        try:
            key.constraints.check_add_node(self, node)
        except Exception as e:
             if VERBOSE:
                  print(f"Constraint check failed for adding node via key {key} (app_key: {app_key}): {e}")
             raise

        # --- Add Node and Update Indices --- 
        self.nodes[node_id] = node
        self._key_id_to_node_id[key_id] = node_id

        self._mark_dirty(node_id)
        return node

    def remove_node(self, node_id: int) -> bool:
        """Removes a node and cleans up the key_id index."""
        node_to_remove = self.nodes.get(node_id)
        if not node_to_remove: return False

        # Get internal key ID directly from node
        key_internal_id = node_to_remove.key_internal_id
        # REMOVED fetching of app_key and kind for index cleanup

        # --- Update Indices BEFORE removing node object --- 
        # 1. key_id index
        if key_internal_id != -1:
            if self._key_id_to_node_id.get(key_internal_id) == node_id:
                 del self._key_id_to_node_id[key_internal_id]
        # REMOVED cleanup for _app_key_kind_to_node_id index

        # --- Remove connections --- 
        target_ids = list(node_to_remove.forward_links)
        source_ids = list(node_to_remove.back_links)
        # ... disconnect from targets ...
        # ... disconnect from sources ...

        # --- Remove node object and clean dirty set --- 
        del self.nodes[node_id]
        self.dirty_node_ids.discard(node_id)
        return True

    def connect_nodes(self, source_id: int, target_id: int):
        """Creates a directed edge from source node to target node, checking constraints."""
        source_node = self.nodes.get(source_id)
        target_node = self.nodes.get(target_id)

        if not (source_node and target_node and source_id != target_id):
            if VERBOSE:
                print(
                    f"Warning: connect_nodes validation failed for {source_id} -> {target_id} (nodes not found or self-loop)"
                )
            return  # Or raise appropriate error

        # --- Check if already connected ---
        if target_id in source_node.forward_links:
            if VERBOSE:
                print(
                    f"Info: connect_nodes skipped for {source_id} -> {target_id} (already connected)"
                )
            return  # Idempotent success

        # --- Get Keys and Constraints (only if not already connected) ---
        source_key = source_node.get_key()
        target_key = target_node.get_key()

        if not source_key or not target_key:
            # Should not happen if nodes have keys, but handle defensively
            raise ValueError(
                f"Cannot connect nodes: Missing key for source ({source_id}) or target ({target_id})."
            )

        # Get constraints from keys
        try:
            source_constraints = source_key.constraints
            target_constraints = target_key.constraints
        except AttributeError as e:
            raise NotImplementedError(
                f"Node key type {type(source_key).__name__} or {type(target_key).__name__} does not implement the 'constraints' property."
            ) from e

        # --- Check Constraints ---
        # Raises ConstraintViolationError on failure
        try:
            # Check from the source's perspective
            source_constraints.check_can_connect_to(source_node, target_node)
            # Check from the target's perspective
            target_constraints.check_can_receive_connection_from(source_node, target_node)
        except Exception as e:  # Catch ConstraintViolationError or others defined in constraints
            if VERBOSE:
                print(
                    f"Constraint check failed for {source_id} ({source_key.kind.name}) -> {target_id} ({target_key.kind.name}): {e}"
                )
            raise  # Re-raise the exception to signal failure

        # --- Constraints Passed: Perform Connection ---
        if VERBOSE:
            print(
                f"Connecting {source_id} ({source_key.kind.name}) -> {target_id} ({target_key.kind.name})"
            )
        source_node._add_forward_link(target_id)
        target_node._add_back_link(source_id)

        # --- Post-Connection Hooks ---
        # These should generally not raise errors, but could log warnings
        try:
            source_constraints.post_connect_to(source_node, target_node)
            target_constraints.post_receive_connection_from(source_node, target_node)
        except Exception as e:
            if VERBOSE:
                print(f"Warning: Post-connection hook failed for {source_id}->{target_id}: {e}")

    def disconnect_nodes(self, source_id: int, target_id: int):
        """Removes a directed edge, checking constraints before and calling hooks after."""
        source_node = self.nodes.get(source_id)
        target_node = self.nodes.get(target_id)

        if not (source_node and target_node):
            if VERBOSE:
                print(
                    f"Warning: disconnect_nodes validation failed for {source_id} -> {target_id} (nodes not found)"
                )
            return  # Or raise

        # --- Check if the link actually exists before proceeding ---
        if target_id not in source_node.forward_links:
            if VERBOSE:
                print(
                    f"Info: disconnect_nodes skipped for {source_id} -> {target_id} (link does not exist)"
                )
            return  # Idempotent success - No link to remove

        # --- Get Keys and Constraints (only if link exists) ---
        source_key = source_node.get_key()
        target_key = target_node.get_key()

        if not source_key or not target_key:
            raise ValueError(
                f"Cannot disconnect nodes: Missing key for source ({source_id}) or target ({target_id})."
            )

        # Get constraints from keys
        try:
            source_constraints = source_key.constraints
            target_constraints = target_key.constraints
        except AttributeError as e:
            raise NotImplementedError(
                f"Node key type {type(source_key).__name__} or {type(target_key).__name__} does not implement the 'constraints' property."
            ) from e

        # --- Check Constraints ---
        # Raises ConstraintViolationError or similar on failure
        try:
            # Check from the source's perspective
            source_constraints.check_can_disconnect_from(source_node, target_node)
            # Check from the target's perspective
            target_constraints.check_can_remove_connection_from(source_node, target_node)
        except Exception as e:
            if VERBOSE:
                print(
                    f"Disconnection constraint check failed for {source_id} ({source_key.kind.name}) -> {target_id} ({target_key.kind.name}): {e}"
                )
            raise  # Re-raise the exception to signal failure

        # --- Constraints Passed: Perform Disconnection ---
        if VERBOSE:
            print(
                f"Disconnecting {source_id} ({source_key.kind.name}) -> {target_id} ({target_key.kind.name})"
            )
        source_node._remove_forward_link(target_id)
        target_node._remove_back_link(source_id)

        # --- Post-Disconnection Hooks ---
        try:
            source_constraints.post_disconnect_from(source_node, target_node)
            target_constraints.post_remove_connection_from(source_node, target_node)
        except Exception as e:
            if VERBOSE:
                print(f"Warning: Post-disconnection hook failed for {source_id}->{target_id}: {e}")

    def has_cycle(self, subgraph_ids: Optional[Set[int]] = None) -> bool:
        """
        Checks for cycles in the graph (or a specified subgraph) using
        Kahn's algorithm (BFS based on in-degrees). Non-recursive.
        """
        nodes_to_check = subgraph_ids if subgraph_ids is not None else set(self.nodes.keys())
        if not nodes_to_check:
            return False

        in_degree: Dict[int, int] = {}
        adj: Dict[int, List[int]] = {}
        valid_nodes_in_subgraph = 0

        # Initialize structures only for valid nodes within the subgraph
        for node_id in nodes_to_check:
            if node_id in self.nodes:
                in_degree[node_id] = 0
                adj[node_id] = []
                valid_nodes_in_subgraph += 1

        if valid_nodes_in_subgraph == 0:
            return False  # No valid nodes to check

        # Calculate in-degrees and build adjacency list considering only subgraph nodes
        for node_id in list(
            in_degree.keys()
        ):  # Use list to avoid modification issues if nodes are removed concurrently (not applicable here but safer)
            node = self.nodes.get(node_id)
            if not node:
                continue  # Skip if node got removed somehow

            for target_id in node.forward_links:
                if target_id in in_degree:  # Check if target is also in the subgraph
                    adj[node_id].append(target_id)
                    in_degree[target_id] += 1

        # Initialize queue with nodes having in-degree 0
        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        visited_count = 0

        while queue:
            u = queue.popleft()
            visited_count += 1

            for v in adj.get(u, []):
                in_degree[v] -= 1
                if in_degree[v] == 0:
                    queue.append(v)

        # Cycle exists if not all nodes in the valid subgraph were visited
        return visited_count != valid_nodes_in_subgraph

    def __deepcopy__(self, memo):
        """
        Custom deepcopy implementation for DGraph.
        Ensures the 'group' is copied by reference, while mutable collections
        like 'nodes' and 'dirty_node_ids' are deep copied.
        """
        # Check memo first
        cls = self.__class__
        result = memo.get(id(self))
        if result is not None:
            return result

        # Create new instance, copying the group reference
        # We don't copy default_factory fields initially
        result = cls(group=self.group)

        # Add to memo *before* copying mutable members
        memo[id(self)] = result

        # Deep copy mutable fields
        result.nodes = copy.deepcopy(self.nodes, memo)
        result.dirty_node_ids = copy.deepcopy(self.dirty_node_ids, memo)
        result._key_id_to_node_id = copy.deepcopy(self._key_id_to_node_id, memo)

        # --- Verification/Fixup --- 
        # This loop ensures the back-references are correct. Deepcopy should
        # handle this via the memo, but this provides extra robustness.
        for node in result.nodes.values():
            if node.graph is not result:
                # This condition should ideally not be met if deepcopy works correctly.
                node.graph = result  # Correct the reference
        # --- End reference update ---

        return result


# --- Graph Wrapper ---


@dataclass
class DGraphWrap:
    """
    A convenience wrapper around a DGraph instance providing methods
    that operate using DGraphNodeKey objects.
    """

    graph: DGraph

    def get_node(self, key: DGraphNodeKey) -> Optional[DGraphNode]:
        """Retrieves a node efficiently using its key via the graph's index."""
        return self.graph.get_node_by_key(key)

    def get_or_add(self, key: DGraphNodeKey) -> DGraphNode:
        """
        Retrieves a node by its key. If it doesn't exist, adds a new node
        with the kind derived from the key.
        """
        # get_node_by_key now handles the auto-add if key exists in group but not graph
        node = self.graph.get_node_by_key(key)
        if node is None:
            # If key wasn't even registered in group, get_node_by_key returned None.
            # We still need to add it explicitly in this case.
            # This also handles the case where get_node_by_key failed to get the key object.
            node = self.graph.add_node(key=key)
        return node

    def connect_nodes(self, source_key: DGraphNodeKey, target_key: DGraphNodeKey) -> bool:
        """
        Connects two nodes identified by their keys.
        Returns False if either node does not exist after attempting retrieval (or auto-add).
        """
        # Use get_node_by_key which might auto-add if key exists in group
        source_node = self.graph.get_node_by_key(source_key)
        target_node = self.graph.get_node_by_key(target_key)

        if source_node and target_node:
            self.graph.connect_nodes(source_node.internal_id, target_node.internal_id)
            return True
        else:
            # print(f"Warning: Could not connect keys. Source found: {source_node is not None}, Target found: {target_node is not None}") # Debugging
            return False

    def disconnect_nodes(self, source_key: DGraphNodeKey, target_key: DGraphNodeKey) -> bool:
        """
        Disconnects two nodes identified by their keys.
        Returns False if either node does not exist.
        """
        # Use get_node_by_key which might auto-add if key exists in group
        source_node = self.graph.get_node_by_key(source_key)
        target_node = self.graph.get_node_by_key(target_key)

        if source_node and target_node:
            self.graph.disconnect_nodes(source_node.internal_id, target_node.internal_id)
            return True
        else:
            return False

    # Add other convenience methods as needed...
    # e.g., remove_node_by_key(key), add_keyless_node(kind) etc.


# --- Key Definition ---


@dataclass(frozen=True, order=True)
class DGraphNodeKey(ABC): # Make it an Abstract Base Class
    """Base class for keys associated with DGraph nodes.
       Subclasses must implement kind, constraints, and application_key properties.
       They must also be hashable and comparable, likely based on their application_key.
    """

    @property
    @abstractmethod
    def kind(self) -> DGraphNodeKind:
        """Returns the kind of node this key represents."""
        pass

    @property
    @abstractmethod
    def constraints(self) -> DGraphNodeConstraints:
        """Returns the constraint implementation specific to this node type."""
        pass

    @property
    @abstractmethod
    def application_key(self) -> Optional[ApplicationKeyBase]:
        """Returns the application-specific key/data object associated with this node, if any."""
        pass

    def __deepcopy__(self, memo: Dict[int, Any]) -> 'DGraphNodeKey':
        # Keys are immutable and potentially shared, return self.
        return self
