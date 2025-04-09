from __future__ import annotations
import weakref
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, Set, List, Type, Iterable
from enum import Enum, auto
from collections import deque # For BFS algorithm
import gc # For example usage
import copy # Import the copy module
from abc import ABC, abstractmethod # For abstract base class

# --- Node Kind Enum ---

class DGraphNodeKind(Enum):
    """Enumeration for the different types of nodes in the graph."""
    GROUP = auto()
    PRODUCER = auto()
    CONSUMER = auto()
    QUERY = auto()

class DGraphNodeConstraintsData(ABC):
    """Abstract base class for data used to implement constraints on DGraph nodes.
    
    """
    pass

class DGraphNodeBase(ABC):
    """Abstract base class for DGraph nodes.
    """
    # kind: DGraphNodeKind
    constraint_data: DGraphNodeConstraintsData

class DGraphNodeConstraints(ABC):
    """Abstract base class for constraints on DGraph nodes.
    This is a capability modifier for nodes in the.
    
    These methods should not be called to make changes outside of the graph or 
    link in copyable objects. This should be used solely for checking constraints
    and doing bookkeeping functions on the graph.
    """

    @abstractmethod
    def disconnect_nodes_check(self, this_node: 'DGraphNode', target: 'DGraphNode'):
        """Called by DGraph.disconnect_nodes() to check if the target is valid before 
        disconnecting on either side.
        """
        pass
    
    @abstractmethod
    def disconnect_nodes_post(self, this_node: 'DGraphNode', target: 'DGraphNode'):
        """Called by DGraph.disconnect_nodes() after the nodes have been disconnected."""
        pass

    @abstractmethod
    def connect_nodes_check(self, this_node: 'DGraphNode', source: 'DGraphNode'):
        """Called by DGraph.connect_nodes() to check if the source is valid before 
        connecting on either side.
        """
        pass

    @abstractmethod
    def connect_nodes_post(self, this_node: 'DGraphNode', source: 'DGraphNode'):
        """Called by DGraph.connect_nodes() after the nodes have been connected."""
        pass

    def __deepcopy__(self, memo: Dict[int, Any]) -> DGraphNodeConstraints:
        """Constraints are immutable so just return self."""
        return self
    

# --- Key Definition ---

@dataclass(frozen=True, order=True)
class DGraphNodeKey(ABC): # Make it an Abstract Base Class
    """Base class for keys associated with DGraph nodes."""
    # Keys must be hashable and comparable for use in dictionaries/sets
    key_data: Any

    # Consider adding __slots__ for memory efficiency if many keys are expected
    # __slots__ = ['key_data']

    @property
    @abstractmethod
    def kind(self) -> DGraphNodeKind:
        """Returns the kind of node this key represents."""
        pass
    
    def __deepcopy__(self, memo: Dict[int, Any]) -> DGraphNodeKey:
        # No copying allowed for keys.
        return self
    

# --- Specific Key Subclasses ---

@dataclass(frozen=True, order=True)
class GroupKey(DGraphNodeKey):
    """Key representing a GROUP node."""
    # __slots__ = ['key_data']
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.GROUP

@dataclass(frozen=True, order=True)
class ProducerKey(DGraphNodeKey):
    """Key representing a PRODUCER node."""
    # __slots__ = ['key_data']
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.PRODUCER

@dataclass(frozen=True, order=True)
class ConsumerKey(DGraphNodeKey):
    """Key representing a CONSUMER node."""
    # __slots__ = ['key_data']
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.CONSUMER


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
    _key_to_internal_id: Dict[DGraphNodeKey, int] = field(default_factory=dict, init=False, repr=False)
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
            return set() # Nothing to do

        # Phase 2: Identify nodes in the specific graph to remove using the graph's index
        node_ids_to_remove: Set[int] = set()
        for key_id in dead_key_ids:
            node_id = graph._key_id_to_node_id.get(key_id) # Use the index
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
            graph.remove_node(node_id) # remove_node handles links, dirty state, and _key_id_to_node_id cleanup

        # Phase 4: Clean up the strong reference map in this group
        for key in keys_to_remove_from_strong_map:
            # print(f"Removing key {key} from strong map") # Debugging
            if key in self._key_to_internal_id: # Check if not already removed by cascade
                 del self._key_to_internal_id[key]

        # Note: The WeakValueDictionary (_internal_id_to_key) cleans itself up automatically.

        return dead_key_ids


# --- Graph Node Definition ---

@dataclass(eq=False) # Equality should be based on ID via DGraph, not fields
class DGraphNode(DGraphNodeBase):
    """Represents a node within the DGraph."""
    graph: DGraph = field(repr=False, compare=False) # Reference to the parent graph
    internal_id: int
    kind: DGraphNodeKind # Kind is now determined by the key used to create it
    key_internal_id: int = -1 # -1 indicates no key associated (or key was reaped)
    forward_links: Set[int] = field(default_factory=set, compare=False) # Set of target node IDs
    back_links: Set[int] = field(default_factory=set, compare=False) # Set of source node IDs

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
        if self.graph and hasattr(self.graph, 'group') and self.graph.group:
             return self.graph.group.get_key_from_id(self.key_internal_id)
        # This might happen if the node exists but its parent graph is being torn down?
        # Or if the node was somehow created without a valid graph reference.
        # print(f"Warning: Node {self.internal_id} has no valid graph reference for key lookup.")
        return None

    def _add_forward_link(self, target_node_id: int):
        """Internal method to add forward link."""
        if target_node_id != self.internal_id: # Prevent self-loops
            # FIX: Check membership *before* adding, only mark dirty if new
            if target_node_id not in self.forward_links:
                 self.forward_links.add(target_node_id)
                 self.graph._mark_dirty(self.internal_id) # Mark dirty only if changed

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
                 self.graph._mark_dirty(self.internal_id) # Mark dirty only if changed

    def _remove_back_link(self, source_node_id: int):
        """Internal method to remove backward link."""
        if source_node_id in self.back_links:
            self.back_links.remove(source_node_id)
            self.graph._mark_dirty(self.internal_id)

    def __repr__(self):
        key_info = f", key_id={self.key_internal_id}" if self.key_internal_id != -1 else ""
        return f"DGraphNode(id={self.internal_id}, kind={self.kind.name}{key_info}, fwd={sorted(list(self.forward_links))}, bck={sorted(list(self.back_links))})"

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
        # Rebuild index if nodes were somehow passed in init (though default_factory makes this unlikely)
        # More robustly, ensure add_node is the only way nodes get added.
        if self.nodes and not self._key_id_to_node_id:
             print("Warning: Rebuilding key index from existing nodes (should not happen with default_factory)")
             for node_id, node in self.nodes.items():
                  if node.key_internal_id != -1:
                       # Avoid overwriting if multiple nodes somehow share a key_id (shouldn't happen)
                       if node.key_internal_id not in self._key_id_to_node_id:
                            self._key_id_to_node_id[node.key_internal_id] = node_id
                       else:
                            print(f"Warning: Key ID {node.key_internal_id} collision during index rebuild.")


    def _mark_dirty(self, node_id: int):
        """Marks a node as dirty (modified)."""
        if node_id in self.nodes: # Avoid marking non-existent nodes dirty
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
                 print(f"Warning: Key object for key_id {key_id} not found via weak ref, cannot auto-add node.")
                 return None # Cannot add node without the key object to get its kind.

        # Node ID found in the index, retrieve the node object
        return self.nodes.get(node_id)

    def add_node(self, key: DGraphNodeKey) -> DGraphNode:
        """
        Adds a new node to the graph, deriving its kind from the key.
        Ensures key uniqueness within this graph.
        """
        # Check if key is already associated with another node in *this* graph
        # Note: get_node_by_key might now try to add the node if index is inconsistent,
        # so we need to be careful here. Let's check the index directly first.
        check_key_id = self.group.get_id_from_key(key)
        if check_key_id is not None and check_key_id in self._key_id_to_node_id:
             existing_node_id = self._key_id_to_node_id[check_key_id]
             # Check if node actually exists (paranoid check)
             if existing_node_id in self.nodes:
                   raise ValueError(f"Key {key} is already associated with node {existing_node_id} in this graph.")
             else:
                   # Index is inconsistent, clean it up
                   del self._key_id_to_node_id[check_key_id]


        # Register key with the group (might return existing ID)
        key_id = self.group.register_key(key) # Use the graph's group
        # Get the kind from the key itself
        kind = key.kind

        # Generate node ID using the graph's group
        node_id = self.group.generate_node_id()
        # Create node, establishing the back-reference to self (this DGraph instance)
        node = DGraphNode(graph=self, internal_id=node_id, kind=kind, key_internal_id=key_id)
        self.nodes[node_id] = node

        # Update the key->node index
        self._key_id_to_node_id[key_id] = node_id

        self._mark_dirty(node_id) # Mark newly added node as dirty
        return node

    # Note: If keyless nodes are needed, a separate method like add_keyless_node(kind) would be required.

    def remove_node(self, node_id: int) -> bool:
        """Removes a node and its connections from the graph."""
        node_to_remove = self.nodes.get(node_id)
        if not node_to_remove:
            return False # Node doesn't exist

        # --- Update key index before removing node ---
        key_id_to_clear = node_to_remove.key_internal_id
        if key_id_to_clear != -1:
            # Remove entry from key->node index if it exists and points to this node
            if self._key_id_to_node_id.get(key_id_to_clear) == node_id:
                 del self._key_id_to_node_id[key_id_to_clear]
        # --- End index update ---

        target_ids = list(node_to_remove.forward_links)
        source_ids = list(node_to_remove.back_links)

        # Disconnect from targets
        for target_id in target_ids:
            target_node = self.nodes.get(target_id)
            if target_node:
                target_node._remove_back_link(node_id) # Marks target dirty

        # Disconnect from sources
        for source_id in source_ids:
            source_node = self.nodes.get(source_id)
            if source_node:
                source_node._remove_forward_link(node_id) # Marks source dirty

        # Remove the node itself
        del self.nodes[node_id]
        # Remove from dirty set if it was there (it's gone now)
        self.dirty_node_ids.discard(node_id)
        # Note: Neighbors were marked dirty by link removal methods

        return True

    def connect_nodes(self, source_id: int, target_id: int):
        """Creates a directed edge from source node to target node."""
        source_node = self.nodes.get(source_id)
        target_node = self.nodes.get(target_id)

        if source_node and target_node and source_id != target_id:
            source_node._add_forward_link(target_id)
            target_node._add_back_link(source_id)
        # Add warnings if nodes not found?

    def disconnect_nodes(self, source_id: int, target_id: int):
        """Removes a directed edge from source node to target node."""
        source_node = self.nodes.get(source_id)
        target_node = self.nodes.get(target_id)

        if source_node and target_node:
            source_node._remove_forward_link(target_id)
            target_node._remove_back_link(source_id)

    def has_cycle(self, subgraph_ids: Optional[Set[int]] = None) -> bool:
        """
        Checks for cycles in the graph (or a specified subgraph) using
        Kahn's algorithm (BFS based on in-degrees). Non-recursive.
        """
        nodes_to_check = subgraph_ids if subgraph_ids is not None else set(self.nodes.keys())
        if not nodes_to_check: return False

        in_degree: Dict[int, int] = {}
        adj: Dict[int, List[int]] = {}
        valid_nodes_in_subgraph = 0

        # Initialize structures only for valid nodes within the subgraph
        for node_id in nodes_to_check:
            if node_id in self.nodes:
                 in_degree[node_id] = 0
                 adj[node_id] = []
                 valid_nodes_in_subgraph += 1

        if valid_nodes_in_subgraph == 0: return False # No valid nodes to check

        # Calculate in-degrees and build adjacency list considering only subgraph nodes
        for node_id in list(in_degree.keys()): # Use list to avoid modification issues if nodes are removed concurrently (not applicable here but safer)
            node = self.nodes.get(node_id)
            if not node: continue # Skip if node got removed somehow

            for target_id in node.forward_links:
                if target_id in in_degree: # Check if target is also in the subgraph
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

        # Deep copy mutable fields using the memo
        result.nodes = copy.deepcopy(self.nodes, memo)
        result.dirty_node_ids = copy.deepcopy(self.dirty_node_ids, memo)
        result._key_id_to_node_id = copy.deepcopy(self._key_id_to_node_id, memo)

        # --- Verification/Fixup: Update graph reference in copied nodes ---
        # This loop ensures the back-references are correct. Deepcopy should
        # handle this via the memo, but this provides extra robustness.
        for node in result.nodes.values():
             if node.graph is not result:
                  # This condition should ideally not be met if deepcopy works correctly.
                  node.graph = result # Correct the reference
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


# --- Example Usage ---
if __name__ == '__main__':
    # Explicitly create the group
    shared_group = DGraphGroup()

    # Create graph instance, passing the shared group
    graph = DGraph(group=shared_group)
    wrap = DGraphWrap(graph) # Create wrapper

    # Create specific key types
    key_g1 = GroupKey("Group 1 Data")
    key_p1 = ProducerKey("Producer 1 Data")
    key_c1 = ConsumerKey("Consumer 1 Data")
    key_c2 = ConsumerKey("Consumer 2 Data")
    key_p2 = ProducerKey("Producer 2 Data") # New key

    # Add nodes using the wrapper's get_or_add_node (kind is derived from key)
    node_g1 = wrap.get_or_add(key_g1)
    node_p1 = wrap.get_or_add(key_p1)
    node_c1 = wrap.get_or_add(key_c1)
    node_c2 = wrap.get_or_add(key_c2)
    # Keyless nodes would need a different method if required:
    # node_lonely = graph.add_keyless_node(kind=DGraphNodeKind.CONSUMER)

    print(f"Initial dirty nodes: {graph.dirty_node_ids}")
    graph.clear_dirty()

    # Connect nodes using the wrapper
    print("\nConnecting nodes using wrapper...")
    wrap.connect_nodes(key_c1, key_p1)
    wrap.connect_nodes(key_c2, key_p1)
    wrap.connect_nodes(key_p1, key_g1)

    # --- Test auto-add feature of get_node_by_key ---
    print("\nTesting get_node_by_key auto-add...")
    # Register P2 key in the group, but don't add node to graph yet
    key_p2_id = shared_group.register_key(key_p2)
    print(f"Node for P2 exists before get? {graph.get_node_by_key(key_p2)}") # Should be None initially
    # Now call get_node_by_key - it should add the node
    node_p2 = graph.get_node_by_key(key_p2)
    print(f"Node for P2 after get: {node_p2}")
    assert node_p2 is not None, "get_node_by_key failed to auto-add node" # Add assertion
    if node_p2: # Check added correctly
         print(f"Node P2 correctly added to graph.nodes? {node_p2.internal_id in graph.nodes}")
         print(f"Node P2 correctly added to graph._key_id_to_node_id? {graph._key_id_to_node_id.get(key_p2_id) == node_p2.internal_id}")
    # --- End Test ---


    print("\nGraph after connections and auto-add:")
    for node_id in list(graph.nodes.keys()): print(graph.get_node(node_id))
    print(f"\nDirty nodes after connections: {graph.dirty_node_ids}")
    print(f"Graph has cycle? {graph.has_cycle()}")

    # Introduce cycle
    print("\nIntroducing cycle G1 -> C1...")
    wrap.connect_nodes(key_g1, key_c1)
    print(f"Graph has cycle? {graph.has_cycle()}")

    # Remove cycle
    print("\nRemoving cycle G1 -> C1...")
    wrap.disconnect_nodes(key_g1, key_c1)
    print(f"Graph has cycle? {graph.has_cycle()}")

    # --- Test deepcopy ---
    print("\n--- Testing deepcopy ---")
    graph_copy = copy.deepcopy(graph)

    print("\n--- Copied Graph ---")
    for node_id in list(graph_copy.nodes.keys()): print(graph_copy.get_node(node_id))
    print(f"Copied Group object ID:   {id(graph_copy.group)}")
    copied_node_c1 = graph_copy.get_node(node_c1.internal_id)
    print(f"Copied Node C1 graph ref ID:   {id(copied_node_c1.graph)}")


    # Verify group is shared (same object)
    print(f"\nGroup shared? (graph.group is graph_copy.group): {graph.group is graph_copy.group}")

    # Verify nodes dictionary is a different object (deep copied)
    print(f"Nodes dict shared? (graph.nodes is graph_copy.nodes): {graph.nodes is graph_copy.nodes}")

    # Verify a node object is a different object
    copied_node_c1_from_orig_graph = graph.nodes[node_c1.internal_id]
    copied_node_c1_from_copy_graph = graph_copy.nodes[node_c1.internal_id]
    print(f"Node C1 shared? (graph.nodes[{node_c1.internal_id}] is graph_copy.nodes[{node_c1.internal_id}]): {copied_node_c1_from_orig_graph is copied_node_c1_from_copy_graph}")


    # Verify the copied node points back to the copied graph
    print(f"Copied Node C1 points to copied graph? (copied_node_c1.graph is graph_copy): {copied_node_c1.graph is graph_copy}")

    # Verify modifying copy doesn't affect original
    print("\nModifying copy (disconnect C2 -> P1)...")
    wrap_copy = DGraphWrap(graph_copy)
    wrap_copy.disconnect_nodes(key_c2, key_p1)

    print("Original graph node P1:")
    print(graph.get_node(node_p1.internal_id))
    print("Copied graph node P1:")
    print(graph_copy.get_node(node_p1.internal_id))


    # --- Test key reaping (on original graph) ---
    print("\n--- Testing key reaping (on original graph) ---")
    node_id_c2 = node_c2.internal_id # Get ID before key ref is deleted
    print(f"Node C2 before reap: {graph.get_node(node_id_c2)}")
    key_id_c2 = graph.group.get_id_from_key(key_c2) # Get key ID
    print(f"Key object for C2 exists: {graph.group.get_key_from_id(key_id_c2) is not None}")

    # Simulate key_c2 being garbage collected
    print("Deleting external reference to key_c2 and running GC...")
    key_c2_ref = key_c2 # Keep ref temporarily if needed for other things
    del key_c2
    gc.collect() # Suggest garbage collection

    # Verify weak ref is likely dead (may depend on GC timing)
    print(f"Key object for C2 exists after GC?: {graph.group.get_key_from_id(key_id_c2) is not None}")

    # Run reap_graph_keys
    print("Running graph.group.reap_graph_keys(graph)...")
    reaped_ids = graph.group.reap_graph_keys(graph) # Use the graph's shared group
    print(f"Reaped key IDs: {reaped_ids}")
    print(f"Node C2 after reap: {graph.get_node(node_id_c2)}") # Should be None
    print(f"Dirty nodes after reap: {graph.dirty_node_ids}") # Neighbors of C2 should be dirty
    print(f"Key C2 still in strong map?: {key_c2_ref in graph.group._key_to_internal_id}") # Should be False if key_c2_ref is the original key

    print("\nOriginal Graph after reaping C2:")
    for node_id in list(graph.nodes.keys()): print(graph.get_node(node_id))

    # Clean up temporary ref
    del key_c2_ref
    gc.collect()

