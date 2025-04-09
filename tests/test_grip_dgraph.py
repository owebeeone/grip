import unittest
import random
import time
import copy
import math
import gc
from typing import Dict, Set, Optional, List, Tuple, Any
import weakref
from grip.grip_dgraph import (
        DGraphNodeKey,
        DGraphNodeKind,
        DGraphGroup,
        DGraphNode,
        DGraph,
        DGraphWrap,
        DGraphNodeConstraints,
        ApplicationKeyBase
    )

from dataclasses import dataclass, field

# --- Define No-Op Constraints for Testing --- #
class NoOpConstraints(DGraphNodeConstraints):
    """A constraint implementation that does nothing and allows everything."""
    def check_can_connect_to(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def check_can_receive_connection_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def post_connect_to(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def post_receive_connection_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def check_can_disconnect_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def check_can_remove_connection_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def post_disconnect_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def post_remove_connection_from(self, source_node: 'DGraphNode', target_node: 'DGraphNode'): pass
    def check_add_node(self, graph: 'DGraph', node_to_add: 'DGraphNode'): pass

_no_op_constraints = NoOpConstraints()

# --- Define Dummy App Key for Testing --- #
@dataclass(frozen=True, order=True)
class TestAppKey(ApplicationKeyBase):
    id: Any

# --- Specific Key Subclasses for Grip Graph ---

@dataclass(frozen=True, order=True)
class GroupKey(DGraphNodeKey):
    """Key representing a GROUP node."""
    app_key: TestAppKey
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.GROUP
    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _no_op_constraints
    @property
    def application_key(self) -> Optional[ApplicationKeyBase]:
        return self.app_key

@dataclass(frozen=True, order=True)
class ProducerKey(DGraphNodeKey):
    """Key representing a PRODUCER node."""
    app_key: TestAppKey
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.PRODUCER
    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _no_op_constraints
    @property
    def application_key(self) -> Optional[ApplicationKeyBase]:
        return self.app_key

@dataclass(frozen=True, order=True)
class ConsumerKey(DGraphNodeKey):
    """Key representing a CONSUMER node."""
    app_key: TestAppKey
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.CONSUMER
    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _no_op_constraints
    @property
    def application_key(self) -> Optional[ApplicationKeyBase]:
        return self.app_key
    
@dataclass(frozen=True, order=True)
class QueryKey(DGraphNodeKey):
    """Key representing a QUERY node."""
    app_key: TestAppKey
    @property
    def kind(self) -> DGraphNodeKind:
        return DGraphNodeKind.QUERY
    @property
    def constraints(self) -> DGraphNodeConstraints:
        return _no_op_constraints
    @property
    def application_key(self) -> Optional[ApplicationKeyBase]:
        return self.app_key



# --- Helper Function for Creating Random Graphs ---


def create_random_graph(
    N: int,
    L: int,
    U: int,
    M: float,
    std_dev: float,
    acyclic: bool,
    kind_weights: Dict[DGraphNodeKind, float],
    seed: int,
) -> Tuple[DGraphWrap, List[DGraphNodeKey]]:
    """
    Creates a random graph with specified parameters.

    Args:
        N: Number of nodes.
        L: Minimum links per node.
        U: Maximum links per node.
        M: Mean links per node (for Gaussian distribution).
        std_dev: Standard deviation for link count distribution.
        acyclic: If True, only connect node i to node j where j > i.
        kind_weights: Dict mapping DGraphNodeKind to relative probability weight.
        seed: Random seed for reproducibility.

    Returns:
        A tuple containing the DGraphWrap instance and a list of the keys used.
    """
    print(
        f"\nCreating random graph: N={N}, L={L}, U={U}, M={M:.1f}, SD={std_dev:.1f}, acyclic={acyclic}, seed={seed}"
    )
    random.seed(seed)
    # Note: np.random.seed() would be needed if using numpy's random functions

    shared_group = DGraphGroup()
    graph = DGraph(group=shared_group)
    wrap = DGraphWrap(graph)

    kinds = list(kind_weights.keys())
    weights = list(kind_weights.values())
    node_keys: List[DGraphNodeKey] = []
    nodes: List[DGraphNode] = []

    # 1. Create Nodes
    print("Creating nodes...")
    node_creation_start = time.time()
    chosen_kinds = random.choices(kinds, weights=weights, k=N)
    for i in range(N):
        kind = chosen_kinds[i]
        # Create unique key data
        key_data = f"Key_{kind.name}_{i}_{seed}"
        if kind == DGraphNodeKind.GROUP:
            key = GroupKey(TestAppKey(key_data))
        elif kind == DGraphNodeKind.PRODUCER:
            key = ProducerKey(TestAppKey(key_data))
        else:  # Consumer
            key = ConsumerKey(TestAppKey(key_data))

        node = wrap.get_or_add(key)  # Uses the key to determine kind
        node_keys.append(key)
        nodes.append(node)
    node_creation_end = time.time()
    print(f"Node creation took {node_creation_end - node_creation_start:.3f}s")

    # 2. Create Links
    print("Creating links...")
    link_creation_start = time.time()
    all_node_ids = [n.internal_id for n in nodes]
    link_count = 0
    for i in range(N):
        source_node = nodes[i]
        source_key = node_keys[i]

        # Determine number of links 'k'
        num_links_float = random.gauss(M, std_dev)
        k = max(L, min(U, int(round(num_links_float))))  # Clamp between L and U

        # Determine potential targets
        if acyclic:
            potential_target_indices = range(i + 1, N)
        else:
            # Exclude self
            potential_target_indices = [idx for idx in range(N) if idx != i]

        # Ensure we don't try to sample more targets than available
        k = min(k, len(potential_target_indices))

        if k > 0:
            chosen_target_indices = random.sample(potential_target_indices, k)
            for target_idx in chosen_target_indices:
                target_key = node_keys[target_idx]
                if wrap.connect_nodes(source_key, target_key):
                    link_count += 1

    link_creation_end = time.time()
    print(f"Link creation ({link_count} links) took {link_creation_end - link_creation_start:.3f}s")

    graph.clear_dirty()  # Start clean for tests
    return wrap, node_keys


# --- Unit Test Class ---


class TestDGraph(unittest.TestCase):
    def assert_graph_equal(self, g1: DGraph, g2: DGraph, msg: Optional[str] = None):
        """Asserts that two DGraph instances are structurally equivalent."""
        self.assertIsNot(g1, g2, f"Graphs should be different instances {msg or ''}")
        self.assertIs(g1.group, g2.group, f"Graphs should share the same group {msg or ''}")
        self.assertEqual(
            set(g1.nodes.keys()),
            set(g2.nodes.keys()),
            f"Graphs should have the same node IDs {msg or ''}",
        )

        for node_id in g1.nodes:
            n1 = g1.nodes[node_id]
            n2 = g2.nodes[node_id]
            self.assertIsNot(n1, n2, f"Node {node_id} should be different instances {msg or ''}")
            self.assertEqual(
                n1.internal_id, n2.internal_id, f"Node {node_id} ID mismatch {msg or ''}"
            )
            self.assertEqual(n1.kind, n2.kind, f"Node {node_id} kind mismatch {msg or ''}")
            self.assertEqual(
                n1.key_internal_id,
                n2.key_internal_id,
                f"Node {node_id} key_id mismatch {msg or ''}",
            )
            self.assertEqual(
                n1.forward_links,
                n2.forward_links,
                f"Node {node_id} forward links mismatch {msg or ''}",
            )
            self.assertEqual(
                n1.back_links, n2.back_links, f"Node {node_id} back links mismatch {msg or ''}"
            )
            self.assertIs(n1.graph, g1, f"Node {node_id} in g1 should point to g1 {msg or ''}")
            self.assertIs(n2.graph, g2, f"Node {node_id} in g2 should point to g2 {msg or ''}")

    def test_basic_add_remove(self):
        """Test adding and removing nodes."""
        seed = 101
        random.seed(seed)
        group = DGraphGroup()
        graph = DGraph(group=group)
        wrap = DGraphWrap(graph)

        key_c1 = ConsumerKey(TestAppKey("C1"))
        key_p1 = ProducerKey(TestAppKey("P1"))

        self.assertEqual(len(graph.nodes), 0)
        self.assertEqual(len(graph.dirty_node_ids), 0)

        # Add C1
        node_c1 = wrap.get_or_add(key_c1)
        self.assertEqual(len(graph.nodes), 1)
        self.assertIn(node_c1.internal_id, graph.nodes)
        self.assertIn(node_c1.internal_id, graph.dirty_node_ids)
        self.assertEqual(node_c1.kind, DGraphNodeKind.CONSUMER)
        self.assertEqual(node_c1.get_key(), key_c1)
        self.assertIsNotNone(graph._key_id_to_node_id.get(node_c1.key_internal_id))
        graph.clear_dirty()

        # Add P1
        node_p1 = wrap.get_or_add(key_p1)
        self.assertEqual(len(graph.nodes), 2)
        self.assertIn(node_p1.internal_id, graph.dirty_node_ids)
        self.assertEqual(node_p1.kind, DGraphNodeKind.PRODUCER)
        graph.clear_dirty()

        # Remove C1
        removed = graph.remove_node(node_c1.internal_id)
        self.assertTrue(removed)
        self.assertEqual(len(graph.nodes), 1)
        self.assertNotIn(node_c1.internal_id, graph.nodes)
        self.assertNotIn(node_c1.internal_id, graph.dirty_node_ids)  # Removed node isn't dirty
        self.assertIsNone(
            graph._key_id_to_node_id.get(node_c1.key_internal_id)
        )  # Check index cleanup

        # Try removing again
        removed_again = graph.remove_node(node_c1.internal_id)
        self.assertFalse(removed_again)

        # Try adding C1 again (should get new ID)
        node_c1_new = wrap.get_or_add(key_c1)
        self.assertEqual(len(graph.nodes), 2)
        self.assertNotEqual(node_c1_new.internal_id, node_c1.internal_id)
        self.assertEqual(node_c1_new.kind, DGraphNodeKind.CONSUMER)

    def test_key_uniqueness_violation(self):
        """Test that adding a node with an existing key raises ValueError."""
        seed = 102
        random.seed(seed)
        group = DGraphGroup()
        graph = DGraph(group=group)
        wrap = DGraphWrap(graph)
        key_p1 = ProducerKey(TestAppKey("P1_Unique"))

        wrap.get_or_add(key_p1)  # Add first time
        # Try adding again with same key - should raise error
        with self.assertRaises(ValueError):
            graph.add_node(key=key_p1)

    def test_connect_disconnect_dirty(self):
        """Test connecting, disconnecting, and dirty flags."""
        seed = 103
        random.seed(seed)
        group = DGraphGroup()
        graph = DGraph(group=group)
        wrap = DGraphWrap(graph)

        key_c1 = ConsumerKey(TestAppKey("C1"))
        key_p1 = ProducerKey(TestAppKey("P1"))
        node_c1 = wrap.get_or_add(key_c1)
        node_p1 = wrap.get_or_add(key_p1)
        graph.clear_dirty()

        # Connect C1 -> P1
        connected = wrap.connect_nodes(key_c1, key_p1)
        self.assertTrue(connected)
        self.assertIn(node_p1.internal_id, node_c1.forward_links)
        self.assertIn(node_c1.internal_id, node_p1.back_links)
        self.assertEqual(graph.dirty_node_ids, {node_c1.internal_id, node_p1.internal_id})
        graph.clear_dirty()

        # Connect again (should do nothing, not mark dirty)
        connected_again = wrap.connect_nodes(key_c1, key_p1)
        self.assertTrue(connected_again)  # Still returns true as nodes exist
        self.assertEqual(len(graph.dirty_node_ids), 0)

        # Disconnect C1 -> P1
        disconnected = wrap.disconnect_nodes(key_c1, key_p1)
        self.assertTrue(disconnected)
        self.assertNotIn(node_p1.internal_id, node_c1.forward_links)
        self.assertNotIn(node_c1.internal_id, node_p1.back_links)
        self.assertEqual(graph.dirty_node_ids, {node_c1.internal_id, node_p1.internal_id})
        graph.clear_dirty()

        # Disconnect again (should do nothing)
        disconnected_again = wrap.disconnect_nodes(key_c1, key_p1)
        self.assertTrue(disconnected_again)  # Still returns true
        self.assertEqual(len(graph.dirty_node_ids), 0)

    def test_key_reaping(self):
        """Test removing nodes via key reaping."""
        seed = 104
        random.seed(seed)
        group = DGraphGroup()
        graph = DGraph(group=group)
        wrap = DGraphWrap(graph)

        key_c1 = ConsumerKey(TestAppKey("C1_Reap"))
        key_p1 = ProducerKey(TestAppKey("P1_Reap"))
        node_c1 = wrap.get_or_add(key_c1)
        node_p1 = wrap.get_or_add(key_p1)
        wrap.connect_nodes(key_c1, key_p1)
        graph.clear_dirty()

        key_id_c1 = group.get_id_from_key(key_c1)
        node_id_c1 = node_c1.internal_id
        node_id_p1 = node_p1.internal_id

        # Simulate key_c1 GC
        key_c1_ref = weakref.ref(key_c1)
        key_c1 = None
        del key_c1
        gc.collect()
        reaped = key_c1_ref()
        if reaped is None:  # Not reaped...
            # Reap keys
            reaped_ids = group.reap_graph_keys(graph)
            self.assertIsNone(group.get_key_from_id(key_id_c1))  # Verify weak ref is dead
            self.assertEqual(reaped_ids, {key_id_c1})

            # Check node C1 was removed
            self.assertIsNone(graph.get_node(node_id_c1))
            self.assertNotIn(key_id_c1, graph._key_id_to_node_id)

            # Check P1 (neighbor) is dirty and its backlink was removed
            self.assertIn(node_id_p1, graph.dirty_node_ids)
            node_p1_after = graph.get_node(node_id_p1)
            self.assertIsNotNone(node_p1_after)
            self.assertNotIn(node_id_c1, node_p1_after.back_links)

    def test_reversibility(self):
        """Test adding and removing connections leaves the graph unchanged."""
        seed = 105
        N = 50
        T = 100  # Number of connections to add/remove
        wrap, keys = create_random_graph(
            N=N,
            L=0,
            U=10,
            M=3,
            std_dev=2,
            acyclic=False,
            kind_weights={DGraphNodeKind.CONSUMER: 1, DGraphNodeKind.PRODUCER: 1},
            seed=seed,
        )
        graph = wrap.graph
        initial_graph = copy.deepcopy(graph)

        # Add T new connections
        added_connections = set()
        attempts = 0
        while len(added_connections) < T and attempts < T * 10:
            attempts += 1
            idx1, idx2 = random.sample(range(N), 2)  # Choose 2 different nodes
            key1, key2 = keys[idx1], keys[idx2]
            node1, node2 = wrap.get_node(key1), wrap.get_node(key2)

            # Check if connection already exists
            if node2.internal_id not in node1.forward_links:
                if wrap.connect_nodes(key1, key2):
                    added_connections.add((key1, key2))

        self.assertEqual(len(added_connections), T, f"Failed to add {T} unique connections")

        # Remove the added connections
        for key1, key2 in added_connections:
            removed = wrap.disconnect_nodes(key1, key2)
            self.assertTrue(removed, f"Failed to remove connection {key1} -> {key2}")

        # Compare final graph with initial graph
        self.assert_graph_equal(initial_graph, graph, "Graph state mismatch after add/remove")

    def test_cycle_detection(self):
        """Test cycle detection correctness."""
        seed = 106
        random.seed(seed)
        group = DGraphGroup()
        graph = DGraph(group=group)
        wrap = DGraphWrap(graph)

        k = [ConsumerKey(TestAppKey(f"C{i}")) for i in range(5)]
        n = [wrap.get_or_add(ki) for ki in k]

        # No cycle initially
        wrap.connect_nodes(k[0], k[1])
        wrap.connect_nodes(k[1], k[2])
        wrap.connect_nodes(k[3], k[4])
        self.assertFalse(graph.has_cycle(), "Acyclic graph reported cycle")

        # Simple cycle 0->1->2->0
        wrap.connect_nodes(k[2], k[0])
        self.assertTrue(graph.has_cycle(), "Simple cycle not detected")
        # Check subgraph
        self.assertTrue(graph.has_cycle({n[0].internal_id, n[1].internal_id, n[2].internal_id}))
        self.assertFalse(graph.has_cycle({n[3].internal_id, n[4].internal_id}))

        # Remove cycle
        wrap.disconnect_nodes(k[2], k[0])
        self.assertFalse(graph.has_cycle(), "Cycle remained after disconnect")

        # More complex cycle 0->1->2, 0->3->4->1
        wrap.connect_nodes(k[0], k[3])
        wrap.connect_nodes(k[3], k[4])
        wrap.connect_nodes(k[4], k[1])  # Creates 0->3->4->1->2
        self.assertFalse(graph.has_cycle(), "Acyclic graph reported cycle (complex)")
        wrap.connect_nodes(k[2], k[0])  # Creates 0->1->2->0 cycle again
        self.assertTrue(graph.has_cycle(), "Complex cycle not detected")

    def test_cycle_performance(self):
        """Basic performance check for cycle detection."""
        seed = 107
        N = 5000
        print("\n--- Cycle Detection Performance ---")
        wrap, keys = create_random_graph(
            N=N,
            L=0,
            U=8,
            M=4,
            std_dev=2,
            acyclic=True,  # Acyclic first
            kind_weights={DGraphNodeKind.CONSUMER: 1},
            seed=seed,
        )
        graph = wrap.graph

        # Time acyclic check
        start_time = time.perf_counter()
        has_cycle_a = graph.has_cycle()
        end_time = time.perf_counter()
        self.assertFalse(has_cycle_a, "Acyclic graph creation failed")
        print(f"Acyclic check ({N} nodes) took: {end_time - start_time:.4f}s")

        # Add a cycle
        idx1 = N // 2
        while True:
            links = list(graph.get_node(idx1).forward_links)
            if len(links) > 0:
                break
            idx1 = random.randint(0, N // 2)
        idx2 = random.choice(links)
        key1, key2 = keys[idx1], keys[idx2]
        wrap.connect_nodes(key2, key1)

        # Time cyclic check
        start_time = time.perf_counter()
        has_cycle_c = graph.has_cycle()
        end_time = time.perf_counter()
        self.assertTrue(has_cycle_c, "Cycle introduction failed")
        print(f"Cyclic check ({N} nodes) took:  {end_time - start_time:.4f}s")
        print("------------------------------------")

    def test_copy_performance(self):
        """Basic performance check for deepcopy."""
        seed = 108
        C = 5  # Number of copies per test case

        param_sets = [
            {"N": 1000, "L": 0, "U": 10, "M": 3, "SD": 1.5, "KW": {DGraphNodeKind.CONSUMER: 1}},
            {
                "N": 5000,
                "L": 0,
                "U": 20,
                "M": 8,
                "SD": 3.0,
                "KW": {DGraphNodeKind.CONSUMER: 3, DGraphNodeKind.PRODUCER: 1},
            },
            {
                "N": 10000,
                "L": 1,
                "U": 50,
                "M": 15,
                "SD": 5.0,
                "KW": {
                    DGraphNodeKind.GROUP: 1,
                    DGraphNodeKind.PRODUCER: 2,
                    DGraphNodeKind.CONSUMER: 2,
                },
            },
        ]

        print("\n--- Deepcopy Performance ---")
        for params in param_sets:
            wrap, _ = create_random_graph(
                N=params["N"],
                L=params["L"],
                U=params["U"],
                M=params["M"],
                std_dev=params["SD"],
                acyclic=False,
                kind_weights=params["KW"],
                seed=seed,
            )
            graph = wrap.graph
            gc.collect()  # GC before timing

            start_time = time.perf_counter()
            for _ in range(C):
                graph_copy = copy.deepcopy(graph)
            end_time = time.perf_counter()

            duration = end_time - start_time
            print(
                f"Params: N={params['N']}, M={params['M']:.1f} | "
                f"Total time for {C} copies: {duration:.4f}s | "
                f"Avg time/copy: {duration / C:.4f}s"
            )
            # Basic check on copy
            self.assertIsNot(graph, graph_copy)
            self.assertIs(graph.group, graph_copy.group)
            self.assertEqual(len(graph.nodes), len(graph_copy.nodes))

        print("--------------------------")


# --- Test Runner ---
if __name__ == "__main__":
    unittest.main()
