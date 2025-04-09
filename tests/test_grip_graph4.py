from dataclasses import dataclass
import unittest
from grip.grip_dgraph import ApplicationKeyBase, DGraphGroup, DGraph, DGraphWrap, DGraphNodeKind
from grip.grip_graph4 import GroupKey, ProducerKey, ConsumerKey, QueryKey, ConstraintViolationError


@dataclass(frozen=True, order=True)
class TestAppKey(ApplicationKeyBase):
    """Application key for testing."""

    app_key: tuple[str, str]


# --- Unit Test Class for DGraph Constraints Interaction ---
class TestDGraphConstraints(unittest.TestCase):
    def setUp(self):
        """Set up a graph instance for constraint tests."""
        self.group = DGraphGroup()
        self.graph = DGraph(group=self.group)
        self.wrap = DGraphWrap(self.graph)
        self.app_key = TestAppKey(("G1", "A1"))
        self.app_key_2 = TestAppKey(("G2", "A2"))
        self.app_key_3 = TestAppKey(("G3", "A3"))
        self.app_key_4 = TestAppKey(("G4", "A4"))

        # Create keys of different types using grip_graph4 keys
        self.key_g1 = GroupKey("G1")
        self.key_g2 = GroupKey("G2")
        self.key_p1 = ProducerKey(("G1", "P1"), self.app_key)
        self.key_c1 = ConsumerKey(("G1", "C1"), self.app_key_2)
        self.key_q1 = QueryKey("Q1_specific")

        # Add nodes corresponding to these keys
        self.node_g1 = self.wrap.get_or_add(self.key_g1)
        self.node_g2 = self.wrap.get_or_add(self.key_g2)
        self.node_p1 = self.wrap.get_or_add(self.key_p1)
        self.node_c1 = self.wrap.get_or_add(self.key_c1)
        self.node_q1 = self.wrap.get_or_add(self.key_q1)

    def test_allowed_connections(self):
        """Test connections that should be allowed by Grip constraints."""
        # Consumer -> Producer
        self.wrap.connect_nodes(self.key_c1, self.key_p1)
        self.assertIn(self.node_p1.internal_id, self.node_c1.forward_links)
        self.assertIn(self.node_c1.internal_id, self.node_p1.back_links)

        # Consumer -> Query
        self.wrap.connect_nodes(self.key_c1, self.key_q1)
        self.assertIn(self.node_q1.internal_id, self.node_c1.forward_links)
        self.assertIn(self.node_c1.internal_id, self.node_q1.back_links)

        # Query -> Producer
        self.wrap.connect_nodes(self.key_q1, self.key_p1)
        self.assertIn(self.node_p1.internal_id, self.node_q1.forward_links)
        self.assertIn(self.node_q1.internal_id, self.node_p1.back_links)

        # Group -> Group
        self.wrap.connect_nodes(self.key_g1, self.key_g2)
        self.assertIn(self.node_g2.internal_id, self.node_g1.forward_links)
        self.assertIn(self.node_g1.internal_id, self.node_g2.back_links)

    def test_disallowed_source_initiation(self):
        """Test cases where the source node type cannot initiate the connection."""
        # Example 1: Producer cannot initiate connections
        with self.assertRaises(ConstraintViolationError):
            self.wrap.connect_nodes(self.key_p1, self.key_c1)  # P -> C

        # Example 2: Group cannot connect TO non-Groups
        with self.assertRaises(ConstraintViolationError):
            self.wrap.connect_nodes(self.key_g1, self.key_p1)  # G -> P

    def test_disallowed_target_reception(self):
        """Test cases where the target node type cannot receive the connection."""
        # Example 1: Consumer cannot receive connections
        with self.assertRaises(ConstraintViolationError):
            self.wrap.connect_nodes(self.key_g1, self.key_c1)  # G -> C

    def test_disconnect_allowed(self):
        """Test disconnection is allowed by default constraints."""
        # Connect first (these are allowed)
        self.wrap.connect_nodes(self.key_c1, self.key_p1)
        self.wrap.connect_nodes(self.key_g1, self.key_g2)
        self.wrap.connect_nodes(self.key_q1, self.key_p1)
        self.wrap.connect_nodes(self.key_c1, self.key_q1)  # Add C->Q connection

        # Disconnect - should not raise ConstraintViolationError
        self.wrap.disconnect_nodes(self.key_c1, self.key_p1)
        self.wrap.disconnect_nodes(self.key_g1, self.key_g2)
        self.wrap.disconnect_nodes(self.key_q1, self.key_p1)
        self.wrap.disconnect_nodes(self.key_c1, self.key_q1)  # Disconnect C->Q

        # Verify disconnection (check a few representative ones)
        self.assertNotIn(self.node_p1.internal_id, self.node_c1.forward_links)
        self.assertNotIn(self.node_c1.internal_id, self.node_p1.back_links)
        self.assertNotIn(self.node_g2.internal_id, self.node_g1.forward_links)
        self.assertNotIn(
            self.node_q1.internal_id, self.node_c1.forward_links
        )  # Verify C->Q disconnect
        self.assertNotIn(self.node_c1.internal_id, self.node_q1.back_links)

    def test_context_uniqueness_constraint(self):
        """Test uniqueness of (app_key, kind) within a Group/Query context."""
        # --- Setup for Uniqueness Test ---
        app_key_unique = TestAppKey(("Hello", "World"))

        # Keys for Producers/Consumers sharing the same app key
        key_p_unique_1 = ProducerKey("P_unique_1", app_key_unique)
        key_p_unique_2 = ProducerKey("P_unique_2", app_key_unique)  # Different key, same app_key
        # key_c_unique_1 = ConsumerKey("C_unique_1", app_key_unique) # See note below

        # Add corresponding nodes
        node_p_unique_1 = self.wrap.get_or_add(key_p_unique_1)
        node_p_unique_2 = self.wrap.get_or_add(key_p_unique_2)
        # node_c_unique_1 = self.wrap.get_or_add(key_c_unique_1) # See note below

        # --- Test Producer Uniqueness within Context (Group G1) ---

        # 1. Connect first producer P_unique_1 -> G1 (Allowed by Producer, Checked by Group)
        self.wrap.connect_nodes(key_p_unique_1, self.key_g1)
        self.assertIn(self.node_g1.internal_id, node_p_unique_1.forward_links)  # P -> G link
        self.assertIn(node_p_unique_1.internal_id, self.node_g1.back_links)  # G <- P link
        # Verify G1's context index
        expected_index_key_p = (app_key_unique, DGraphNodeKind.PRODUCER)
        self.assertIsNotNone(self.node_g1.context_resource_index)
        self.assertIn(expected_index_key_p, self.node_g1.context_resource_index)
        self.assertEqual(
            self.node_g1.context_resource_index[expected_index_key_p], node_p_unique_1.internal_id
        )

        # 2. Try connecting second producer P_unique_2 (same app_key) -> G1
        # Group G1's check_can_receive_connection_from should fail
        with self.assertRaisesRegex(ConstraintViolationError, ".*Cannot connect Node.*"):
            self.wrap.connect_nodes(key_p_unique_2, self.key_g1)
        # Verify G1's index hasn't changed
        self.assertEqual(len(self.node_g1.context_resource_index), 1)
        self.assertEqual(
            self.node_g1.context_resource_index[expected_index_key_p], node_p_unique_1.internal_id
        )

        # --- Test Consumer with Same App Key (Currently disallowed C->G connection) ---
        # NOTE: Based on current constraints (Consumer ALLOWED_TARGETS = {PRODUCER, QUERY}),
        # a Consumer cannot connect TO a Group. Therefore, the uniqueness check for Consumers
        # within a Group context via connect_nodes(C -> G) is not directly testable this way.
        # If the model changes (e.g., G -> C connection or different registration mechanism),
        # this test needs updating.

        # --- Test Uniqueness Isolation Between Contexts ---

        # Connect P_unique_1 -> G2 (Different context node G2) - Should succeed
        self.wrap.connect_nodes(key_p_unique_1, self.key_g2)
        self.assertIsNotNone(self.node_g2.context_resource_index)
        self.assertIn(expected_index_key_p, self.node_g2.context_resource_index)
        self.assertEqual(
            self.node_g2.context_resource_index[expected_index_key_p], node_p_unique_1.internal_id
        )
        # Ensure G1's index is still correct
        self.assertEqual(
            self.node_g1.context_resource_index[expected_index_key_p], node_p_unique_1.internal_id
        )

        # --- Test Disconnect and Reconnect ---

        # Disconnect P_unique_1 -> G1
        self.wrap.disconnect_nodes(key_p_unique_1, self.key_g1)
        self.assertNotIn(expected_index_key_p, self.node_g1.context_resource_index)

        # Now connect P_unique_2 -> G1 (Should succeed as P1 was removed)
        self.wrap.connect_nodes(key_p_unique_2, self.key_g1)
        self.assertIn(expected_index_key_p, self.node_g1.context_resource_index)
        self.assertEqual(
            self.node_g1.context_resource_index[expected_index_key_p],
            node_p_unique_2.internal_id,  # Should now point to P2
        )


# --- Test Runner ---
if __name__ == "__main__":
    unittest.main()
