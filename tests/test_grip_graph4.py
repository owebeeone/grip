import unittest
from grip.grip_dgraph import (
    DGraphGroup,
    DGraph,
    DGraphWrap
)
from grip.grip_graph4 import (
    GroupKey,
    ProducerKey,
    ConsumerKey,
    QueryKey,
    ConstraintViolationError
)

# --- Unit Test Class for DGraph Constraints Interaction ---
class TestDGraphConstraints(unittest.TestCase):
    def setUp(self):
        """Set up a graph instance for constraint tests."""
        self.group = DGraphGroup()
        self.graph = DGraph(group=self.group)
        self.wrap = DGraphWrap(self.graph)

        # Create keys of different types using grip_graph4 keys
        self.key_g1 = GroupKey("G1")
        self.key_g2 = GroupKey("G2")
        self.key_p1 = ProducerKey(("G1", "P1"))
        self.key_c1 = ConsumerKey(("G1", "C1"))
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
            self.wrap.connect_nodes(self.key_p1, self.key_c1) # P -> C

        # Example 2: Group cannot connect TO non-Groups
        with self.assertRaises(ConstraintViolationError):
            self.wrap.connect_nodes(self.key_g1, self.key_p1) # G -> P

    def test_disallowed_target_reception(self):
        """Test cases where the target node type cannot receive the connection."""
        # Example 1: Consumer cannot receive connections
        with self.assertRaises(ConstraintViolationError):
            self.wrap.connect_nodes(self.key_g1, self.key_c1) # G -> C

        # Example 2: Group cannot receive from non-Groups
        with self.assertRaises(ConstraintViolationError):
            self.wrap.connect_nodes(self.key_p1, self.key_g1) # P -> G

    def test_disconnect_allowed(self):
        """Test disconnection is allowed by default constraints."""
        # Connect first (these are allowed)
        self.wrap.connect_nodes(self.key_c1, self.key_p1)
        self.wrap.connect_nodes(self.key_g1, self.key_g2)
        self.wrap.connect_nodes(self.key_q1, self.key_p1)
        self.wrap.connect_nodes(self.key_c1, self.key_q1) # Add C->Q connection

        # Disconnect - should not raise ConstraintViolationError
        self.wrap.disconnect_nodes(self.key_c1, self.key_p1)
        self.wrap.disconnect_nodes(self.key_g1, self.key_g2)
        self.wrap.disconnect_nodes(self.key_q1, self.key_p1)
        self.wrap.disconnect_nodes(self.key_c1, self.key_q1) # Disconnect C->Q

        # Verify disconnection (check a few representative ones)
        self.assertNotIn(self.node_p1.internal_id, self.node_c1.forward_links)
        self.assertNotIn(self.node_c1.internal_id, self.node_p1.back_links)
        self.assertNotIn(self.node_g2.internal_id, self.node_g1.forward_links)
        self.assertNotIn(self.node_q1.internal_id, self.node_c1.forward_links) # Verify C->Q disconnect
        self.assertNotIn(self.node_c1.internal_id, self.node_q1.back_links)


# --- Test Runner ---
if __name__ == "__main__":
    unittest.main()


