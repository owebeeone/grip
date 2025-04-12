import unittest
from grip.grip_core import GripRegistry, GripKey, DuplicateGripKey

class TestGripCore(unittest.TestCase):

    def setUp(self):
        """Set up a new GripRegistry before each test."""
        self.grip = GripRegistry()

    def test_add_grip_key(self):
        """Test adding a GripKey with a default value and inferred type."""
        key = self.grip.add.MyKey(42)
        self.assertIsInstance(key, GripRegistry) # Adding returns the registry itself
        self.assertEqual(self.grip.ref.MyKey._spec.default, 42)
        self.assertIs(self.grip.ref.MyKey._spec.data_type, int)

    def test_duplicate_grip_key_raises(self):
        """Test that adding a duplicate GripKey name raises DuplicateGripKey."""
        self.grip.add.SomeKey("hello")
        with self.assertRaises(DuplicateGripKey):
            self.grip.add.SomeKey("world")

    def test_ref_access_and_missing_key(self):
        """Test accessing an existing key via ref and handling missing keys."""
        self.grip.add.Existing(123)
        self.assertEqual(self.grip.ref.Existing._spec.default, 123)
        with self.assertRaises(KeyError):
            _ = self.grip.ref.Missing

    def test_lazy_creates_new(self):
        """Test that lazy access creates a GripKey but doesn't register it."""
        key = self.grip.lazy.Dynamic
        self.assertIsInstance(key, GripKey)
        # Check that it's not registered yet
        with self.assertRaises(KeyError):
            _ = self.grip.ref.Dynamic

    def test_lazy_then_define_type(self):
        """Test lazy access followed by checking it's not yet defined."""
        _ = self.grip.lazy.Config
        # Check that it's not registered yet
        with self.assertRaises(KeyError):
            _ = self.grip.ref.Config

    def test_define_with_explicit_type(self):
        """Test defining a key with an explicit type and no default value."""
        self.grip.add.Size(data_type=int)
        self.assertIs(self.grip.ref.Size._spec.data_type, int)
        self.assertIsNone(self.grip.ref.Size._spec.default)

if __name__ == "__main__":
    unittest.main()
