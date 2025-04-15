import unittest
import copy
from typing import Any

# Assuming GripKey is in grip.grip_key
from grip.grip_base import DuplicateGripKey
from grip.grip_key import GripKey, GripRegistryImpl


# Mock Grip class for testing purposes
class MockGrip:
    def __init__(self, name="test_grip"):
        self.name = name

    def __repr__(self) -> str:
        return f"MockGrip(name='{self.name}')"

    # Add other methods/attributes if GripKey interacts more deeply
    pass


class TestGripKey(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures."""
        self.mock_grip = MockGrip()
        self.key_name = "my_key"
        self.main_key = GripKey(self.key_name, self.mock_grip)
        self.pro_key = self.main_key.pro
        self.main_key.spec.data_type = int
        self.main_key.spec.default = 100
        
        self.key2_name = "my_key2"
        self.main_key2 = GripKey(self.key2_name, grip=self.mock_grip)
        self.pro_key2 = self.main_key2.pro
        self.main_key2.spec.data_type = int
        self.main_key2.spec.default = 200
        
    def test_definition(self):
        """Test definition of GripKeyMain and GripKeyMain.Prospective."""
        
        self.assertEqual(self.main_key.name, self.key_name)
        self.assertIs(self.main_key.grip, self.mock_grip)
        self.assertEqual(self.pro_key.name, self.key_name)
        self.assertIs(self.pro_key.grip, self.mock_grip)
        
        
        
    def test_hash_and_equality(self):
        """Test hash and equality of GripKeyMain and GripKeyMain.Prospective."""
        self.assertNotEqual(self.main_key, self.main_key2)
        self.assertNotEqual(hash(self.main_key), hash(self.main_key2))
        self.assertNotEqual(self.pro_key, self.pro_key2)
        self.assertNotEqual(hash(self.pro_key), hash(self.pro_key2))
        
        self.assertEqual(self.main_key, self.main_key)
        self.assertEqual(hash(self.main_key), hash(self.main_key))
        self.assertEqual(self.pro_key, self.pro_key)
        self.assertEqual(hash(self.pro_key), hash(self.pro_key))
        
        self.assertNotEqual(self.main_key, self.pro_key)
        self.assertNotEqual(hash(self.main_key), hash(self.pro_key))
        
        hash_before = hash(self.main_key)
        self.main_key.spec.data_type = float
        self.assertEqual(hash(self.main_key), hash_before)
        
        hash_before = hash(self.main_key)
        self.main_key.spec.data_type = int
        self.main_key.spec.default = 23


    def test_grip_key_main_spec_modification(self):
        """Test modifying the spec (indirectly, as spec is mutable)."""

        self.assertEqual(self.main_key.spec.data_type, int)
        self.assertEqual(self.main_key.spec.default, 100)
        # Ensure prospective key also sees the change
        self.assertEqual(self.pro_key.spec.data_type, int)
        self.assertEqual(self.pro_key.spec.default, 100)


    def test_grip_key_prospective_properties(self):
        """Test properties of GripKeyMain.Prospective."""
        self.assertEqual(self.pro_key.name, self.key_name)
        self.assertIs(self.pro_key.grip, self.mock_grip)
        self.assertIs(self.pro_key.spec, self.main_key.spec) # Should be the same spec object
        self.assertFalse(self.pro_key.is_main)
        self.assertIs(self.pro_key.pro, self.pro_key) # pro of prospective is itself
        self.assertIs(self.pro_key.main, self.main_key)

    def test_deepcopy(self):
        """Test that deepcopy returns the same instance."""
        main_key_copy = copy.deepcopy(self.main_key)
        pro_key_copy = copy.deepcopy(self.pro_key)
        self.assertIs(main_key_copy, self.main_key)
        self.assertIs(pro_key_copy, self.pro_key)

    def test_equality_and_hash(self):
        """Test equality and hashability based on datatree."""
        # Create another identical main key
        main_key_2 = GripKey(self.key_name, self.mock_grip)
        # Create a different main key
        main_key_diff_name = GripKey(name="other_key", grip=self.mock_grip)
        
        # Main keys
        self.assertNotEqual(self.main_key, main_key_2)
        self.assertNotEqual(hash(self.main_key), hash(main_key_2))

        self.assertNotEqual(self.main_key, main_key_diff_name)
        self.assertNotEqual(hash(self.main_key), hash(main_key_diff_name)) # Hash depends only on name

    def test_registry(self):
        """Test registry of GripKeyMain and GripKeyMain.Prospective."""
        grip = GripRegistryImpl()
        grip.add.MyKey(42)
        self.assertEqual(grip.ref.MyKey.spec.default, 42)
        self.assertEqual(grip.ref.MyKey.spec.data_type, int)
        with self.assertRaises(DuplicateGripKey):
            grip.add.MyKey(42)
            
        with self.assertRaises(KeyError):
            grip.ref.OtherKey

        lazy_key = grip.lazy.OtherKey
        self.assertEqual(lazy_key.spec.default, None)
        self.assertEqual(lazy_key.spec.data_type, None)
        
    def test_accessors(self):
        """Test accessors of GripRegistryImpl."""
        grip = GripRegistryImpl()
        grip.add("MyKey", 42, float)
        self.assertEqual(grip.ref("MyKey").spec.default, 42)
        self.assertEqual(grip.ref("MyKey").spec.data_type, float)
        
        lazy_key = grip.lazy.OtherKey
        self.assertEqual(lazy_key, grip.lazy("OtherKey"))
        self.assertEqual(grip.lazy("OtherKey").spec.default, None)
        self.assertEqual(grip.lazy("OtherKey").spec.data_type, None)
        
        with self.assertRaises(KeyError):
            grip.ref("OtherKey")
            
        grip.add("OtherKey", 42, float)
        self.assertEqual(grip.lazy("OtherKey").spec.default, 42)
        self.assertEqual(grip.lazy("OtherKey").spec.data_type, float)
        
        self.assertEqual(grip.ref.OtherKey, grip.lazy("OtherKey"))
        

if __name__ == '__main__':
    unittest.main()
