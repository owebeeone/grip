import unittest
from grip.grip_matcher_spec import MatcherSpec, MatchItem, MatchParam
from grip.grip_key import GripKey

# Mock Grip for testing purposes
class MockGrip:
    pass

class TestMatcherSpec(unittest.TestCase):

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_grip = MockGrip()
        self.key1 = GripKey("key1", self.mock_grip)
        self.key2 = GripKey("key2", self.mock_grip)
        self.key3 = GripKey("key3", self.mock_grip)

    def test_matcher_spec_init(self):
        """Test successful initialization of MatcherSpec."""
        matchers = {
            self.key1: MatchItem(key=self.key1, value="value1")
        }
        parameters = {
            self.key2: MatchParam(key=self.key2)
        }
        spec = MatcherSpec(matchers=matchers, parameters=parameters)
        self.assertEqual(spec.matchers, matchers)
        self.assertEqual(spec.parameters, parameters)

    def test_matcher_spec_duplicate_key_error(self):
        """Test that initializing with duplicate keys raises ValueError."""
        matchers = {
            self.key1: MatchItem(key=self.key1, value="value1")
        }
        parameters = {
            self.key1: MatchParam(key=self.key1) # Duplicate key
        }
        with self.assertRaisesRegex(ValueError, "Duplicate matchers in MatcherSpec"):
            MatcherSpec(matchers=matchers, parameters=parameters)

    def test_matcher_spec_empty_init(self):
        """Test initialization with empty dictionaries."""
        spec = MatcherSpec(matchers={}, parameters={})
        self.assertEqual(spec.matchers, {})
        self.assertEqual(spec.parameters, {})

    def test_match_item_defaults(self):
        """Test MatchItem default weight."""
        item = MatchItem(key=self.key1, value="some_value")
        self.assertEqual(item.key, self.key1)
        self.assertEqual(item.value, "some_value")
        self.assertEqual(item.weight, 1.0)

    def test_match_item_custom_weight(self):
        """Test MatchItem with a custom weight."""
        item = MatchItem(key=self.key1, value="another_value", weight=0.5)
        self.assertEqual(item.key, self.key1)
        self.assertEqual(item.value, "another_value")
        self.assertEqual(item.weight, 0.5)

    def test_match_param_defaults(self):
        """Test MatchParam default weight."""
        param = MatchParam(key=self.key2)
        self.assertEqual(param.key, self.key2)
        self.assertEqual(param.weight, 1.0)

    def test_match_param_custom_weight(self):
        """Test MatchParam with a custom weight."""
        param = MatchParam(key=self.key2, weight=2.0)
        self.assertEqual(param.key, self.key2)
        self.assertEqual(param.weight, 2.0)

if __name__ == '__main__':
    unittest.main() 