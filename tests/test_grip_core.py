import datetime
import unittest
from grip.grip_key import GripKey
from grip.grip_core import GripRegistryImpl, DuplicateGripKey, TapScope
from grip.grip_tap import GripTapScopeDefiner


class TestGripCore(unittest.TestCase):
    
    def setUp(self):
        self.grip_registry = GripRegistryImpl()
    
    def test_add_grip_key(self):
        grip_registry = self.grip_registry
        key = grip_registry.add.MyKey(42)
        assert isinstance(key, GripKey), f"key is {type(key)}"
        assert grip_registry.ref.MyKey._spec.default == 42
        assert grip_registry.ref.MyKey._spec.data_type is int


    def test_duplicate_grip_key_raises(self):
        grip_registry = self.grip_registry
        grip_registry.add.SomeKey("hello")
        with self.assertRaises(DuplicateGripKey):
            grip_registry.add.SomeKey("world")


    def test_ref_access_and_missing_key(self):
        grip_registry = self.grip_registry
        grip_registry.add.Existing(123)
        assert grip_registry.ref.Existing._spec.default == 123
        with self.assertRaises(KeyError):
            _ = grip_registry.ref.Missing


    def test_lazy_creates_new(self):
        grip_registry = self.grip_registry
        key = grip_registry.lazy.Dynamic
        assert isinstance(key, GripKey)
        with self.assertRaises(KeyError):
            _ = grip_registry.ref.Dynamic


    def test_lazy_then_define_type(self):
        grip_registry = self.grip_registry
        _ = grip_registry.lazy.Config
        with self.assertRaises(KeyError):
            _ = grip_registry.ref.Config

    def test_register_tapscope(self):
        grip_registry = self.grip_registry
        # Create a tapscope directly through attribute access
        scope = grip_registry.scope
        baseUi = scope.add.BaseUi()
        assert isinstance(baseUi, TapScope)
        assert "BaseUi" in grip_registry._tapscopes
        
        assert baseUi == grip_registry.scope.ref.BaseUi
        assert baseUi == grip_registry.scope.get("BaseUi")
        
        adminPages = scope.add.AdminPages(baseUi)
        assert isinstance(adminPages, TapScope)
        assert "AdminPages" in grip_registry._tapscopes
        
        assert baseUi in adminPages.parents()
        

    def test_register_tap_definition(self):
        grip_registry: GripRegistryImpl = self.grip_registry
        # Create Grips for use in our test
        grip_registry.add.UserName(str)
        grip_registry.add.UserId(int)
        grip_registry.add.IsAdmin(bool, False)
        grip_registry.add.BackgroundColour(str, "grey")
        grip_registry.add.FontSize(float, 11)
        grip_registry.add.CurrentDate(datetime.date, datetime.date.today())
        
        # Create a TapScope
        userSettingsScope = grip_registry.scope.add.UserSettings()
        assert isinstance(userSettingsScope, TapScope)
        
        # Define a tapscope with a simple Tap definition
        # that matches UserId with 123 and CurrentDate with 1st April
        # and uses IsAdmin as a parameter. It outputs BackgroundColour and FontSize
        # and uses the IsAdmin parameter to determine the BackgroundColour
        
        scope: GripTapScopeDefiner = grip_registry.scope
        add: GripTapScopeDefiner.Definer = scope.add
        
        userDetails: TapScope = add.UserDetails()
        assert isinstance(userDetails, TapScope)
        userDetails.simple(
                UserId=123, 
                CurrentDate=datetime.date(datetime.date.today().year, 4, 1)
            ) \
            .parameters(grip_registry.ref.IsAdmin) \
            .output.BackgroundColour(
                lambda IsAdmin: "red" if IsAdmin else "blue") \
            .output.FontSize(12) \
            .build()



if __name__ == "__main__":
    unittest.main()
