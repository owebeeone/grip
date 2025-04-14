import datetime
import unittest
from grip.grip_key import GripKey
from grip.grip_core import GripRegistryImpl, DuplicateGripKey, TapScope


class TestGripCore(unittest.TestCase):
    
    def setUp(self):
        self.grip = GripRegistryImpl()
    
    def test_add_grip_key(self):
        grip = self.grip
        key = grip.add.MyKey(42)
        assert isinstance(key, GripKey), f"key is {type(key)}"
        assert grip.ref.MyKey._spec.default == 42
        assert grip.ref.MyKey._spec.data_type is int


    def test_duplicate_grip_key_raises(self):
        grip = self.grip
        grip.add.SomeKey("hello")
        with self.assertRaises(DuplicateGripKey):
            grip.add.SomeKey("world")


    def test_ref_access_and_missing_key(self):
        grip = self.grip
        grip.add.Existing(123)
        assert grip.ref.Existing._spec.default == 123
        with self.assertRaises(KeyError):
            _ = grip.ref.Missing


    def test_lazy_creates_new(self):
        grip = self.grip
        key = grip.lazy.Dynamic
        assert isinstance(key, GripKey)
        with self.assertRaises(KeyError):
            _ = grip.ref.Dynamic


    def test_lazy_then_define_type(self):
        grip = self.grip
        _ = grip.lazy.Config
        with self.assertRaises(KeyError):
            _ = grip.ref.Config

    def test_register_tapscope(self):
        grip = self.grip
        # Create a tapscope directly through attribute access
        scope = grip.scope
        baseUi = scope.add.BaseUi()
        assert isinstance(baseUi, TapScope)
        assert "BaseUi" in grip._tapscopes
        
        assert baseUi == grip.scope.ref.BaseUi
        assert baseUi == grip.scope.get("BaseUi")
        
        adminPages = scope.add.AdminPages(baseUi)
        assert isinstance(adminPages, TapScope)
        assert "AdminPages" in grip._tapscopes
        
        assert baseUi in adminPages.parents()
        

    def test_register_tap_definition(self):
        grip = self.grip
        # Create Grips for use in our test
        grip.add.UserName(str)
        grip.add.UserId(int)
        grip.add.IsAdmin(bool, False)
        grip.add.BackgroundColour(str, "grey")
        grip.add.FontSize(float, 11)
        grip.add.CurrentDate(datetime.date, datetime.date.today())
        
        # Create a TapScope
        userSettingsScope = grip.scope.add.UserSettings()
        assert isinstance(userSettingsScope, TapScope)
        
        # Define a tapscope with a simple Tap definition
        # that matches UserId with 123 and CurrentDate with 1st April
        # and uses IsAdmin as a parameter. It outputs BackgroundColour and FontSize
        # and uses the IsAdmin parameter to determine the BackgroundColour
        
        userDetails: TapScope = grip.scope.add.UserDetails()
        assert isinstance(userDetails, TapScope)
        userDetails.simple(
                UserId=123, 
                CurrentDate=datetime.date(datetime.date.today().year, 4, 1)
            ) \
            .parameters(grip.ref.IsAdmin) \
            .output.BackgroundColour(
                lambda IsAdmin: "red" if IsAdmin else "blue") \
            .output.FontSize(12) \
            .build()



if __name__ == "__main__":
    unittest.main()
