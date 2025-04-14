import sys
import unittest
from grip.grip_core import GripRegistry, GripRegistry, TapScope, TapDefinition
import datetime

with open("sys_path.txt", "w") as f:
    print(sys.path, file=f)


class TestTapBuilder(unittest.TestCase):
    def test_fluent_tap_builder(self):
        # Create a Grip instance
        grip = GripRegistry()

        # Define some GripKeys
        grip.add.UserId(int)
        grip.add.CurrentDate(datetime.date)
        grip.add.IsAdmin(bool)
        grip.add.BackgroundColour(str)
        grip.add.FontSize(int)

        # Create a user settings scope
        userSettingsScope = grip.scope.UserSettings()
        self.assertEqual(userSettingsScope.name, "UserSettings")

        # Define a tapscope with a simple Tap definition
        # that matches UserId with 123 and CurrentDate with 1st April
        # and uses IsAdmin as a parameter. It outputs BackgroundColour and FontSize
        # and uses the IsAdmin parameter to determine the BackgroundColour
        userDetails = userSettingsScope.add.UserDetails()
        self.assertIsInstance(userDetails, TapScope)

        # Create the tap definition using the fluent API
        result = (
            userDetails.simple(UserId=123, CurrentDate=datetime.date(datetime.date.today().year, 4, 1))
            .parameters(grip.ref.IsAdmin)
            .output.BackgroundColour(lambda IsAdmin: "red" if IsAdmin else "blue")
            .output.FontSize(12)
            .build()
        )

        # Verify the result is the Grip instance
        self.assertIs(result, grip)

        # Check that the tap definition was registered
        tap_def = grip.get_tap_definition("UserDetails")
        self.assertIsNotNone(tap_def)
        self.assertEqual(tap_def.name, "UserDetails")

        # Check the output keys
        self.assertIn(grip.ref.BackgroundColour, tap_def.output_gripkeys)
        self.assertIn(grip.ref.FontSize, tap_def.output_gripkeys)

        # Check the parameters
        self.assertIn(grip.ref.IsAdmin, tap_def.parameter_gripkeys_for_production)

        # Check the matcher conditions
        self.assertIn(grip.ref.UserId, tap_def.parameter_gripkeys_for_matching)
        self.assertIn(grip.ref.CurrentDate, tap_def.parameter_gripkeys_for_matching)

        # Check the tapscope
        self.assertIn("UserSettings", tap_def.tapscopes)

        # Test the matcher
        context_params = {
            grip.ref.UserId: 123,
            grip.ref.CurrentDate: datetime.date(datetime.date.today().year, 4, 1),
            grip.ref.IsAdmin: True,
        }
        self.assertTrue(tap_def.matcher.match(context_params))

        # Test with wrong UserId
        wrong_context = {
            grip.ref.UserId: 456,
            grip.ref.CurrentDate: datetime.date(datetime.date.today().year, 4, 1),
            grip.ref.IsAdmin: True,
        }
        self.assertFalse(tap_def.matcher.match(wrong_context))

        # Test the production logic
        result = tap_def.production_logic(context_params)
        self.assertEqual(result[grip.ref.BackgroundColour], "red")
        self.assertEqual(result[grip.ref.FontSize], 12)

        # Test with IsAdmin = False
        context_params[grip.ref.IsAdmin] = False
        result = tap_def.production_logic(context_params)
        self.assertEqual(result[grip.ref.BackgroundColour], "blue")
        self.assertEqual(result[grip.ref.FontSize], 12)

    def test_multiple_taps_in_scope(self):
        # Create a Grip instance
        grip = GripRegistry()

        # Define some GripKeys
        grip.add.UserId(int)
        grip.add.ItemId(int)
        grip.add.ItemName(str)
        grip.add.ItemColor(str)

        # Create scopes
        itemScope = grip.scope.Items()

        # Create multiple taps in the same scope
        itemDetails = itemScope.add.ItemDetails()
        itemDetails.simple(ItemId=1).output.ItemName("First Item").output.ItemColor("Red").build()

        secondItem = itemScope.add.SecondItem()
        secondItem.simple(ItemId=2).output.ItemName("Second Item").output.ItemColor("Blue").build()

        # Test that both taps are registered
        self.assertIn("ItemDetails", grip._tap_definitions)
        self.assertIn("SecondItem", grip._tap_definitions)

        # Test that both are in the same scope
        self.assertIn("Items", grip._tap_definitions["ItemDetails"].tapscopes)
        self.assertIn("Items", grip._tap_definitions["SecondItem"].tapscopes)

        # Test getting taps for a key with scope filter
        item_name_taps = grip.get_tap_definitions_for_key(grip.ref.ItemName, tapscope="Items")
        self.assertEqual(len(item_name_taps), 2)

    def test_callbacks_with_parameters(self):
        # Create a Grip instance
        grip = GripRegistry()

        # Define some GripKeys
        grip.add.UserId(int)
        grip.add.UserName(str)
        grip.add.UserStatus(str)

        # Create scope
        userScope = grip.scope.Users()

        # Create a tap with a callback that uses multiple parameters
        userInfo = userScope.add.UserInfo()
        userInfo.simple().parameters(grip.ref.UserId, grip.ref.UserName).output.UserStatus(
            lambda UserId, UserName: f"User {UserId} ({UserName}) is active"
        ).build()

        # Test the production logic
        tap_def = grip.get_tap_definition("UserInfo")
        context_params = {grip.ref.UserId: 42, grip.ref.UserName: "Alice"}
        result = tap_def.production_logic(context_params)
        self.assertEqual(result[grip.ref.UserStatus], "User 42 (Alice) is active")


if __name__ == "__main__":
    unittest.main()



