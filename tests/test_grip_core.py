import pytest
from grip.grip_core import Grip, GripKey, DuplicateGripKey


def test_add_grip_key():
    grip = Grip()
    key = grip.add.MyKey(42)
    assert isinstance(key, Grip)
    assert grip.ref.MyKey._spec.default == 42
    assert grip.ref.MyKey._spec.data_type is int


def test_duplicate_grip_key_raises():
    grip = Grip()
    grip.add.SomeKey("hello")
    with pytest.raises(DuplicateGripKey):
        grip.add.SomeKey("world")


def test_ref_access_and_missing_key():
    grip = Grip()
    grip.add.Existing(123)
    assert grip.ref.Existing._spec.default == 123
    with pytest.raises(KeyError):
        _ = grip.ref.Missing


def test_lazy_creates_new():
    grip = Grip()
    key = grip.lazy.Dynamic
    assert isinstance(key, GripKey)
    with pytest.raises(KeyError):
        _ = grip.ref.Dynamic


def test_lazy_then_define_type():
    grip = Grip()
    _ = grip.lazy.Config
    with pytest.raises(KeyError):
        _ = grip.ref.Config

def test_define_with_explicit_type():
    grip = Grip()
    grip.add.Size(data_type=int)
    assert grip.ref.Size._spec.data_type is int
    assert grip.ref.Size._spec.default is None

if __name__ == "__main__":
    pytest.main([__file__])
