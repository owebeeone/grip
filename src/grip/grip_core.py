from datatrees import datatree, dtfield
from typing import Any, Optional

# Base exception for GRIP-related errors
class GripBaseException(Exception):
    """Base exception for GRIP-related errors."""
    pass

# Raised when a duplicate GripKey is defined
class DuplicateGripKey(GripBaseException):
    """Raised when a duplicate GripKey is defined."""
    pass

# Internal spec for a GripKey containing its type and default value
@datatree
class _GripKeySpec:
    """Internal spec for a GripKey containing its type and default value."""
    data_type: Optional[type] = None
    default: Any = None

# GripKey is an identifier for a value managed by GRIP.
# It is bound to a parent Grip and contains type/default spec.
@datatree(frozen=True)
class GripKey:
    """
    GripKey is an identifier for a value managed by GRIP.
    It is bound to a parent Grip and contains type/default spec.
    """
    name: str
    grip: 'Grip' = dtfield(compare=False, repr=False, hash=False)
    _spec: _GripKeySpec = dtfield(default_factory=_GripKeySpec, compare=False, repr=False, hash=False)


# Grip is the registry for all GripKeys. It supports strict, lazy, and declarative access.
@datatree
class Grip:
    """
    Grip is the registry for all GripKeys.
    It supports strict access (.ref), on-demand creation (.lazy),
    and declarative definition of keys (.add).
    """
    _keys: dict[str, GripKey] = dtfield(default_factory=dict, repr=False)

    add: 'Grip.Definer' = dtfield(self_default=lambda self: self.Definer(self))
    ref: 'Grip.Referrer' = dtfield(self_default=lambda self: self.Referrer(self))
    lazy: 'Grip.LazyReferrer' = dtfield(self_default=lambda self: self.LazyReferrer(self))

    # Defines new GripKeys. Raises if already defined.
    @datatree
    class Definer:
        """
        Namespace for defining new GripKeys using attribute access.
        Example: grip.add.MyKey(42) defines a GripKey called 'MyKey'.
        """
        grip: 'Grip'
        
        # Define the type and default value for this GripKey (once only).
        def apply_spec(self, key, default: Any = None, data_type: Optional[type] = None) -> 'Grip':
            if data_type is None and default is not None:
                data_type = type(default)

            if key._spec.data_type is not None:
                raise DuplicateGripKey(f"GripKey '{key.name}' already has a data type")

            key._spec.data_type = data_type
            key._spec.default = default
            return key.grip

        def __getattr__(self, name: str):
            try:
                attr = self.__getattribute__(name)
                if attr is not None:
                    return attr
            except AttributeError:
                pass

            if name in self.grip._keys:
                raise DuplicateGripKey(f"GripKey '{name}' already defined")

            key = GripKey(name=name, grip=self.grip)
            self.grip._keys[name] = key

            def definer(default: Any = None, data_type: Optional[type] = None):
                return self.apply_spec(key=key, default=default, data_type=data_type)

            return definer

    # Strict access to only explicitly defined GripKeys
    @datatree
    class Referrer:
        """
        Namespace for referring to previously defined GripKeys.
        Raises KeyError if the GripKey is not found.
        """
        grip: 'Grip'

        def __getattr__(self, name: str) -> GripKey:
            try:
                attr = self.__getattribute__(name)
                if attr is not None:
                    return attr
            except AttributeError:
                pass

            if name not in self.grip._keys:
                raise KeyError(f"GripKey '{name}' not found")
            key = self.grip._keys[name] 
            if key._spec.data_type is None:
                # This is a lazy key, it's not defined yet.
                raise KeyError(f"GripKey '{name}' has is not defined")
            return key

    # Allows on-demand creation of undefined GripKeys
    @datatree
    class LazyReferrer:
        """
        Namespace that creates a new GripKey on first access.
        Use with care—does not enforce upfront declaration.
        """
        grip: 'Grip'

        def __getattr__(self, name: str) -> GripKey:
            try:
                attr = self.__getattribute__(name)
                if attr is not None:
                    return attr
            except AttributeError:
                pass

            key = self.grip._keys.get(name)
            if key is None:
                key = GripKey(name=name, grip=self.grip)
                self.grip._keys[name] = key
            return key
