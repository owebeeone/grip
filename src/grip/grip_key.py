# Internal spec for a GripKey containing its type and default value
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional

from datatrees.datatrees import datatree, dtfield

from grip.grip_base import DuplicateGripKey
from grip.grip_graph import ApplicationKeyBase
from grip.grip_interfaces import GripRegistry

from grip.grip_interfaces import GripKeySpec, GripKeyBase

@datatree
class _GripKeySpec:
    """Internal spec for a GripKey containing its type and default value."""
    data_type: type | None = None
    default: Any = None
    
    def __deepcopy__(self, memo: dict) -> 'GripKey':
        return self


# GripKey is an identifier for a value managed by GRIP.
# It is bound to a parent Grip and contains type/default spec.
@datatree(frozen=True, init=False, slots=True)
class GripKey(GripKeyBase):
    """
    GripKey is an identifier for a value managed by GRIP.
    It is bound to a parent Grip and contains type/default spec.
    """
    _name: str
    _is_main: bool = True
    _grip: 'GripRegistry' = dtfield(compare=False, repr=False, hash=False)
    _spec: _GripKeySpec = dtfield(
        default_factory=_GripKeySpec, compare=False, repr=False, hash=False)
    _alt: 'GripKey' = dtfield(compare=False, repr=False, hash=False)
    _id: int
    _id_counter: ClassVar[int] = 1
    
    def __init__(self, *args, **kwds):
        """
        A GripKey can be initialized with a name and grip, or with a main GripKey.
        If initialized with a main GripKey, the GripKey will be a prospective GripKey.
        
        Can't use the dataclasses constructor because it doesn't support
        the alternative GripKey initialization. 
        
        Args:
            *args:
                name: str
                grip: Grip
            **kwds:
                _alt: GripKey
                _name: str
                _grip: Grip
                
        From a comparison standpoint, a gripkey is only equal to itself since the id
        is different for each instance. When creating a new GripKey, using the a
        (name, grip) constructor, the pro key is automatically created.
        """
        alt = kwds.get('_alt', None)
        if alt is None:
            # Initialize with name and grip
            name = args[0] if len(args) > 0 else kwds['name']
            grip = args[1] if len(args) > 1 else kwds['grip']
            is_main = True
            spec = _GripKeySpec()
        else:
            # Initialize with a GripKey to make an alternative GripKey
            assert len(args) == 0, "Alternative GripKey initialization doesn't support args"
            assert 'name' not in kwds, "Alternative GripKey initialization doesn't support name"
            assert 'grip' not in kwds, "Alternative GripKey initialization doesn't support grip"
            name = alt.name
            grip = alt.grip
            is_main = not alt.is_main
            spec = alt.spec
        
        # Initialize the GripKey using object.__setattr__ because frozen dataclasses
        # don't support setting attributes any other way.
        object.__setattr__(self, '_name', name)
        object.__setattr__(self, '_grip', grip)
        object.__setattr__(self, '_is_main', is_main)
        object.__setattr__(self, '_spec', spec)
        object.__setattr__(self, '_id', self._new_id())
        if is_main:
            object.__setattr__(self, '_alt', GripKey(_alt=self))
        else:
            object.__setattr__(self, '_alt', alt)
    
    @classmethod
    def _new_id(cls) -> int:
        cls._id_counter += 1
        return cls._id_counter
    
    @property
    def name(self) -> str:
        return self._name
    
    @property
    def grip(self) -> GripRegistry:
        return self._grip
    
    @property
    def spec(self) -> _GripKeySpec:
        return self._spec
    
    @property
    def pro(self) -> 'GripKey':
        if self._is_main:
            return self._alt
        else:
            return self
    
    @property
    def main(self) -> 'GripKey':
        if self._is_main:
            return self
        else:
            return self._alt
    
    @property
    def is_main(self) -> bool:
        return self._is_main
    
    def __deepcopy__(self, memo: dict) -> 'GripKey':
        return self

# GripRegistry is the registry for all GripKeys. It supports strict, lazy, 
# and declarative access.
@datatree
class GripRegistryImpl(GripRegistry):
    """
    Grip is the registry for all GripKeys.
    It supports strict access (.ref), on-demand creation (.lazy),
    and declarative definition of keys (.add).
    """
    
    add: 'GripRegistryImpl.Definer' = dtfield(self_default=lambda self: self.Definer(self))
    ref: 'GripRegistryImpl.Referrer' = dtfield(self_default=lambda self: self.Referrer(self))
    lazy: 'GripRegistryImpl.LazyReferrer' = dtfield(self_default=lambda self: self.LazyReferrer(self))

    # Defines new GripKeys. Raises if already defined.
    @datatree
    class Definer:
        """
        Namespace for defining new GripKeys using attribute access.
        Example: grip.add.MyKey(42) defines a GripKey called 'MyKey'.
        """
        grip_registry: 'GripRegistry'
        
        # Define the type and default value for this GripKey (once only).
        def apply_spec(self, key, default: Any = None, data_type: Optional[type] = None) -> 'GripRegistry':
            if data_type is None and default is not None:
                data_type = type(default)

            if key._spec.data_type is not None:
                raise DuplicateGripKey(f"GripKey '{key.name}' already has a data type")

            key._spec.data_type = data_type
            key._spec.default = default
            return key
        
        def _insert_key(self, name: str):
            if name in self.grip_registry._keys \
                and self.grip_registry._keys[name]._spec.data_type is not None:
                raise DuplicateGripKey(f"GripKey '{name}' already defined")
            key = GripKey(name=name, grip=self.grip_registry)
            self.grip_registry._keys[name] = key
            return key

        def __call__(self, name: str, default: Any = None, data_type: type | None = None) -> 'GripRegistryImpl.Definer':
            key = self._insert_key(name)
            return self.apply_spec(key, default, data_type)

        def __getattr__(self, name: str):
            try:
                attr = self.__getattribute__(name)
                if attr is not None:
                    return attr
            except AttributeError:
                pass
            
            key = self._insert_key(name)

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
        grip: 'GripRegistry'
        
        def get(self, name: str) -> GripKey:
            if name not in self.grip._keys:
                raise KeyError(f"GripKey '{name}' not found")
            key = self.grip._keys[name] 
            if key._spec.data_type is None:
                # This is a lazy key, it's not defined yet.
                raise KeyError(f"GripKey '{name}' has is not defined")
            return key
        
        def __call__(self, name: str) -> GripKey:
            return self.get(name)

        def __getattr__(self, name: str) -> GripKey:
            try:
                attr = self.__getattribute__(name)
                if attr is not None:
                    return attr
            except AttributeError:
                pass
            return self.get(name)


    # Allows on-demand creation of undefined GripKeys
    @datatree
    class LazyReferrer:
        """
        Namespace that creates a new GripKey on first access.
        Use with care—does not enforce upfront declaration.
        """
        grip: 'GripRegistry'
        
        def get(self, name: str) -> GripKey:
            key = self.grip._keys.get(name)
            if key is None:
                key = GripKey(name=name, grip=self.grip)
                self.grip._keys[name] = key
            return key
        
        def __call__(self, name: str) -> GripKey:
            return self.get(name)

        def __getattr__(self, name: str) -> GripKey:
            try:
                attr = self.__getattribute__(name)
                if attr is not None:
                    return attr
            except AttributeError:
                pass
            return self.get(name)


