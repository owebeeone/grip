# Internal spec for a GripKey containing its type and default value
from abc import ABC, abstractmethod
from typing import Any, ClassVar, Optional, TYPE_CHECKING

from datatrees.datatrees import datatree, dtfield


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
class GripKey:
    """
    GripKey is an identifier for a value managed by GRIP.
    It is bound to a parent Grip and contains type/default spec.
    """
    _name: str
    _is_main: bool = True
    _grip: 'Grip' = dtfield(compare=False, repr=False, hash=False)
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
            name = args[0] if len(args) > 0 else kwds['_name']
            grip = args[1] if len(args) > 1 else kwds['_grip']
            is_main = True
            spec = _GripKeySpec()
        else:
            # Initialize with a GripKey to make an alternative GripKey
            assert len(args) == 0, "Alternative GripKey initialization doesn't support args"
            assert '_name' not in kwds, "Alternative GripKey initialization doesn't support _name"
            assert '_grip' not in kwds, "Alternative GripKey initialization doesn't support _grip"
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
    def grip(self) -> 'Grip':
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

