

from datatrees import datatree, dtfield
from abc import ABC, abstractmethod
from typing import Any

class GripKeyBase(ABC):
 
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    
    @property
    @abstractmethod
    def grip(self) -> 'GripRegistry':
        pass
    
    @property
    @abstractmethod
    def spec(self) -> '_GripKeySpec':
        pass
    
    @property
    @abstractmethod
    def pro(self) -> 'GripKeyBase':
        pass
    
    @property
    @abstractmethod
    def main(self) -> 'GripKeyBase':
        pass
    
    @property
    @abstractmethod
    def is_main(self) -> bool:
        pass 
       

@datatree
class GripRegistry(ABC):
    """
    GripRegistry is the registry for GripKeys.
    """
    _keys: dict[str, 'GripKeyBase'] = dtfield(default_factory=dict, repr=False)
    
    def get_grip(self, name: str) -> 'GripKeyBase':
        """
        Get an existing GripKey by name.
        """
        return self._keys[name]

    @abstractmethod
    def add(self, name: str, default: Any = None, data_type: type | None = None) \
        -> 'GripKeyBase':
        """
        Add a new GripKey to the registry.
        """
        pass

    @abstractmethod
    def ref(self, name: str) -> 'GripKeyBase':
        """
        Get an existing GripKey by name. Raises KeyError if the GripKey is not found
        or if it is not yet fully defined.
        """
        pass
    
    @abstractmethod
    def lazy(self, name: str) -> 'GripKeyBase':
        """
        Get an existing GripKey by name if it exists, otherwise create a placeholder.
        Properties of the placeholder are not yet defined and accessing them before
        definition is an error.
        """
        pass


@datatree
class GripDGraphNodeBase(ABC):
    """
    GripDGraphNodeBase is a base class for all GripDGraphNodes.
    """
    pass

@datatree
class GrokRuntimeBase:
    """
    GrokRuntime is the runtime for a Grok application.
    """
    grip_registry: GripRegistry
    
    @abstractmethod
    def log(self, 
            origin_key: GripDGraphNodeBase, 
            msg: str = None, 
            exc: Exception = None,
            data: Any = None):
        pass

