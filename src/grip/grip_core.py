from datatrees import datatree, dtfield
from typing import Any, Optional, Dict, List, Sequence, Callable

from grip.grip_key import GripKey, GripRegistry
from grip.grip_tap import TapDefinition, TapScope, Tap, GripTapScopeDefiner
from grip.grip_base import GripBaseException

# Raised when a duplicate GripKey is defined
class DuplicateGripKey(GripBaseException):
    """Raised when a duplicate GripKey is defined."""
    pass


# GripRegistry is the registry for all GripKeys. It supports strict, lazy, 
# and declarative access.
@datatree
class GripRegistryImpl(GripRegistry):
    """
    Grip is the registry for all GripKeys.
    It supports strict access (.ref), on-demand creation (.lazy),
    and declarative definition of keys (.add).
    """
    _keys: dict[str, GripKey] = dtfield(default_factory=dict, repr=False)
    _tap_definitions: Dict[str, TapDefinition] = dtfield(default_factory=dict, repr=False)
    _tapscopes: Dict[str, TapScope] = dtfield(default_factory=dict, repr=False)

    add: 'GripRegistryImpl.Definer' = dtfield(self_default=lambda self: self.Definer(self))
    ref: 'GripRegistryImpl.Referrer' = dtfield(self_default=lambda self: self.Referrer(self))
    lazy: 'GripRegistryImpl.LazyReferrer' = dtfield(self_default=lambda self: self.LazyReferrer(self))
    scope: 'GripTapScopeDefiner' = dtfield(self_default=GripTapScopeDefiner)
    tap: 'GripRegistryImpl.TapDefiner' = dtfield(self_default=lambda self: self.TapDefiner(self))

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

        def __getattr__(self, name: str):
            try:
                attr = self.__getattribute__(name)
                if attr is not None:
                    return attr
            except AttributeError:
                pass

            if name in self.grip_registry._keys:
                raise DuplicateGripKey(f"GripKey '{name}' already defined")

            key = GripKey(name=name, grip=self.grip_registry)
            self.grip_registry._keys[name] = key

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
        grip: 'GripRegistry'

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

  
    # Defines new Tap definitions using attribute access
    @datatree
    class TapDefiner:
        """
        Namespace for defining new Tap definitions using attribute access.
        Example: grip.tap.UserInfoTap(...) defines a Tap definition called 'UserInfoTap'.
        """
        grip: 'Grip'
        
        def __getattr__(self, name: str):
            try:
                attr = self.__getattribute__(name)
                if attr is not None:
                    return attr
            except AttributeError:
                pass
                
            def definer(
                output_keys: Sequence[GripKey],
                matcher: Matcher,
                production_logic: Callable,
                input_keys: Sequence[GripKey] = None,
                parameter_keys_for_matching: Sequence[GripKey] = None,
                parameter_keys_for_production: Sequence[GripKey] = None,
                tapscopes: Sequence[str] = None,
                weight: float = 1.0,
                tap_factory: Callable[..., Tap] = None
            ):
                if name in self.grip._tap_definitions:
                    raise ValueError(f"Tap definition '{name}' already exists")
                
                # Convert sequences to sets
                output_gripkeys = set(output_keys)
                input_gripkeys = set(input_keys or [])
                parameter_gripkeys_for_matching = set(parameter_keys_for_matching or [])
                parameter_gripkeys_for_production = set(parameter_keys_for_production or [])
                tapscope_names = set(tapscopes or [])
                
                # Create the Tap definition
                tap_def = TapDefinition(
                    name=name,
                    output_gripkeys=output_gripkeys,
                    matcher=matcher,
                    production_logic=production_logic,
                    input_gripkeys=input_gripkeys,
                    parameter_gripkeys_for_matching=parameter_gripkeys_for_matching,
                    parameter_gripkeys_for_production=parameter_gripkeys_for_production,
                    tapscopes=tapscope_names,
                    tier=tier,
                    weight=weight,
                    tap_factory=tap_factory
                )
                
                # Register it with the Grip registry
                self.grip._tap_definitions[name] = tap_def
                
                return self.grip
                
            return definer

  
    # TAP definition registration and management methods
    def register_tap_definition(self, tap_definition: TapDefinition) -> None:
        """
        Register a TAP definition with this Grip registry.
        
        Args:
            tap_definition: The TAP definition to register.
            
        Raises:
            ValueError: If a TAP definition with the same name already exists.
        """
        if tap_definition.name in self._tap_definitions:
            raise ValueError(f"TAP definition with name '{tap_definition.name}' already registered")
            
        self._tap_definitions[tap_definition.name] = tap_definition
    
    def get_tap_definition(self, name: str) -> Optional[TapDefinition]:
        """
        Get a TAP definition by name.
        
        Args:
            name: The name of the TAP definition to get.
            
        Returns:
            The TAP definition if found, None otherwise.
        """
        return self._tap_definitions.get(name)
    
    def get_tap_definitions_for_key(self, key: GripKey, tapscope: Optional[str] = None) -> List[TapDefinition]:
        """
        Get all TAP definitions that can provide the given key, optionally filtered by tapscope.
        
        Args:
            key: The GripKey to find TAP definitions for.
            tapscope: Optional tapscope to filter by.
            
        Returns:
            List of TAP definitions that can provide the given key.
        """
        result = []
        
        for tap_def in self._tap_definitions.values():
            if key in tap_def.output_gripkeys:
                if tapscope is None or tapscope in tap_def.tapscopes:
                    result.append(tap_def)
                    
        return result
    
    # TapScope registration and management methods
    def register_tapscope(self, tapscope: TapScope) -> None:
        """
        Register a TapScope with this Grip registry.
        
        Args:
            tapscope: The TapScope to register.
            
        Raises:
            ValueError: If a TapScope with the same name already exists.
        """
        if tapscope.name in self._tapscopes:
            raise ValueError(f"TapScope with name '{tapscope.name}' already registered")
            
        self._tapscopes[tapscope.name] = tapscope
    
    def get_tapscope(self, name: str) -> Optional[TapScope]:
        """
        Get a TapScope by name.
        
        Args:
            name: The name of the TapScope to get.
            
        Returns:
            The TapScope if found, None otherwise.
        """
        return self._tapscopes.get(name)
