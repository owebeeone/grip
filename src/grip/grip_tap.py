from dataclasses import fields
from enum import Enum, auto
import inspect
from datatrees import datatree, dtfield, Node
from typing import Any, ClassVar, Optional, Self, Set, Dict, List, Callable, Sequence
import asyncio

from abc import ABC, abstractmethod

from grip.grip_key import GripKey, GripRegistry
from grip.grip_matcher_spec import MatchParam, MatcherSpec
from grip.grip_matcher_spec import MatchItem

from grip.grip_base import GripBaseException



# Raised when a duplicate GripKey is defined
class DuplicateGripKey(GripBaseException):
    """Raised when a duplicate GripKey is defined."""
    pass

class DuplicateScope(GripBaseException):
    """Raised when a duplicate TapScope is defined."""
    pass

class DuplicateOutput(GripBaseException):
    """Raised when a duplicate output is defined."""
    pass

class InvalidOutputUnexpectedLength(GripBaseException):
    """Raised when the number of outputs does not match the number of results."""
    pass

class InvalidTapOutputType(GripBaseException):
    """Raised when the type of the output specifier is invalid."""
    pass

@datatree(frozen=True)
class NOT_PROVIDED_TYPE:
    """
    A special value indicating that a parameter is not provided.
    """
    pass

NOT_PROVIDED = NOT_PROVIDED_TYPE()

# Abstract Tap interface that defines what GROK expects
class Tap(ABC):
    """
    Abstract interface for a Target Attribute Provider (TAP) instance.
    This is the interface that GROK interacts with at runtime.
    """
    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name of this TAP."""
        pass
        
    @property
    @abstractmethod
    def outputs(self) -> set[GripKey]:
        """Return the set of GripKeys this TAP can provide."""
        pass
    
    @property
    @abstractmethod
    def inputs(self) -> set[GripKey]:
        """Return the set of GripKeys this TAP requires."""
        pass

    def activate(self) -> None:
        """
        Activate this TAP and prepare it for data production.
        """
        return
        
    def deactivate(self) -> None:
        """Deactivate this TAP and release any resources."""
        return
    
    @abstractmethod
    def input_data(self, input_data: dict[GripKey, Any]) -> None:
        """
        input_data is a dictionary of GripKeys and their values, it
        is provided by the context.
        """
        pass
    
    @abstractmethod
    def output_data_future(self) -> asyncio.Future[dict[GripKey, Any]]:
        """
        output_data is a dictionary of GripKeys and their values, it
        is the data that this TAP will produce. This returns a future
        that will resolve to the data.
        """
        pass


@datatree
class TapFactory(ABC):
    """
    A factory for creating TAPs.
    """

    @abstractmethod
    def create_tap_from_spec(self, spec: 'TapDefinition') -> Tap:
        """Create a TAP instance from a TapDefinition."""
        pass

class TapRestriction(Enum):
    """
    Restrictions on a TAP that limit what data it can provide depending
    on the phase of the Grok.
    """
    STATELESS = auto() # No state, can be used in any phase.
    MAIN_ONLY = auto() # Only prapagate to main consumers.
    PROSPECTIVE_ONLY = auto() # Only prapagate to prospective consumers.
    # During matching phase only propagate to main consumers.
    PHASE_DEPENDENT_MAIN = auto()
    # During matching phase only propagate to prospective consumers.
    PHASE_DEPENDENT_PROSPECTIVE = auto()

@datatree
class TapPair:
    """
    A pair of TAPs that are used to send data to the consumer.
    
    
    """
    tap_restriction: TapRestriction
    main: Tap   

# TapDefinition that helps GROK understand how to instantiate a Tap
# and deliver the correct data to the TAP.
@datatree
class TapDefinition:
    """
    Definition of a Target Attribute Provider (TAP) that specifies
    what data it can provide and how to produce that data.
    This is registered globally in the Grip registry.
    """
    # Core properties
    name: str
    grip_registry: 'GripRegistry'
    matcher: MatcherSpec
    tap_output: Set[GripKey]
    tapscopes: Set['TapScope']
    tap_factory: Callable[[dict[GripKey, Any]], TapPair]
    tap_parameters: Set[GripKey] = dtfield(default_factory=set) # Data provided by the query context. 
    tap_source_parameters: Set[GripKey] = dtfield(default_factory=set)  # Data provided bt the consumer's context.

    def __post_init__(self):
        # Check that all fields are initialized
        for field in fields(self):
            attr_name = field.name
            attr_value = getattr(self, attr_name)
            if attr_value is NOT_PROVIDED:
                raise ValueError(f"Field '{attr_name}' is not initialized")
    
    def create_tap(self, **kwargs) -> Tap:
        """
        Create a Tap instance based on this definition.
        
        Args:
            **kwargs: Additional arguments to pass to the tap_factory.
            
        Returns:
            A Tap instance.
            
        Raises:
            ValueError: If no tap_factory is provided.
        """
        if self.tap_factory is None:
            raise ValueError(f"No tap_factory provided for TAP '{self.name}'")
            
        return self.tap_factory(definition=self, **kwargs)


@datatree(frozen=True)
class SimpleTapFunction:
    """
    A simple TAP that produces data from a function.
    """
    function: Callable[..., Any]
    inputs: frozenset[GripKey]
    # A single output or a tuple of outputs.
    # Order of the tuple must match the order of the returned 
    # tuple from the function.
    output_or_outputs: tuple[GripKey] | GripKey
    
    def __post_init__(self):
        # Check that the outputs are unique.
        if isinstance(self.output_or_outputs, GripKey):
            return
        elif isinstance(self.output_or_outputs, tuple):
            outs = set(self.output_or_outputs)
            dupes = []
            if len(self.output_or_outputs) != len(outs):
                for out in self.output_or_outputs:
                    try:
                        outs.remove(out)
                    except KeyError:
                        dupes.append(out.name)
                
                raise DuplicateOutput(
                    "Outputs must be unique - " + ", ".join(dupes))
        else:
            raise InvalidTapOutputType(f"Invalid output_or_outputs: {type(self.output_or_outputs)}")

    def call(self, data: dict[GripKey, Any]) -> dict[GripKey, Any]:
        """
        Call the function with the given data.
        """
        result: tuple[Any, ...] | Any = self.function(**data)
        if isinstance(self.outputs, GripKey):
            return {self.outputs: result}
        if not isinstance(result, tuple):
            raise InvalidOutputUnexpectedLength(
                f"Expected tuple, got {type(result)}")
        if len(self.outputs) != len(result):
            raise InvalidOutputUnexpectedLength(
                f"Expected {len(self.outputs)} outputs, got {len(result)}")
        return {out: result[i] for i, out in enumerate(self.outputs)}
    
    @property
    def outputs(self) -> set[GripKey]:
        """
        Return the set of GripKeys that this TAP produces.
        """
        if isinstance(self.output_or_outputs, GripKey):
            return {self.output_or_outputs}
        return set(self.output_or_outputs)
    
class TapControl(Enum):
    """
    The control signals that a TAP can send to the consumer.
    """
    DEACTIVATE = auto()
    
@datatree
class TapContolMessageBase:
    CONTROL_TYPE: ClassVar[TapControl] = None
    pass

@datatree
class DeactivateTap(TapContolMessageBase):
    CONTROL_TYPE: ClassVar[TapControl] = TapControl.DEACTIVATE
    pass
    
class TapMonitor(Enum):
    """
    The monitor signals sent by the TAP to the monitor stream.
    """
    ERROR = auto()

@datatree
class TapMonitorMessageBase:
    MONITOR_TYPE: ClassVar[TapMonitor] = None

@datatree
class TapMonitorError(TapMonitorMessageBase):
    MONITOR_TYPE: ClassVar[TapMonitor] = TapMonitor.ERROR
    error_message: str
    exception: Exception = None

class TapInputStream(Enum):
    CONTROL = auto()
    INPUT = auto()

class TapOutputStream(Enum):
    MONITOR = auto()
    OUTPUT = auto()

@datatree
class SimpleTap(Tap):
    """
    A simple TAP that produces data from a list of functions.
    """
    _name: str
    control_input_grip: GripKey # The grip key for the control input.
    monitor_output_grip: GripKey # The grip key for the error output.
    tap_functions: tuple[SimpleTapFunction]
    active: bool = False
    in_stream_futures: list[asyncio.Future[dict[GripKey, Any]] | None] = dtfield(
        default_factory=[None] * len(TapInputStream))
    out_stream_futures: list[asyncio.Future[dict[GripKey, Any]] | None] = dtfield(
        default_factory=[None] * len(TapOutputStream))

    @property
    def name(self) -> str:
        return self._name
        
    @property
    def outputs(self) -> set[GripKey]:
        result = set()
        for func in self.tap_functions:
            # Make sure the outputs are unique.
            if isinstance(func.outputs, GripKey):
                result.add(func.outputs)
            else:
                result.update(func.outputs)
        return result
    
    @property
    def inputs(self) -> set[GripKey]:
        result = set()
        for func in self.tap_functions:
            result.update(func.inputs)
        return result

    def activate(self) -> None:
        if self.active:
            return
        for stream in TapInputStream:
            self.in_stream_futures[stream.value - 1] = asyncio.Future()
            
        for stream in TapOutputStream:
            self.out_stream_futures[stream.value - 1] = asyncio.Future()
        self.active = True
        asyncio.create_task(self._input_data_listener())
        
    def handle_input_future(self, fut: asyncio.Future[dict[GripKey, Any]]) -> None:
        try:
            input_data = await fut
        except asyncio.CancelledError:
            self.active = False
            logging.error(f"Input future cancelled")
            raise
        
    def handle_control_future(self, fut: asyncio.Future[dict[GripKey, Any]]) -> None:
        try:
            control_data = await fut
        except asyncio.CancelledError:
            self.active = False
            logging.error(f"Control future cancelled")
            raise

    async def _input_data_listener(self) -> None:
        """
        Listen for input data from the control and input streams.
        """
        while self.active:
            curr_futures = self.in_stream_futures.copy()
            done, pending = await asyncio.wait(
                curr_futures,
                return_when=asyncio.FIRST_COMPLETED)
            for fut in done:
                if TapInputStream.CONTROL == curr_futures.index(fut) + 1:
                    self.handle_control_future(fut)
                elif TapInputStream.INPUT == curr_futures.index(fut) + 1:
                    self.handle_input_future(fut)
                    
    async def _send_output_data(self, output_data: dict[GripKey, Any]) -> None:
        """
        Send the output data to the output stream.
        """
        self.out_stream_futures[TapOutputStream.OUTPUT.value - 1].set_result(output_data)
        
    def deactivate(self) -> None:
        assert self.future is not None
        self.active = False
        old_futures = self.stream_futures
        for future in old_futures:
            if future is not None:
                future.cancel()
        self.stream_futures = [None] * len(TapStream)                
        return
    
    def input_data(self, input_data: dict[GripKey, Any]) -> None:
        result = dict()
        for func in self.tap_functions:
            # need to pass the subset of input_data that matches the 
            # function's inputs.
            func_inputs = {
                g: v 
                for g, v in input_data.items() 
                if g in func.inputs}
            result.update(func.call(func_inputs))
        
        previous_future = self.future
        self.future = asyncio.Future()
        previous_future.set_result(result)
    
    def output_data_future(self) -> asyncio.Future[dict[GripKey, Any]]:
        """
        output_data is a dictionary of GripKeys and their values, it
        is the data that this TAP will produce. This returns a future
        that will resolve to the data.
        """
        assert self.future is not None
        return self.future
    

# Simple implementation of a Matcher
@datatree
class SimpleMatcherBuilder:
    """
    A simple implementation of the Matcher protocol that checks for
    required parameters and optional conditions.
    """
    matcher_spec: MatcherSpec = dtfield(default_factory=MatcherSpec)
    
    def add_matchers(self, **matchers: Dict[GripKey, Any]) -> bool:
        # Check that all required parameters are present
        dupes = [key for 
                 key in matchers.keys() 
                 if key in self.matcher_spec.matchers.keys() 
                    or key in self.matcher_spec.parameters.keys()]
        if dupes:
            raise ValueError(f"Trying to add duplicate parameters in m: {dupes}")
            
        # Check that all conditions are satisfied
        match_items = [
            (key, MatchItem(key=key, value=value)) 
            for key, value in matchers.items()]
        self.matcher_spec.matchers.update(match_items)
        self.matcher_spec.validate()
                
        return self
    
    def add_parameters(self, *parameters: list[GripKey, Any]) -> bool:
        # Check that all required parameters are present
        dupes = [key for 
                 key in parameters 
                 if key in self.matcher_spec.matchers.keys() 
                    or key in self.matcher_spec.parameters.keys()]
        if dupes:
            raise ValueError(f"Trying to add duplicate parameters in m: {dupes}")
            
        # Check that all conditions are satisfied
        match_params = [
            (key, MatchParam(key=key)) 
            for key, value in parameters.items()]
        self.matcher_spec.matchers.update(match_params)
        self.matcher_spec.validate()

# TapScope for building Tap definitions with a fluent API
@datatree
class TapScope:
    """
    Builder for a Tap definition using a fluent API.
    Created via tapscope.add.TapName()
    """
    name: str
    grip_registry: 'GripRegistry' = dtfield(compare=False, repr=False, hash=False)
    parent_scopes: List['TapScope'] = None
    simple_counter: int = 0
    
    def __post_init__(self):
        if not isinstance(self.grip_registry, GripRegistry):
            raise ValueError("TapScope must be initialized with a GripRegistry instance, "
                             f"not a {type(self.grip_registry)}")

    def get_grip_registry(self) -> GripRegistry:
        return self.grip_registry

    def parents(self) -> List['TapScope']:
        """
        Return the list of parent scopes for this TapScope.
        """
        return self.parent_scopes
    
    def _next_simple_id(self) -> int:
        """
        Return the next simple ID for this TapScope.
        """
        self.simple_counter += 1
        return self.simple_counter
    
    def _next_simple_name(self) -> int:
        """
        Return the next simple name for this TapScope.
        """
        return f"{self.name}_simple_{self._next_simple_id()}"
    
    def simple(self, **conditions) -> 'TapBuilder':
        """
        Start building a Tap definition with simple matcher conditions.
        
        Args:
            **conditions: Key-value pairs for the matcher conditions.
                          Keys should be string names of GripKeys.
            
        Returns:
            A TapBuilder for continued building.
        """
        
        name = self._next_simple_name()
        
        # Convert string keys to GripKey references
        condition_dict = {}
        for key_name, value in conditions.items():
            grip_key = self.grip_registry._keys.get(key_name)
            if grip_key is None:
                raise KeyError(f"GripKey '{key_name}' not found")
            item = MatchItem(key=grip_key, value=value)
            condition_dict[grip_key] = item
            
        matcher = MatcherSpec(matchers=condition_dict, parameters=set())
        
        return TapBuilder(
            tapscope=self,
            name=name,
            tapscopes=self.parent_scopes[0] if self.parent_scopes else None,
            grip_registry=self.grip_registry,
            matcher=matcher
        )


# Builder for constructing a Tap definition step by step
@datatree
class TapBuilder:
    """
    Builder for a Tap definition using a fluent API.
    Supports method chaining for a declarative definition.
    """
    tapscope: TapScope
    name: str

    # Required fields are injected datatree with default value NOT_PROVIDED. This
    # allows us to inject all fields from TapDefinition and fill them in later.
    tap_def_node: Node[TapDefinition] = Node(TapDefinition, default_if_missing=NOT_PROVIDED)
    
    def get_grip(self, name: str) -> GripKey:
        return self.get_grip_registry().get_grip(name)
    
    def get_grip_registry(self) -> GripRegistry:
        return self.tapscope.get_grip_registry()
    
    def parameters(self, *params: list[GripKey, Any]) -> Self:
        """
        Specify parameters required for production.
        
        Args:
            *params: GripKeys required for production.
            
        Returns:
            Self for method chaining.
        """
        for param in params:
            if not isinstance(param, GripKey):
                raise TypeError(f"Expected GripKey, got {type(param)}")
            self.tap_parameters.add(param)
            
        return self
    
    @property
    def output(self) -> 'TapOutputBuilder':
        """
        Start defining outputs for this Tap definition.
        
        Returns:
            A TapOutputBuilder for defining outputs.
        """
        return TapOutputBuilder(self)
    
    def build(self) -> 'GripRegistry':
        """
        Build and register the Tap definition with the Grip registry.
        
        Returns:
            The Grip instance for method chaining.
        """
        # Create a simple matcher from our conditions and required params
        matcher = SimpleMatcherBuilder(
            required_parameters=self.matcher_required_params,
            conditions=self.matcher_conditions
        )
        
        # Define a production logic function that uses the outputs
        def production_logic(context_params):
            result = {}
            for key, value_or_func in self.output_keys.items():
                if callable(value_or_func):
                    # If it's a function, call it with the relevant parameters
                    param_values = {
                        k.name: context_params.get(k) 
                        for k in self.production_params
                        if k in context_params
                    }
                    result[key] = value_or_func(**param_values)
                else:
                    # Otherwise, use the static value
                    result[key] = value_or_func
            return result
        
        # Create and register the Tap definition
        tapscope_names = [self.tapscope.name] if self.tapscope else []
        
        tap_def = self.tap_def_node(tapscopes=set(tapscope_names))
        TapDefinition(
            name=self.name,
            output_gripkeys=set(self.output_keys.keys()),
            matcher=matcher,
            production_logic=production_logic,
            parameter_gripkeys_for_matching=set(self.matcher_conditions.keys()) | self.matcher_required_params,
            parameter_gripkeys_for_production=self.production_params,
            tapscopes=set(tapscope_names),
            tier=self.tier,
            weight=self.weight
        )
        
        self.grip._tap_definitions[self.name] = tap_def
        
        return self


class TapFactoryBuilder:
    """
    Builds a TapPair from a function.
    """
    tap_output_builder: 'TapOutputBuilder'
    tap_restriction: TapRestriction = dtfield(default=TapRestriction.MAIN_ONLY)
    tap_pair_node: Node[TapPair] = \
        Node(TapPair, exclude='main', default_if_missing=NOT_PROVIDED)
        
    
    def add_function(
        self, 
        output_grip: GripKey,
        params: dict[GripKey, Any], 
        function: Callable[..., Any]) -> 'TapFactoryBuilder':

        factory = lambda: function
        
        return self.tap_pair_node(
            tap_pair=function)

# Builder for defining outputs for a Tap definition
@datatree
class TapOutputBuilder:
    """
    Builder for defining outputs for a Tap definition.
    Accessed via tapBuilder.output.KeyName(value_or_function)
    """
    builder: TapBuilder
    tap_factory_builder: TapFactoryBuilder = dtfield(
        self_default=lambda s: TapFactoryBuilder(
            grip_registry=s.get_grip_registry()),
        compare=False, repr=False, hash=False)
    
    def get_grip_registry(self) -> GripRegistry:
        return self.builder.get_grip_registry()
    
    def __getattr__(self, key_name: str):
        """
        Define an output for the Tap definition.
        
        Args:
            key_name: The name of the GripKey to output.
            
        Returns:
            A function that takes a value or function.
        """
        
        try:
            return self.__getattribute__(key_name)
        except AttributeError:
            pass
        
        
        
        def output_definer(value_or_function):
            # Find or create the GripKey
            grip_key = self.builder.get_grip(key_name)
            if grip_key is None:
                raise KeyError(f"GripKey '{key_name}' not found")
            
            if not callable(value_or_function):
                # It's a static value
                self.tap_factory_builder.add_const_function(output_grip=grip_key)
                function = lambda: value_or_function
                param_names = tuple()
            else:   
                # It's a function
                function = value_or_function
                self.tap_factory_builder.add_function(output_grip=grip_key, function)

                param_names = tuple(
                    inspect.signature(value_or_function).parameters.keys())
            
            params = set(
                self.builder.get_grip(p) for p in param_names)
                
            # Add the output to the builder
            self.builder.tap_output = \
                self.builder.tap_output | params \
                if self.builder.tap_output not in (None, NOT_PROVIDED) \
                else params
            
            # Return the builder for method chaining
            return self.builder
            
        return output_definer




@datatree
class TapDefiner:
    """
    Namespace for defining new Tap definitions using attribute access.
    Example: grip.tap.UserInfoTap(...) defines a Tap definition called 'UserInfoTap'.
    """
    grip: 'Grip'
    
    add: 'Grip.Definer' = dtfield(self_default=lambda self: self.Definer(self))
    ref: 'Grip.Referrer' = dtfield(self_default=lambda self: self.Referrer(self))
    
    
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
        
# Defines new TapScopes using attribute access
@datatree
class GripTapScopeDefiner:
    """
    Namespace for defining new TapScopes using attribute access.
    Example: grip.scope.UserInterface() defines a TapScope called 'UserInterface'.
    """
    grip: 'GripRegistry'
    
    add: 'GripTapScopeDefiner.Definer' = dtfield(
        self_default=lambda self: self.Definer(self.grip))
    ref: 'GripTapScopeDefiner.Referrer' = dtfield(
        self_default=lambda self: self.Referrer(self.grip))
    
    def __post_init__(self):
        if not isinstance(self.grip, GripRegistry):
            raise ValueError("GripTapScopeDefiner must be initialized with a GripRegistry instance, "
                             f"not a {type(self.grip)}")
    
    # def __getattr__(self, name: str):
    #     try:
    #         attr = self.__getattribute__(name)
    #         if attr is not None:
    #             return attr
    #     except AttributeError:
    #         pass
            
    #     def definer(parent_scopes: List[TapScope] = None):
    #         if name in self.grip._tapscopes:
    #             raise ValueError(f"TapScope '{name}' already exists")
                
    #         tapscope = TapScope(name=name, grip=self.grip, parent_scopes=parent_scopes or [])
    #         self.grip._tapscopes[name] = tapscope
    #         return tapscope
        
    #     def __call__(self, *args, **kwargs):
    #         return self.definer(*args, **kwargs)
            
    #     return definer
    
    # Defines new GripKeys. Raises if already defined.
    @datatree
    class Definer:
        """
        Namespace for defining new Tap scopes using attribute access.
        Example: scope.add.MyScope(scope.ref.Inherit) defines a scope MyScope inheriting
        from the scope Inherit.
        """
        grip_registry: 'GripRegistry'
        
        def __post_init__(self):
            if not isinstance(self.grip_registry, GripRegistry):
                raise ValueError("GripTapScopeDefiner must be initialized with a GripRegistry instance, "
                                 f"not a {type(self.grip_registry)}")
        
        # Define the type and default value for this GripKey (once only).
        def apply_spec(self, scope: TapScope, parents: List[TapScope] = None) -> 'GripRegistry':
            if scope.parent_scopes is not None:
                raise ValueError("Scope already defined")
            if any(not isinstance(parent, TapScope) for parent in parents):
                msg = ', '.join(type(parent).__name__ for parent in parents if not isinstance(parent, TapScope))
                raise ValueError(f"Parents provided must be TapScope instances - "
                                 f"provided following non-TapScope types: {msg}")
            scope.parent_scopes = parents
            return scope

        def __getattr__(self, name: str):
            try:
                attr = self.__getattribute__(name)
                if attr is not None:
                    return attr
            except AttributeError:
                pass
            
            if name.startswith('__') and name.endswith('__'):
                return super().__getattr__(name)

            if name in self.grip_registry._tapscopes:
                raise DuplicateScope(f"Scope '{name}' already defined")

            scope = TapScope(name=name, grip_registry=self.grip_registry)
            self.grip_registry._tapscopes[name] = scope

            def scope_definer(*args):
                return self.apply_spec(scope=scope, parents=args)

            return scope_definer

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

            if name not in self.grip._tapscopes:
                raise KeyError(f"TapScope '{name}' not found")
            scope = self.grip._tapscopes[name] 
            if scope.parent_scopes is None:
                # This is a lazy key, it's not defined yet.
                raise KeyError(f"TapScope '{name}' definition is not complete")
            return scope

    def get(self, name: str) -> TapScope:
        if name not in self.grip._tapscopes:
            raise KeyError(f"TapScope '{name}' not found")
        return self.grip._tapscopes[name]
        

# # Grip is the registry for all GripKeys. It supports strict, lazy, and declarative access.
# @datatree
# class Grip:
#     """
#     Grip is the registry for all GripKeys and TAPs.
#     It supports strict access (.ref), on-demand creation (.lazy),
#     and declarative definition of keys (.add).
#     """
#     _keys: Dict[str, GripKey] = dtfield(default_factory=dict, repr=False)
#     _tap_definitions: Dict[str, TapDefinition] = dtfield(default_factory=dict, repr=False)
#     _tapscopes: Dict[str, TapScope] = dtfield(default_factory=dict, repr=False)

#     add: 'Grip.Definer' = dtfield(self_default=lambda self: self.Definer(self))
#     ref: 'Grip.Referrer' = dtfield(self_default=lambda self: self.Referrer(self))
#     lazy: 'Grip.LazyReferrer' = dtfield(self_default=lambda self: self.LazyReferrer(self))
#     scope: 'GripTapScopeDefiner' = dtfield(self_default=GripTapScopeDefiner)
#     tap: 'Grip.TapDefiner' = dtfield(self_default=lambda self: self.TapDefiner(self))

#     # Defines new GripKeys. Raises if already defined.
#     @datatree
#     class Definer:
#         """
#         Namespace for defining new GripKeys using attribute access.
#         Example: grip.add.MyKey(42) defines a GripKey called 'MyKey'.
#         """
#         grip: 'Grip'
        
#         # Define the type and default value for this GripKey (once only).
#         def apply_spec(self, 
#                        key, 
#                        arg1: Any = NOT_PROVIDED, 
#                        arg2: Any = NOT_PROVIDED) -> 'Grip':
#             if arg1 is NOT_PROVIDED:
#                 raise ValueError("A type argument is required for a GripKey definition")
#             if isinstance(arg1, type):
#                 data_type = arg1
#                 default = arg2 if arg2 is not NOT_PROVIDED else None
#             elif isinstance(arg2, type):
#                 data_type = arg2
#                 default = arg1
#             elif not isinstance(arg1, type) and arg2 is NOT_PROVIDED:
#                 data_type = type(arg1)
#                 default = arg1
#             else:
#                 raise ValueError("A type argument is required for a GripKey definition")
            
#             if key._spec.data_type is not None:
#                 raise DuplicateGripKey(f"GripKey '{key.name}' already has a data type")

#             key._spec.data_type = data_type
#             key._spec.default = default
#             return key

#         def __getattr__(self, name: str):
#             try:
#                 attr = self.__getattribute__(name)
#                 if attr is not None:
#                     return attr
#             except AttributeError:
#                 pass

#             if name in self.grip._keys:
#                 raise DuplicateGripKey(f"GripKey '{name}' already defined")

#             key = GripKey(name, self.grip)
#             self.grip._keys[name] = key

#             def definer(arg1: Any = NOT_PROVIDED, arg2: Any = NOT_PROVIDED):
#                 """To define a GripKey, provide a type and optional default value or
#                 just the default value with an inferred type.
#                 """
#                 return self.apply_spec(key=key, arg1=arg1, arg2=arg2)

#             return definer

#     # Strict access to only explicitly defined GripKeys
#     @datatree
#     class Referrer:
#         """
#         Namespace for referring to previously defined GripKeys.
#         Raises KeyError if the GripKey is not found.
#         """
#         grip: 'Grip'

#         def __getattr__(self, name: str) -> GripKey:
#             try:
#                 attr = self.__getattribute__(name)
#                 if attr is not None:
#                     return attr
#             except AttributeError:
#                 pass

#             if name not in self.grip._keys:
#                 raise KeyError(f"GripKey '{name}' not found")
#             key = self.grip._keys[name] 
#             if key._spec.data_type is None:
#                 # This is a lazy key, it's not defined yet.
#                 raise KeyError(f"GripKey '{name}' has is not defined")
#             return key

#     # Allows on-demand creation of undefined GripKeys
#     @datatree
#     class LazyReferrer:
#         """
#         Namespace that creates a new GripKey on first access.
#         Use with care—does not enforce upfront declaration.
#         """
#         grip: 'Grip'

#         def __getattr__(self, name: str) -> GripKey:
#             try:
#                 attr = self.__getattribute__(name)
#                 if attr is not None:
#                     return attr
#             except AttributeError:
#                 pass

#             key = self.grip._keys.get(name)
#             if key is None:
#                 key = GripKey(name=name, grip=self.grip)
#                 self.grip._keys[name] = key
#             return key
            

  
#     # TAP definition registration and management methods
#     def register_tap_definition(self, tap_definition: TapDefinition) -> None:
#         """
#         Register a TAP definition with this Grip registry.
        
#         Args:
#             tap_definition: The TAP definition to register.
            
#         Raises:
#             ValueError: If a TAP definition with the same name already exists.
#         """
#         if tap_definition.name in self._tap_definitions:
#             raise ValueError(f"TAP definition with name '{tap_definition.name}' already registered")
            
#         self._tap_definitions[tap_definition.name] = tap_definition
    
#     def get_tap_definition(self, name: str) -> Optional[TapDefinition]:
#         """
#         Get a TAP definition by name.
        
#         Args:
#             name: The name of the TAP definition to get.
            
#         Returns:
#             The TAP definition if found, None otherwise.
#         """
#         return self._tap_definitions.get(name)
    
#     def get_tap_definitions_for_key(self, key: GripKey, tapscope: Optional[str] = None) -> List[TapDefinition]:
#         """
#         Get all TAP definitions that can provide the given key, optionally filtered by tapscope.
        
#         Args:
#             key: The GripKey to find TAP definitions for.
#             tapscope: Optional tapscope to filter by.
            
#         Returns:
#             List of TAP definitions that can provide the given key.
#         """
#         result = []
        
#         for tap_def in self._tap_definitions.values():
#             if key in tap_def.output_gripkeys:
#                 if tapscope is None or tapscope in tap_def.tapscopes:
#                     result.append(tap_def)
                    
#         return result
    
#     # TapScope registration and management methods
#     def register_tapscope(self, tapscope: TapScope) -> None:
#         """
#         Register a TapScope with this Grip registry.
        
#         Args:
#             tapscope: The TapScope to register.
            
#         Raises:
#             ValueError: If a TapScope with the same name already exists.
#         """
#         if tapscope.name in self._tapscopes:
#             raise ValueError(f"TapScope with name '{tapscope.name}' already registered")
            
#         self._tapscopes[tapscope.name] = tapscope
    
#     def get_tapscope(self, name: str) -> Optional[TapScope]:
#         """
#         Get a TapScope by name.
        
#         Args:
#             name: The name of the TapScope to get.
            
#         Returns:
#             The TapScope if found, None otherwise.
#         """
#         return self._tapscopes.get(name)
