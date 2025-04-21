

import asyncio
from copy import copy
from dataclasses import InitVar, field
from datatrees import datatree, dtfield
from abc import ABC, abstractmethod
from typing import Any, Iterator

from grip.grip_graph import ConsumerKey, GroupKey, ProducerKey, ApplicationKeyBase, GraphNodeBase
from grip.grip_stream import GripStream, StreamProcessor, StreamScope


@datatree
class GripKeySpec:
    """Internal spec for a GripKey containing its type and default value."""
    data_type: type | None = None
    default: Any = None
    
    def __deepcopy__(self, memo: dict) -> 'GripKeySpec':
        return self

class GripKeyBase(ABC, ApplicationKeyBase):
 
    @property
    @abstractmethod
    def name(self) -> str:
        pass
    
    @property
    @abstractmethod
    def registry(self) -> 'GripRegistry':
        pass
    
    @property
    @abstractmethod
    def spec(self) -> 'GripKeySpec':
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
class GripGraphContextNode(GroupKey, ABC):
    """
    A "group node" in a GripGraph.
    
    GripContext is a substructure of a GripGraph. It consists of a number
    of nodes. Query contexts allow queries to define output Grips that
    are the same as the input Grips by scoping how the input Grips are
    visible to the resulting graph.
    
    These nodes are only weakly referenced, meaning that they can be 
    garbage collected if they are not referenced by objects outside
    of the graph. The GripContext is responsible for maintaining
    the lifetime of the nodes but as a GripContext is reaped,
    the nodes are also reaped regardless if they are contained within
    a graph or not. i.e. Dropping a GripContext will drop all the
    nodes it contains but not necessarily immediately, it will depend
    on garbage collector behaviour.
    """
    pass


@datatree
class DripMessage:
    """
    A Message is a message from a GripDripFeeder to a GripDrip.
    """
    grip: GripKeyBase
    data: Any

@datatree
class DripBatch:
    """
    A DripBatch contains multiple messages keyed by their GripKey.
    """
    messages: dict[GripKeyBase, DripMessage] = dtfield(default_factory=dict)
    
    def add_message(self, message: DripMessage) -> None:
        self.messages[message.grip] = message
        
    def get_message(self, grip: GripKeyBase) -> DripMessage:
        return self.messages[grip]
    
    def __iter__(self) -> Iterator[DripMessage]:
        return iter(self.messages.values())

class DripStreamScope(StreamScope[DripBatch | DripMessage]):
    """
    A DripStreamScope is a stream scope for a DripStreamData.
    """


@datatree(eq=False)
class DripStream(GripStream[DripBatch | DripMessage]):
    """
    A DripStream is a stream for a Drip.
    """

@datatree(eq=False)
class DripStreamProcessor(StreamProcessor[DripBatch | DripMessage]):
    """
    A DripStreamProcessor is a stream processor for a DripStream.
    """

@datatree(eq=False)
class GripDripFeeder(ABC, ProducerKey):
    """
    A DripFeeder is a dripfeeder for a GripKey.
    """
    pass

@datatree(eq=False)
class GripDrip(ABC, ConsumerKey):
    """
    A Drip is a drip for a GripKey.
    """
    
    grip: InitVar[GripKeyBase]
    grips: set[GripKeyBase] = field(init=False)
    _client_streams: set[DripStream] = dtfield(default_factory=set, repr=False)
    _lock: asyncio.Lock = dtfield(default_factory=asyncio.Lock, repr=False)
    
    def __post_init__(self, grip: GripKeyBase):
        self.grips = {grip}
    
    # Drip is allowed to have excatly one application key.
    @property
    def application_keys(self) -> set[GripKeyBase]:
        return self.grips
    
    @abstractmethod
    async def register_stream(self, client_stream: DripStream) -> None:
        """
        Register a stream for the Drip.
        """
        async with self._lock:
            self._client_streams.add(client_stream)


@datatree(eq=False)
class GripDripContextConnection(ABC):
    """
    A GripDripContextConnection is a connection between a Drip and a DripFeeder
    for a specific context's source Node.
    """
    
    @abstractmethod
    def get_drip_context_node(self) -> GripGraphContextNode:
        """
        Get the context for the connection.
        """
        pass
    
    @abstractmethod
    def send_to_drips(self, batch: DripBatch | DripMessage) -> None:
        """
        Send a batch of data or a single message to the Drips on destination
        context's Node.
        """
        pass


@datatree(eq=False)
class DripFeederConnection(ABC):
    """
    A DripFeederConnection is a connection occurs when a Drip connects to a DripFeeder
    to inform the connection handler of data from the source Node for the
    requested grips.
    """
    drip_context_connection: GripDripContextConnection
    
    @abstractmethod
    def get_drip_specific_feeds(self) -> set[GripKeyBase]:
        """
        This informs the Grok processor that when a Drip->DripFeeder connection
        is made, the given Grips from the context of the Drip should be fed to
        the connection handler.
        """
        pass
    
    @abstractmethod
    async def send_from_source_node(self, message: DripMessage | DripBatch) -> None:
        """
        Notifies the connection handler of data from the source Node for the 
        requested grips.
        """
        pass



@datatree(eq=False)
class GripTapConnectionHandler(ABC):
    """
    GripTapConnectionHandler is provided by the DripFeeder to receive connection
    specific information from the destination Drip's context Node.

    i.e. A Drip connects to DripFeeder and if the DripFeeder requires source
    based Drip connections, it will recieve a feed for that connection. If 
    the DripFeeder has more than one Drip connection they all share the same
    connection handler.
    """
    
    @abstractmethod
    async def on_connect(self, node: GripDripContextConnection) -> DripFeederConnection:
        """
        Called when the connection to the source Node is established.
        """
        pass

    

@datatree(eq=False)
class GripTapResources(ABC):
    """
    GripTapResources are the resources for a GripTap, this is specific to the
    GripTap instance and can be held by the GripTap instance for allocating 
    resources.
    """
    
    @abstractmethod
    async def register_connection_handler(self, handler: GripTapConnectionHandler) -> None:
        """
        Register a connection handler for the tap.
        """
        pass

    @abstractmethod
    def get_stream_processor(self) -> DripStreamProcessor:
        """
        Get the stream processor suggested for the tap. The tap is free to
        ignore this suggestion.
        """
        pass


@datatree(eq=False)
class GripTap(ABC):
    """
    GripConverterTap is a tap for a GripConverter.
    """
    
    @abstractmethod
    def input_grips(self) -> list[GripKeyBase]:
        """
        The input grips for the converter.
        """
        pass
    
    @abstractmethod
    def output_grips(self) -> list[GripKeyBase]:
        """
        The output grips for the converter.
        
        A DripFeeder for each of these grips will be created and passed
        provided to the initialize method.
        """
        pass

    @abstractmethod
    async def initialize(self, resources: GripTapResources) -> None:
        """
        Initialize the tap. Upon return, it should be ready to receive
        data via the send method.
        """
        pass
    
    @abstractmethod
    def get_drip_specific_feeds(self) -> set[GripKeyBase]:
        """
        This informs the Grok processor that when a Drip->DripFeeder connection
        is made, the given Grips from the context of the Drip should be fed to
        the connection handler.
        """
        pass
    
    @abstractmethod
    async def send(self, batch: DripBatch | DripMessage) -> None:
        """
        Send a batch of data or a single message to the tap.
        """
        pass

@datatree(eq=False)
class GripTapConstant(ABC):
    """
    A constant tap for a set of grips.
    
    This is a simple tap that can be used to return a constant value. There
    are no updates to the value, it is always the same. Drip that connect to
    this will receive one message at the time of connection is established.
    
    If the graph mutations cause the Drip to be disconnected and then
    reconnected, it will receive a new message with the same value. That
    message maybe elided if the stream is set the elide the same value.
    """
    
    @abstractmethod
    def get_input_grips(self) -> set[GripKeyBase]:
        """
        The set of grips that this tap will return a constant value for.
        """
        pass
    
    @abstractmethod
    def get_value(self, grips: set[GripKeyBase]) -> DripBatch:
        """
        Get the constant value for the tap.
        """
        pass
    
    
@datatree(eq=False)
class GripTapMatchStateManager(ABC):
    """
    Manages a specific match state for a Matcher.
    """
    
    @abstractmethod
    async def set_match_state(self, state: bool) -> None:
        """
        Matcher indicates the match state for the manager.
        True indicates a match, False indicates no match.
        """
        pass

    
@datatree(eq=False)
class GripTapMatcher(ABC):
    """
    GripTapMatcher is a matcher for a GripTap.
    """
    
    @abstractmethod
    def get_input_grips(self) -> set[GripKeyBase]:
        """
        The input grips required for the matcher.
        """
        pass
    
    @abstractmethod
    def set_match_state_manager(self, manager: 'GripTapMatchStateManager') -> None:
        """
        Set the match state stream for the matcher.
        """
        pass


@datatree(eq=False)
class GripTapMatcherFactory(ABC):
    """
    Matchers inputs to select the correct tap to use.
    """
    
    @abstractmethod
    def instantiate(self, context: 'GripQueryContext') -> GripTapMatcher:
        """
        Instantiate all matchers for the context.
        """
        pass


@datatree(eq=False)
class GripTapScope(ABC):
    """
    GripTapScope is a scope for a GripTap.
    """
    
    @abstractmethod
    def get_scope_matchers(self) -> Iterator[GripTapMatcherFactory]:
        """
        Get the matchers for the scope.
        """
        pass

@datatree(eq=False)
class GripContext(ABC):
    """
    GripContext is a wrapper around a subgraph of a GripGraph. The
    GripContext is essentially a view of the subgraph that can be
    and the producers available to the input nodes.
    """
    grip_registry: GripRegistry
    
    @abstractmethod
    def get_input_node(self) -> GripGraphContextNode:
        pass
    
    @abstractmethod
    def get_output_node(self) -> GripGraphContextNode:
        pass
    
    @abstractmethod
    def add_tap(self, tap: GripTap) -> None:
        """
        Add a tap to the context. This is enabled when added and remains 
        active until the context is removed.
        """
        pass


@datatree(eq=False)
class GripQueryContext(ABC, GripContext):
    """
    GripQueryContext is a context for a GripQuery.
    
    In addition to the GripContext's input and output nodes, a GripQueryContext
    also has a matcher node and a query node. The matcher node is the node that
    will be used to match the query and the query node is the node that will be
    used to query the context.
    
    TODO: Describe the subgraph for the query context.
    """
    
    def add_top_scope(self, scope: GripTapScope) -> None:
        """
        Add a top scope to the query context.
        """
        pass
    
    @abstractmethod
    def get_matcher_node(self) -> GripGraphContextNode:
        """
        The matcher node is the node that will be used to match the query.
        """
        pass
    
    @abstractmethod
    def get_query_input_node(self) -> GripGraphContextNode:
        """
        The query input node is the node that will be used to query the context.
        """
        pass


@datatree(eq=False)
class GripContextBuilder:
    """
    GripContextBuilder is a builder for GripContexts.
    """
    pass

@datatree(eq=False)
class GrokRuntimeBase:
    """
    GrokRuntime is the runtime for a Grok application.
    """
    grip_registry: GripRegistry
    
    @abstractmethod
    def log(self, 
            origin_key: GraphNodeBase, 
            msg: str = None, 
            exc: Exception = None,
            data: Any = None):
        pass


    @abstractmethod
    def root_context(self) -> GripContext:
        pass
    
    @abstractmethod
    def build_context(
        self, 
        builder: GripContextBuilder,
        parents: list[GripContext]
        ) -> GripContext:
        pass
    
    @abstractmethod
    async def send_message(
        self, 
        message: DripMessage | DripBatch, 
        key: ProducerKey | None = None, 
        connection: GripDripContextConnection | None = None
        ) -> None:
        """
        Send a message to consumers.
        If producer key is provided, the message will be sent to all the listeners of
        the producer key that have outstanding drips for the message grips.
        
        Alternatively, if a connection is provided, the message will be sent to the
        connection group/context for that specific connection.
        """
        pass
    
    @abstractmethod
    async def connect_groups(self, 
                             from_group: GripGraphContextNode, 
                             to_group: GripGraphContextNode) -> None:
        """
        This establishes a "depends" relationship between the two groups.
        
        When the from group is resolving for a grip producer, the to group
        will be searched for a producer that satisfies the request.
        
        Visa-versa, if a producer in the to group is informing there is a new
        producer for a grip, the from group will be searched for a consumer
        that satisfies the request.
        
        The number of connections is unlimited but the resulting graph must
        be acyclic.
        
        These conections are weak, meaning that they are not referenced by
        the (usually) containing context, the garbage collector will reap
        them and break the connections.
        """
        pass
    
    @abstractmethod
    async def connect_consumer(
        self, 
        consumer: GripDrip, 
        connection: GripGraphContextNode) -> None:
        """
        Establishes a connection between a consumer and a context node.
        
        This is used to inform the runtime that a consumer is interested in
        data from a specific context node.
        
        The GROK will select the producer for the context node send the most
        recent data to the consumer.

        Note that this will be weakly referenced and may be garbage collected
        unless the caller holds a reference to consumer.
        """
        pass
    
    
    @abstractmethod
    async def connect_producer(
        self, 
        producer: GripDripFeeder, 
        connection: GripGraphContextNode) -> None:
        """
        Establishes a connection between a producer and a context node.
        
        This is used to inform the runtime that a producer is interested in
        data from a specific context node.
        
        The GROK will select will establish a connection between the DripFeeder 
        for and all "visible" Drips.

        Note that this will be weakly referenced and may be garbage collected
        unless the caller holds a reference to consumer.
        """
        pass
