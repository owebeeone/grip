
from logging import Logger
from typing import Any
from grip.grip_dgraph import DGraph, DGraphGroup, DGraphNodeKey
from grip.grip_key import GripRegistry, GripKey
from grip.grip_context import GripContext, GripContextBuilder
from datatrees import datatree, dtfield


@datatree
class GrokRuntime(GrokRuntimeBase):
    """
    GrokRuntime is the runtime for a Grok application.
    """
    grip_registry: GripRegistry 
    
    graph_group: DGraphGroup = dtfield(default_factory=DGraphGroup)
    default_graph: DGraph = dtfield(self_default=lambda self: DGraph(self.graph_group))
    _root_context: GripContext = dtfield(init=False)
    
    def __post_init__(self):
        self.root_context = GripContext(
            grip_registry=self.grip_registry,
            grok_runtime=self,
            keys_requested=None,
            parent=None,
            values={}
        )
        self.default_graph.add_node(self.root_context)
    
    
    def log(self, 
            origin_key: DGraphNodeKey, 
            msg: str = None, 
            exc: Exception = None,
            data: Any = None):
        """
        Log a message or an exception.
        """
        if msg is None and exc is None:
            raise ValueError("At least one of msg or exc must be provided")
        
        # TODO: Implement logging
        assert False
        
    
    
    
    def root_context(self) -> GripContext:
        """
        The root context is the context for the entire application.
        """
        return self._root_context
    
    def build_context(
        self, 
        builder: GripContextBuilder,
        parents: list[GripContext]
        ) -> GripContext:
    
    
