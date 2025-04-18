from dataclasses import InitVar, dataclass, field
from typing import Any, Optional
from grip.grip_dgraph import ApplicationKeyBase
from grip.grip_graph import ProducerKey, ConsumerKey, QueryKey
from grip.grip_grok import GrokRuntime
from grip.grip_key import GripKey
from grip.grip_stream import GripStream



class GripContext: pass



@dataclass
class GripDrip(ConsumerKey):
    """
    A GripDrip is a controlled drip.
    """
    
    grip: InitVar[GripKey]
    grips: set[GripKey] = field(init=False)
    grok: GrokRuntime
    context: GripContext
    streams: set[GripStream]
    
    def __post_init__(self, grip: GripKey):
        self.grips = {grip}
    
    def application_keys(self) -> GripKey:
        """Returns the GripKey for this Drip."""
        return self.grip
    
    def send(self, message: DripMessage):
        """
        Send a message to all Drip clients.
        """
        for stream in self.streams:
            try:
                stream.send(message)
            except Exception as e:
                self.grok.log(
                    self.grip, 
                    f"Error sending message to stream: {e}", 
                    e, 
                    message)
    
    
class Connection:
    """
    A Connection is a connection to a GripDripFeeder.
    """
    
    
    
class GripDripFeeder(ProducerKey):
    """
    A GripDripFeeder is a feeder for a GripDrip.
    """
    
    grip: GripKey
    grok: GrokRuntime
    context: GripContext
    streams: set[GripStream]
    
    def application_key(self) -> GripKey:
        """Returns the GripKey for this DripFeeder."""
        return self.grip
    
    
    
    
