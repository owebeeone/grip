from grip.grip_key import GripKey
from datatrees import datatree
from typing import Any

@datatree
class MatchItem:
    """
    A single item in the match criteria for a TAP.
    
    Matches if the values are equal - period.
    """
    key: GripKey
    value: Any
    weight: float=1.0
    
class MatchParam:
    """
    Parameters are for factory-like TAPs that first match on MatchItems and
    once these are found, use the parameters to create the Tap instance and
    become matched items upon tap creation.
    
    Matches on the TapFactory then Matches on the Tap instance.
    """
    key: GripKey
    weight: float=1.0

# Matcher protocol for determining if a TAP can satisfy a query
@datatree
class MatcherSpec:
    """
    Matcher specification for a Tap or TapFactory.
    
    To be satisfied, a TAP must match all matchers only for factories and
    all parameters must be at least available. Once it is matched, and a
    Tap is for the match to remain active, the Tap must match all parameters
    inluding any match items.
    
    A match is satisfied if it is the highest weight match. This means the 
    average weight of all the matching items, including paramet
    """
    matchers: dict[GripKey, MatchItem]
    parameters: dict[GripKey, MatchParam]


