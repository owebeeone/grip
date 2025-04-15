
# Base exception for GRIP-related errors
class GripBaseException(Exception):
    """Base exception for GRIP-related errors."""
    pass

class DuplicateGripKey(GripBaseException):
    """Exception raised when a GripKey is defined more than once."""
    pass
