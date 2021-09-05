from typing import Dict


class BaseRow(object):
    """
    Base class for Row serialization of database query results.
    Inherited by all row classes and ensures that serialize() is implemented.
    """

    def serialize(self) -> Dict:
        raise NotImplementedError("Row model needs to define a serialize function")
