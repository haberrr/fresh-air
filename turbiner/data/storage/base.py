from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Tuple, List
from dataclasses import dataclass


@dataclass
class SchemaField:
    name: str
    field_type: str
    default: Optional[Any] = None
    description: Optional[str] = None
    mode: Optional[str] = None


class Resource(ABC):
    """
    Base abstract class for a Resource object that reflects the data somewhere in the storage.
    """

    @abstractmethod
    def __init__(
            self,
            path: Tuple[str, ...] | str,
            data: Optional[List[Dict[str, Any]]] = None,
            schema: Optional[List[SchemaField]] = None,
            **kwargs,
    ):
        pass

    @abstractmethod
    def write(self, data: Optional[List[Dict[str, Any]]] = None, append: bool = True, **kwargs) -> None:
        pass

    @abstractmethod
    def read(self, **kwargs) -> List[Dict[str, Any]]:
        pass
