from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, Tuple, List, Union, Sequence
from dataclasses import dataclass


@dataclass
class SchemaField:
    name: str
    field_type: Union[str, type]
    default: Optional[Any] = None
    description: Optional[str] = None
    mode: Optional[str] = None
    fields: Optional[Sequence['SchemaField']] = None


class Resource(ABC):
    """
    Base abstract class for a Resource object that reflects the data somewhere in the storage.
    """

    @abstractmethod
    def __init__(
            self,
            path: Tuple[str, ...] | str,
            project_id: Optional[str] = None,
            schema: Optional[List[SchemaField]] = None,
            **kwargs,
    ):
        pass

    @abstractmethod
    def write(self, data: List[Dict[str, Any]], append: bool = True, **kwargs) -> None:
        pass

    @abstractmethod
    def read(self, **kwargs) -> List[Dict[str, Any]]:
        pass
