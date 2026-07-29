"""Type stubs for the private native extension module."""

from typing import List, Optional, Tuple

__version__: str

class VortexRdfError(Exception):
    """Raised when a Vortex-RDF store operation fails."""

class TermDict:
    def decode(self, code: int) -> Optional[str]: ...
    def __len__(self) -> int: ...

class U32Column:
    """Read-only u32 column; supports the buffer protocol
    (``memoryview(col).cast("I")`` is a zero-copy view)."""

    def __len__(self) -> int: ...
    def __buffer__(self, flags: int, /) -> memoryview: ...

class VortexRdfStore:
    def __init__(
        self,
        path: str,
        max_resident_terms: Optional[int] = None,
        in_memory: bool = False,
    ) -> None: ...
    def layout(self) -> str: ...
    def __len__(self) -> int: ...
    def term_dict(self) -> Optional[TermDict]: ...
    def match_codes(
        self,
        s: Optional[str] = None,
        p: Optional[str] = None,
        o: Optional[str] = None,
        g: Optional[str] = None,
    ) -> Optional[Tuple[U32Column, U32Column, U32Column, U32Column]]: ...
    def match_triples(
        self,
        s: Optional[str] = None,
        p: Optional[str] = None,
        o: Optional[str] = None,
        g: Optional[str] = None,
    ) -> List[Tuple[str, str, str]]: ...
    def match_compact(
        self,
        s: Optional[str] = None,
        p: Optional[str] = None,
        o: Optional[str] = None,
        g: Optional[str] = None,
    ) -> Tuple[List[str], List[Tuple[int, int, int]]]: ...

def serialize_rdf(
    input_path: str,
    output_path: str,
    layout: str = "default",
    dictionary_placement: str = "padded",
    format: Optional[str] = None,
    builder: str = "unsorted-stream",
) -> None: ...
