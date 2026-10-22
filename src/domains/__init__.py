"""
Domain packs and the registry that loads them.
"""

from pathlib import Path

from .pack import DomainPack, DomainPackError, load_pack

PACKS_DIR = Path(__file__).parent / "packs"


class DomainRegistry:
    """
    Every pack in a directory, keyed by its id. Files starting with "_" (templates) are skipped.
    """

    def __init__(self, packs: dict[str, DomainPack]):
        self._packs = packs

    @classmethod
    def from_directory(cls, directory: str | Path = PACKS_DIR) -> "DomainRegistry":
        packs = {}
        for path in sorted(Path(directory).glob("*.toml")):
            if path.name.startswith("_"):
                continue
            pack = load_pack(path)
            packs[pack.id] = pack
        return cls(packs)

    def replace_with(self, other: "DomainRegistry") -> None:
        """
        Adopt another registry's packs in place (hot reload; the registry object is shared).
        """
        self._packs = dict(other._packs)

    def get(self, domain_id: str) -> DomainPack:
        try:
            return self._packs[domain_id]
        except KeyError:
            raise KeyError(f"no domain pack '{domain_id}' (have: {', '.join(sorted(self._packs))})") from None

    def ids(self) -> list[str]:
        return sorted(self._packs)


__all__ = [
    "PACKS_DIR",
    "DomainPack",
    "DomainPackError",
    "DomainRegistry",
    "load_pack",
]
