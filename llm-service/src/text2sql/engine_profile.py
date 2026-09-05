from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .compiler import resolve_dialect


DialectFamily = Literal["mysql", "postgres", "other"]


@dataclass(frozen=True)
class EngineProfile:
    requested: str
    sqlglot: str
    family: DialectFamily
    identifier_quote: str
    supports_schemas: bool

    @property
    def is_mysql(self) -> bool:
        return self.family == "mysql"

    @property
    def is_postgres(self) -> bool:
        return self.family == "postgres"


def get_engine_profile(dialect: str) -> EngineProfile:
    d = (dialect or "").strip()
    sg = resolve_dialect(d or "postgresql")
    if sg == "mysql":
        return EngineProfile(
            requested=d or "mysql",
            sqlglot=sg,
            family="mysql",
            identifier_quote="`",
            supports_schemas=True,
        )
    if sg == "postgres":
        return EngineProfile(
            requested=d or "postgresql",
            sqlglot=sg,
            family="postgres",
            identifier_quote='"',
            supports_schemas=True,
        )
    return EngineProfile(
        requested=d or sg,
        sqlglot=sg,
        family="other",
        identifier_quote='"',
        supports_schemas=True,
    )

