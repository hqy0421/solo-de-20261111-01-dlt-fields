from typing import Dict, Literal, Optional

from dlt.common.schema.typing import TColumnName, TypedDict


class RelationalNormalizerConfigPropagation(TypedDict, total=False):
    root: Optional[Dict[TColumnName, TColumnName]]
    tables: Optional[Dict[str, Dict[TColumnName, TColumnName]]]


TColumnMappingConflictMode = Literal["merge", "disambiguate", "reject"]
"""How to resolve different source paths that normalize into the same column name"""


class RelationalNormalizerConfig(TypedDict, total=False):
    max_nesting: Optional[int]
    propagation: Optional[RelationalNormalizerConfigPropagation]
    root_key_propagation: Optional[bool]
    column_mapping_conflict: Optional[TColumnMappingConflictMode]
