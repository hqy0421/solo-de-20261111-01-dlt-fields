import os
from typing import List

import pytest

from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer
from dlt.common.schema import Schema
from dlt.common.schema.exceptions import ColumnMappingConflictException
from dlt.common.schema.typing import TSchemaUpdate
from dlt.common.schema.utils import new_column, new_table
from dlt.common.storages import NormalizeStorage
from dlt.common.storages.configuration import NormalizeStorageConfiguration
from dlt.common.typing import StrAny

from dlt.normalize import Normalize
from dlt.normalize.exceptions import NormalizeJobFailed
from dlt.normalize.items_normalizers import JsonLItemsNormalizer
from dlt.normalize.validate import validate_and_update_schema

from tests.utils import get_test_storage_root

# reuse fixtures and helpers from the main normalize test module
from tests.normalize.test_normalize import (  # noqa: F401
    default_caps,
    extract_items,
    load_or_create_schema,
    logger_autouse,
    normalize_pending,
    raw_normalize,
)

pytestmark = pytest.mark.serial


def _make_item_normalizer(schema: Schema) -> JsonLItemsNormalizer:
    n = Normalize()
    normalize_storage = NormalizeStorage(
        True,
        NormalizeStorageConfiguration(
            os.path.join(get_test_storage_root(), "pipeline", "normalized")
        ),
    )
    return JsonLItemsNormalizer(None, None, normalize_storage, schema, "load_id", n.config)


def _normalize_chunk(schema: Schema, items: List[StrAny]) -> TSchemaUpdate:
    return _make_item_normalizer(schema)._normalize_chunk(
        root_table_name="items",
        items=items,
        may_have_pua=False,
        skip_write=True,
    )


def test_normalize_chunk_schema_update_carries_mapping() -> None:
    schema = Schema("mapped")
    schema_update = _normalize_chunk(schema, [{"field": 1, "nested": {"inner": 2}}])
    assert schema_update["items"][0]["x-column-mapping"] == {
        '["field"]': "field",
        '["nested","inner"]': "nested__inner",
    }


def test_normalize_persists_column_mapping(raw_normalize: Normalize) -> None:
    schema = Schema("mapped")
    extract_items(
        raw_normalize.normalize_storage,
        [{"field": 1, "nested": {"inner": 2}}],
        schema,
        "items",
    )
    normalize_pending(raw_normalize, schema)
    expected = {'["field"]': "field", '["nested","inner"]': "nested__inner"}
    assert schema.get_column_mapping("items") == expected
    # the mapping is persisted together with the schema
    stored = raw_normalize.schema_storage.load_schema("mapped").get_column_mapping("items")
    assert stored == expected


def test_column_mapping_stable_across_loads(raw_normalize: Normalize) -> None:
    schema = Schema("mapped")
    RelationalNormalizer.update_normalizer_config(
        schema, {"column_mapping_conflict": "disambiguate"}
    )
    extract_items(raw_normalize.normalize_storage, [{"a__b": 1, "a": {"b": 2}}], schema, "items")
    normalize_pending(raw_normalize, schema)
    first_mapping = schema.get_column_mapping("items")
    disambiguated = first_mapping['["a","b"]']
    assert disambiguated != "a__b"

    # new run with the schema reloaded from storage and fields in different order
    schema2 = load_or_create_schema(raw_normalize, "mapped")
    extract_items(raw_normalize.normalize_storage, [{"a": {"b": 5}, "a__b": 4}], schema2, "items")
    normalize_pending(raw_normalize, schema2)
    assert schema2.get_column_mapping("items") == first_mapping
    assert {"a__b", disambiguated} <= set(
        schema2.get_table_columns("items", include_incomplete=True)
    )


def test_column_mapping_adopts_preexisting_columns(raw_normalize: Normalize) -> None:
    # table with columns but no mapping, as stored by dlt versions without lineage support
    schema = Schema("mapped")
    RelationalNormalizer.update_normalizer_config(
        schema, {"column_mapping_conflict": "disambiguate"}
    )
    schema.update_table(
        new_table(
            "items",
            write_disposition="append",
            columns=[new_column("a__b", "bigint", nullable=True)],
        )
    )
    extract_items(raw_normalize.normalize_storage, [{"a__b": 1}, {"a": {"b": 2}}], schema, "items")
    normalize_pending(raw_normalize, schema)
    mapping = schema.get_column_mapping("items")
    assert mapping['["a__b"]'] == "a__b"
    disambiguated = mapping['["a","b"]']
    assert disambiguated != "a__b"
    # the pre-existing column keeps its name, the colliding path gets a new column
    assert {"a__b", disambiguated} <= set(
        schema.get_table_columns("items", include_incomplete=True)
    )


def test_column_mapping_reject_fails_normalize(raw_normalize: Normalize) -> None:
    schema = Schema("mapped")
    RelationalNormalizer.update_normalizer_config(schema, {"column_mapping_conflict": "reject"})
    extract_items(raw_normalize.normalize_storage, [{"a__b": 1, "a": {"b": 2}}], schema, "items")
    with pytest.raises(NormalizeJobFailed) as exc_info:
        normalize_pending(raw_normalize, schema)
    assert isinstance(exc_info.value.__cause__, ColumnMappingConflictException)


def test_parallel_mapping_conflict_retries_to_convergence() -> None:
    schema = Schema("mapped")
    RelationalNormalizer.update_normalizer_config(
        schema, {"column_mapping_conflict": "disambiguate"}
    )

    # two workers starting from the same schema, each seeing only one of the colliding paths
    update_a = _normalize_chunk(schema.clone(), [{"a__b": 1}])
    update_b = _normalize_chunk(schema.clone(), [{"a": {"b": 2}}])

    validate_and_update_schema(schema, [update_a])
    # worker B's natural name now collides with a mapping from worker A
    with pytest.raises(ColumnMappingConflictException):
        validate_and_update_schema(schema, [update_b])

    # retrying B with the merged schema deterministically disambiguates its path
    update_b_retry = _normalize_chunk(schema.clone(), [{"a": {"b": 2}}])
    validate_and_update_schema(schema, [update_b_retry])
    mapping = schema.get_column_mapping("items")
    assert mapping['["a__b"]'] == "a__b"
    disambiguated = mapping['["a","b"]']
    assert disambiguated != "a__b"

    # a further run on the converged schema produces no updates at all
    assert _normalize_chunk(schema.clone(), [{"a": {"b": 2}}]) == {}
    # and still disambiguates deterministically on a schema reloaded from storage
    reloaded = Schema.from_dict(schema.to_dict())
    assert _normalize_chunk(reloaded, [{"a": {"b": 2}}]) == {}
    assert reloaded.get_column_mapping("items") == mapping
