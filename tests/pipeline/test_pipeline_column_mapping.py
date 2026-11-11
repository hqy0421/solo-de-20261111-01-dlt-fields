import dlt
from dlt.common.schema import Schema
from dlt.common.normalizers.json.relational import DataItemNormalizer as RelationalNormalizer
from dlt.common.utils import uniq_id


def test_pipeline_column_mapping_persists_across_runs() -> None:
    pipeline_name = "pipe_column_mapping_" + uniq_id()
    dataset_name = "column_mapping_" + uniq_id()
    schema = Schema("mapped")
    RelationalNormalizer.update_normalizer_config(
        schema, {"column_mapping_conflict": "disambiguate"}
    )

    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",
        dataset_name=dataset_name,
    )
    pipeline.run([{"a__b": 1, "a": {"b": 2}}], table_name="items", schema=schema)
    mapping = pipeline.default_schema.get_column_mapping("items")
    assert mapping['["a__b"]'] == "a__b"
    disambiguated = mapping['["a","b"]']
    assert disambiguated != "a__b"

    with pipeline.sql_client() as client:
        row = client.execute_sql(f'SELECT "a__b", "{disambiguated}" FROM items')[0]
    assert row == (1, 2)

    # a new pipeline instance loads the mapping from storage and keeps the column names
    pipeline2 = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination="duckdb",
        dataset_name=dataset_name,
    )
    pipeline2.run([{"a": {"b": 5}, "a__b": 4}], table_name="items")
    assert pipeline2.default_schema.get_column_mapping("items") == mapping
    with pipeline2.sql_client() as client:
        rows = client.execute_sql(
            f'SELECT "a__b", "{disambiguated}" FROM items ORDER BY 1'
        )
    assert rows == [(1, 2), (4, 5)]
