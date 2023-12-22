#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

from unittest.mock import ANY, MagicMock, Mock, call, patch
import uuid
import pytest
import urllib3
from airbyte_cdk.models import ConfiguredAirbyteCatalog
from destination_astra.config import AstraIndexingModel
from destination_astra.indexer import AstraIndexer
from destination_astra.astra_client import AstraClient


def create_astra_indexer():
    config = AstraIndexingModel(astra_db_id="", astra_db_region="", astra_db_keyspace="", collection="")
    indexer = AstraIndexer(config, 3)

    indexer.client.delete_documents = MagicMock()
    indexer.client.insert_documents = MagicMock()
    indexer.client.find_documents = MagicMock()
    return indexer


def create_index_description(dimensions=3):
    return {"name": "", "options": {"vector": {"dimension": dimensions, "metric": "cosine"}}}


@pytest.fixture(scope="module", autouse=True)
def mock_describe_index():
    with patch("astra.describe_index") as mock:
        mock.return_value = create_index_description()
        yield mock


def test_astra_index_upsert_and_delete(mock_describe_index):
    indexer = create_astra_indexer()
    indexer.index(
        [
            Mock(page_content="test", metadata={"_ab_stream": "abc"}, embedding=[1, 2, 3]),
            Mock(page_content="test2", metadata={"_ab_stream": "abc"}, embedding=[4, 5, 6]),
        ],
        "ns1",
        "some_stream",
    )
    indexer.delete(["delete_id1", "delete_id2"], "ns1", "some_stram")
    indexer.client.delete_documents.assert_called_with(
        collection_name="", filter={"_ab_record_id": {"$in": ["delete_id1", "delete_id2"]}}, namespace="ns1"
    )
    indexer.client.insert_documents.assert_called_with(
        collection_name="",
        documents=[
            {"_id": str(uuid.uuid4()), "$vector": [1, 2, 3], "_ab_stream": "abc", "text": "test"},
            {"_id": str(uuid.uuid4()), "$vector": [4, 5, 6], "_ab_stream": "abc", "text": "test2"},
        ],
    )


def test_astra_index_empty_batch():
    indexer = create_astra_indexer()
    indexer.index([], "ns1", "some_stream")
    indexer.client.delete_documents.assert_not_called()
    indexer.client.insert_documents.assert_not_called()


def test_astra_index_upsert_batching():
    indexer = create_astra_indexer()
    indexer.index(
        [Mock(page_content=f"test {i}", metadata={"_ab_stream": "abc"}, embedding=[i, i, i]) for i in range(50)],
        "ns1",
        "some_stream",
    )
    assert indexer.client.insert_documents.call_count == 2
    for i in range(40):
        assert indexer.client.insert_documents.call_args_list[0].kwargs["vectors"][i] == (
            ANY,
            [i, i, i],
            {"_ab_stream": "abc", "text": f"test {i}"},
        )
    for i in range(40, 50):
        assert indexer.client.insert_documents.call_args_list[1].kwargs["vectors"][i - 40] == (
            ANY,
            [i, i, i],
            {"_ab_stream": "abc", "text": f"test {i}"},
        )


def generate_catalog():
    return ConfiguredAirbyteCatalog.parse_obj(
        {
            "streams": [
                {
                    "stream": {
                        "name": "example_stream",
                        "json_schema": {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": {}},
                        "supported_sync_modes": ["full_refresh", "incremental"],
                        "source_defined_cursor": False,
                        "default_cursor_field": ["column_name"],
                        "namespace": "ns1",
                    },
                    "primary_key": [["_id"]],
                    "sync_mode": "incremental",
                    "destination_sync_mode": "append_dedup",
                },
                {
                    "stream": {
                        "name": "example_stream2",
                        "json_schema": {"$schema": "http://json-schema.org/draft-07/schema#", "type": "object", "properties": {}},
                        "supported_sync_modes": ["full_refresh", "incremental"],
                        "source_defined_cursor": False,
                        "default_cursor_field": ["column_name"],
                        "namespace": "ns2",
                    },
                    "primary_key": [["_id"]],
                    "sync_mode": "full_refresh",
                    "destination_sync_mode": "overwrite",
                },
            ]
        }
    )


def test_astra_pre_sync(mock_describe_index):
    indexer = create_astra_indexer()
    indexer.pre_sync(generate_catalog())
    indexer.client.delete_documents.assert_called_with(filter={"_ab_stream": "ns2_example_stream2"}, namespace="ns2")


@pytest.mark.parametrize(
    "index_list, describe_throws,reported_dimensions,check_succeeds, error_message",
    [
        (["myindex"], None, 3, True, None),
        (["other_index"], None, 3, False, "Index myindex does not exist in environment"),
        (
            ["myindex"],
            urllib3.exceptions.MaxRetryError(None, "", reason=Exception("Failed to resolve 'apps.astra.datastax.com'")),
            3,
            False,
            "Failed to resolve environment",
        ),
        (["myindex"], None, 4, False, "Make sure embedding and indexing configurations match."),
        (["myindex"], Exception("describe failed"), 3, False, "describe failed"),
        (["myindex"], Exception("describe failed"), 4, False, "describe failed"),
    ],
)
@patch("astra.describe_index")
@patch("astra.list_indexes")
def test_astra_check(list_mock, describe_mock, index_list, describe_throws, reported_dimensions, check_succeeds, error_message):
    indexer = create_astra_indexer()
    indexer.embedding_dimensions = 3
    if describe_throws:
        describe_mock.side_effect = describe_throws
    else:
        describe_mock.return_value = create_index_description(dimensions=reported_dimensions)
    list_mock.return_value = index_list
    result = indexer.check()
    if check_succeeds:
        assert result is None
    else:
        assert error_message in result
