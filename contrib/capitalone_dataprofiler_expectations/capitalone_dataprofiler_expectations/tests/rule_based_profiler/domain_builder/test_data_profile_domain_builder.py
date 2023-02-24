from typing import List

import pandas as pd
import pytest
from ruamel.yaml import YAML

import great_expectations.exceptions as gx_exceptions
from great_expectations import DataContext
from great_expectations.core.domain import (
    INFERRED_SEMANTIC_TYPE_KEY,
    Domain,
    SemanticDomainTypes,
)
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.rule_based_profiler.domain_builder import (
    ColumnDomainBuilder,
    ColumnPairDomainBuilder,
    DomainBuilder,
    MultiColumnDomainBuilder,
    TableDomainBuilder,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,
    build_parameter_container_for_variables,
)

yaml = YAML(typ="safe")

# noinspection PyPep8Naming


@pytest.mark.integration
@pytest.mark.slow  # 1.21s
def test_column_domain_builder(
    alice_columnar_table_single_batch_context,
    alice_columnar_table_single_batch,
):
    data_context: DataContext = alice_columnar_table_single_batch_context

    profiler_config: str = alice_columnar_table_single_batch["profiler_config"]

    full_profiler_config_dict: dict = yaml.load(profiler_config)

    variables_configs: dict = full_profiler_config_dict.get("variables")
    if variables_configs is None:
        variables_configs = {}

    variables: ParameterContainer = build_parameter_container_for_variables(
        variables_configs=variables_configs
    )

    batch_request: dict = {
        "datasource_name": "alice_columnar_table_single_batch_datasource",
        "data_connector_name": "alice_columnar_table_single_batch_data_connector",
        "data_asset_name": "alice_columnar_table_single_batch_data_asset",
    }

    domain_builder: DomainBuilder = ColumnDomainBuilder(
        data_context=data_context)
    domains: List[Domain] = domain_builder.get_domains(
        rule_name="my_rule", variables=variables, batch_request=batch_request
    )

    assert len(domains) == 7
    assert domains == [
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "id": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "event_type",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "event_type": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "user_id",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "user_id": SemanticDomainTypes.NUMERIC.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "event_ts",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "event_ts": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "server_ts",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "server_ts": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "device_ts",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "device_ts": SemanticDomainTypes.TEXT.value,
                },
            },
        },
        {
            "rule_name": "my_rule",
            "domain_type": MetricDomainTypes.COLUMN.value,
            "domain_kwargs": {
                "column": "user_agent",
            },
            "details": {
                INFERRED_SEMANTIC_TYPE_KEY: {
                    "user_agent": SemanticDomainTypes.TEXT.value,
                },
            },
        },
    ]
