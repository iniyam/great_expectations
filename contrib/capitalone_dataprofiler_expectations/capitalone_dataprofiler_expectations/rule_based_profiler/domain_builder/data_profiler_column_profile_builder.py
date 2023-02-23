from __future__ import annotations

from typing import (
    TYPE_CHECKING,
    ClassVar,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Union,
    cast,
)



import great_expectations.exceptions as gx_exceptions
from great_expectations.core.domain import Domain, SemanticDomainTypes
from great_expectations.core.metric_domain_types import MetricDomainTypes
from great_expectations.data_context.util import instantiate_class_from_config
from great_expectations.rule_based_profiler.domain_builder import DomainBuilder
from great_expectations.rule_based_profiler.helpers.util import (
    build_domains_from_column_names,
    get_parameter_value_and_validate_return_type,
)
from great_expectations.rule_based_profiler.parameter_container import (
    ParameterContainer,  # noqa: TCH001
)
from great_expectations.rule_based_profiler.semantic_type_filter import (
    SemanticTypeFilter,  # noqa: TCH001
)
from great_expectations.validator.metric_configuration import MetricConfiguration

if TYPE_CHECKING:
    from great_expectations.data_context.data_context.abstract_data_context import (
        AbstractDataContext,
    )
    from great_expectations.validator.validator import Validator


# ===================================================
# Imports From Column_domain_builder

#minimize import later
import DataProfiler as dp

from great_expectations.rule_based_profiler.domain_builder import ColumnDomainBuilder

class DataProfilerColumnDomainBuilder(ColumnDomainBuilder):
    

    # ===================================================
    # Task 1: Override the _get_domains() function so that when it calls self.get_effective_column_names() you also pass in rule_name .
    def _get_domains(
        self,
        rule_name: str,
        variables: Optional[ParameterContainer] = None,
        runtime_configuration: Optional[dict] = None,
    ) -> List[Domain]:
        """
        Obtains and returns domains for all columns of a table (or for configured columns, if they exist in the table).

        Args:
            rule_name: name of Rule object, for which "Domain" objects are obtained.
            variables: Optional variables to substitute when evaluating.
            runtime_configuration: Optional[dict] = None,

        Returns:
            List of domains that match the desired columns and filtering criteria.
        """
        batch_ids: List[str] = self.get_batch_ids(variables=variables)  # type: ignore[assignment] # could be None

        validator: Validator = self.get_validator(variables=variables)  # type: ignore[assignment] # could be None

        effective_column_names: List[str] = self.get_effective_column_names(
            batch_ids=batch_ids,
            validator=validator,
            variables=variables,
            rule_name=rule_name
        )

        column_name: str
        domains: List[Domain] = build_domains_from_column_names(
            rule_name=rule_name,
            column_names=effective_column_names,
            domain_type=self.domain_type,
            table_column_name_to_inferred_semantic_domain_type_map=self.semantic_type_filter.table_column_name_to_inferred_semantic_domain_type_map,  # type: ignore[union-attr] # could be None
        )

        return domains


    # ===================================================
    # Task 2: Overload the get_effective_column_names() to accept the rule_name: Str parameter.
    def get_effective_column_names(
        self,
        batch_ids: Optional[List[str]] = None,
        validator: Optional[Validator] = None,
        variables: Optional[ParameterContainer] = None,
        rule_name: Optional[str] = None
    ) -> List[str]:
        """
        This method applies multiple directives to obtain columns to be included as part of returned "Domain" objects.
        """

        # new stuff
        # lower everything such as NUMERIC
        profile_path:str = variables["profile_path"]

        profile = dp.Profiler.load(profile_path)

        report = profile.report(report_options={"output_format":"compact"})

        effective_column_names = list()

        rule_name_to_data_type = {"NUMERIC": {"int", "float"}, "TIMESTAMP": {"datetime"}, "STRING": {"string"}}

        data_type_from_rule = rule_name_to_data_type[rule_name]

        for col in report["data_stats"]:
            if col["data_type"] in data_type_from_rule:
                effective_column_names.append(col["column_name"])

        # /new stuffeffective_column_names
        return effective_column_names
    


