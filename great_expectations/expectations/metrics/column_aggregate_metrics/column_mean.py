from great_expectations.execution_engine import (
    PandasExecutionEngine,
    SparkDFExecutionEngine,
    SqlAlchemyExecutionEngine,
)
from great_expectations.expectations.metrics.column_aggregate_metric_provider import (
    ColumnAggregateMetricProvider,
    column_aggregate_partial,
    column_aggregate_value,
)
from great_expectations.optional_imports import F
from great_expectations.optional_imports import sqlalchemy as sa


class ColumnMean(ColumnAggregateMetricProvider):
    """MetricProvider Class for Aggregate Mean MetricProvider"""

    metric_name = "column.mean"

    @column_aggregate_value(engine=PandasExecutionEngine)
    def _pandas(cls, column, **kwargs):
        """Pandas Mean Implementation"""
        return column.mean()

    @column_aggregate_partial(engine=SqlAlchemyExecutionEngine)
    def _sqlalchemy(cls, column, **kwargs):
        """SqlAlchemy Mean Implementation"""
        # column * 1.0 needed for correct calculation of avg in MSSQL
        return sa.func.avg(1.0 * column)

    @column_aggregate_partial(engine=SparkDFExecutionEngine)
    def _spark(cls, column, _table, _column_name, **kwargs):
        """Spark Mean Implementation"""
        types = dict(_table.dtypes)
        if types[_column_name] not in ("int", "float", "double", "bigint"):
            raise TypeError("Expected numeric column type for function mean()")
        return F.mean(column)
