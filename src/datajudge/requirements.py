from abc import ABC
from collections.abc import MutableSequence
from typing import (
    Callable,
    Collection,
    Dict,
    List,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
)

import sqlalchemy as sa

from .constraints import column as column_constraints
from .constraints import date as date_constraints
from .constraints import groupby as groupby_constraints
from .constraints import miscs as miscs_constraints
from .constraints import nrows as nrows_constraints
from .constraints import numeric as numeric_constraints
from .constraints import row as row_constraints
from .constraints import stats as stats_constraints
from .constraints import uniques as uniques_constraints
from .constraints import varchar as varchar_constraints
from .constraints.base import Constraint, TestResult
from .db_access import (
    Condition,
    DataReference,
    DataSource,
    ExpressionDataSource,
    RawQueryDataSource,
    TableDataSource,
    get_date_growth_rate,
)
from .utils import OutputProcessor, output_processor_limit

T = TypeVar("T")


class TableQualifier:
    def __init__(self, db_name: str, schema_name: str, table_name: str):
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name

    def get_within_requirement(self):
        return WithinRequirement.from_table(
            db_name=self.db_name,
            schema_name=self.schema_name,
            table_name=self.table_name,
        )

    def get_between_requirement(self, table_qualifier):
        return BetweenRequirement.from_tables(
            db_name1=self.db_name,
            schema_name1=self.schema_name,
            table_name1=self.table_name,
            db_name2=table_qualifier.db_name,
            schema_name2=table_qualifier.schema_name,
            table_name2=table_qualifier.table_name,
        )


class Requirement(ABC, MutableSequence):
    def __init__(self):
        self._constraints: List[Constraint] = []
        self.data_source: DataSource

    def insert(self, index: int, value: Constraint) -> None:
        self._constraints.insert(index, value)

    def __getitem__(self, i):
        return self._constraints[i]

    def __setitem__(self, i, o) -> None:
        self._constraints[i] = o

    def __delitem__(self, i) -> None:
        del self._constraints[i]

    def __len__(self) -> int:
        return len(self._constraints)

    def test(self, engine) -> List[TestResult]:
        return [constraint.test(engine) for constraint in self]


class WithinRequirement(Requirement):
    def __init__(self, data_source: DataSource):
        self.data_source = data_source
        super().__init__()

    @classmethod
    def from_table(cls, db_name: str, schema_name: str, table_name: str):
        return cls(
            data_source=TableDataSource(
                db_name=db_name, schema_name=schema_name, table_name=table_name
            )
        )

    @classmethod
    def from_raw_query(cls, query: str, name: str, columns: Optional[List[str]] = None):
        """Create a ``WithinRequirement`` based on a raw query string.

        The ``query`` parameter can be passed any query string returning rows, e.g.
        ``"SELECT * FROM myschema.mytable LIMIT 1337"`` or
        ``"SELECT id, name FROM table1 UNION SELECT id, name FROM table2"``.

        The ``name`` will be used to represent this query in error messages.

        If constraints rely on specific columns, these should be provided here via
        ``columns``, e.g. ``["id", "name"]``.
        """
        return cls(data_source=RawQueryDataSource(query, name, columns=columns))

    @classmethod
    def from_expression(cls, expression: sa.sql.expression.FromClause, name: str):
        """Create a ``WithinRequirement`` based on a sqlalchemy expression.

        Any sqlalchemy object implementing the ``alias`` method can be passed as an
        argument for the ``expression`` parameter. This could, e.g. be an
        ``sqlalchemy.Table`` object or the result of a ``sqlalchemy.select`` call.

        The ``name`` will be used to represent this expression in error messages.
        """
        return cls(data_source=ExpressionDataSource(expression, name))

    def add_column_existence_constraint(
        self, columns: List[str], name: Optional[str] = None, cache_size=None
    ):
        # Note that columns are not meant to be part of the reference.
        ref = DataReference(self.data_source)
        self._constraints.append(
            column_constraints.ColumnExistence(ref, columns, cache_size=cache_size)
        )

    def add_primary_key_definition_constraint(
        self,
        primary_keys: List[str],
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Check that the primary key constraints in the database are exactly equal to the given column names.

        Note that this doesn't actually check that the primary key values are unique across the table.
        """
        ref = DataReference(self.data_source)
        self._constraints.append(
            miscs_constraints.PrimaryKeyDefinition(
                ref, primary_keys, name=name, cache_size=cache_size
            )
        )

    def add_uniqueness_constraint(
        self,
        columns: Optional[List[str]] = None,
        max_duplicate_fraction: float = 0,
        condition: Optional[Condition] = None,
        max_absolute_n_duplicates: int = 0,
        infer_pk_columns: bool = False,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Columns should uniquely identify row.

        Given a set of columns, satisfy conditions of a primary key, i.e.
        uniqueness of tuples from said columns. This constraint has a tolerance
        for inconsistencies, expressed via max_duplicate_fraction. The latter
        suggests that the number of uniques from said columns is larger or equal
        to (1 - max_duplicate_fraction) the number of rows.

        If infer_pk_columns is True, columns will be retrieved from the primary keys.
        When columns=None and infer_pk_columns=False, the fallback is validating that all
        rows in a table are unique.
        """
        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            miscs_constraints.Uniqueness(
                ref,
                max_duplicate_fraction=max_duplicate_fraction,
                max_absolute_n_duplicates=max_absolute_n_duplicates,
                infer_pk_columns=infer_pk_columns,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_column_type_constraint(
        self,
        column: str,
        column_type: Union[str, sa.types.TypeEngine],
        name: Optional[str] = None,
        cache_size=None,
    ):
        """
        Check if a column type matches the expected column_type.

        The column_type can be provided as a string (backend-specific type name), a backend-specific SQLAlchemy type, or a SQLAlchemy's generic type.

        If SQLAlchemy's generic types are used, the check is performed using `isinstance`, which means that the actual type can also be a subclass of the target type.
        For more information on SQLAlchemy's generic types, see https://docs.sqlalchemy.org/en/20/core/type_basics.html

        Parameters
        ----------
        column : str
            The name of the column to which the constraint will be applied.

        column_type : Union[str, sa.types.TypeEngine]
            The expected type of the column. This can be a string, a backend-specific SQLAlchemy type, or a generic SQLAlchemy type.

        name : Optional[str]
            An optional name for the constraint. If not provided, a name will be generated automatically.
        """
        ref = DataReference(self.data_source, [column])
        self._constraints.append(
            column_constraints.ColumnType(
                ref,
                column_type=column_type,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_null_absence_constraint(
        self,
        column: str,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            miscs_constraints.MaxNullFraction(
                ref, max_null_fraction=0, name=name, cache_size=cache_size
            )
        )

    def add_max_null_fraction_constraint(
        self,
        column: str,
        max_null_fraction: float,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Assert that ``column`` has less than a certain fraction of ``NULL`` values.

        ``max_null_fraction`` is expected to lie within [0, 1].
        """
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            miscs_constraints.MaxNullFraction(
                ref,
                max_null_fraction=max_null_fraction,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_n_rows_equality_constraint(
        self,
        n_rows: int,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, None, condition)
        self._constraints.append(
            nrows_constraints.NRowsEquality(
                ref, n_rows=n_rows, name=name, cache_size=cache_size
            )
        )

    def add_n_rows_min_constraint(
        self,
        n_rows_min: int,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, None, condition)
        self._constraints.append(
            nrows_constraints.NRowsMin(
                ref, n_rows=n_rows_min, name=name, cache_size=cache_size
            )
        )

    def add_n_rows_max_constraint(
        self,
        n_rows_max: int,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, None, condition)
        self._constraints.append(
            nrows_constraints.NRowsMax(
                ref, n_rows=n_rows_max, name=name, cache_size=cache_size
            )
        )

    def add_uniques_equality_constraint(
        self,
        columns: List[str],
        uniques: Collection[T],
        filter_func: Optional[Callable[[List[T]], List[T]]] = None,
        map_func: Optional[Callable[[T], T]] = None,
        reduce_func: Optional[Callable[[Collection], Collection]] = None,
        output_processors: Optional[
            Union[OutputProcessor, List[OutputProcessor]]
        ] = output_processor_limit,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Check if the data's unique values are equal to a given set of values.

        The ``UniquesEquality`` constraint asserts if the values contained in a column
        of a ``DataSource`` are strictly the ones of a reference set of expected values,
        specified via the ``uniques`` parameter.

        Null values in the columns ``columns`` are ignored. To assert the non-existence of them use
        the :meth:`~datajudge.requirements.WithinRequirement.add_null_absence_constraint`` helper method
        for ``WithinRequirement``.
        By default, the null filtering does not trigger if multiple columns are fetched at once.
        It can be configured in more detail by supplying a custom ``filter_func`` function.
        Some exemplary implementations are available as :func:`~datajudge.utils.filternull_element`,
        :func:`~datajudge.utils.filternull_never`, :func:`~datajudge.utils.filternull_element_or_tuple_all`,
        :func:`~datajudge.utils.filternull_element_or_tuple_any`.
        Passing ``None`` as the argument is equivalent to :func:`~datajudge.utils.filternull_element` but triggers a warning.
        The current default of :func:`~datajudge.utils.filternull_element`
        Cause (possibly often unintended) changes in behavior when the users adds a second column
        (filtering no longer can trigger at all).
        The default will be changed to :func:`~datajudge.utils.filternull_element_or_tuple_all` in future versions.
        To silence the warning, set ``filter_func`` explicitly.

        See the ``Uniques`` class for further parameter details on ``map_func`` and
        ``reduce_func``, and ``output_processors``.
        """

        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            uniques_constraints.UniquesEquality(
                ref,
                uniques=uniques,
                filter_func=filter_func,
                map_func=map_func,
                reduce_func=reduce_func,
                output_processors=output_processors,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_uniques_superset_constraint(
        self,
        columns: List[str],
        uniques: Collection[T],
        max_relative_violations: float = 0,
        filter_func: Optional[Callable[[List[T]], List[T]]] = None,
        map_func: Optional[Callable[[T], T]] = None,
        reduce_func: Optional[Callable[[Collection], Collection]] = None,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        output_processors: Optional[
            Union[OutputProcessor, List[OutputProcessor]]
        ] = output_processor_limit,
        cache_size=None,
    ):
        """Check if unique values of columns are contained in the reference data.

        The ``UniquesSuperset`` constraint asserts that reference set of expected values,
        specified via ``uniques``, is contained in given columns of a ``DataSource``.

        Null values in the columns ``columns`` are ignored. To assert the non-existence of them use
        the :meth:`~datajudge.requirements.WithinRequirement.add_null_absence_constraint`` helper method
        for ``WithinRequirement``.
        By default, the null filtering does not trigger if multiple columns are fetched at once.
        It can be configured in more detail by supplying a custom ``filter_func`` function.
        Some exemplary implementations are available as :func:`~datajudge.utils.filternull_element`,
        :func:`~datajudge.utils.filternull_never`, :func:`~datajudge.utils.filternull_element_or_tuple_all`,
        :func:`~datajudge.utils.filternull_element_or_tuple_any`.
        Passing ``None`` as the argument is equivalent to :func:`~datajudge.utils.filternull_element` but triggers a warning.
        The current default of :func:`~datajudge.utils.filternull_element`
        Cause (possibly often unintended) changes in behavior when the users adds a second column
        (filtering no longer can trigger at all).
        The default will be changed to :func:`~datajudge.utils.filternull_element_or_tuple_all` in future versions.
        To silence the warning, set ``filter_func`` explicitly..

        ``max_relative_violations`` indicates what fraction of unique values of the given
        ``DataSource`` are not represented in the reference set of unique values. Please
        note that ``UniquesSubset`` and ``UniquesSuperset`` are not symmetrical in this regard.

        One use of this constraint is to test for consistency in columns with expected
        categorical values.

        See ``Uniques`` for further details on ``map_func``, ``reduce_func``,
        and ``output_processors``.
        """

        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            uniques_constraints.UniquesSuperset(
                ref,
                uniques=uniques,
                max_relative_violations=max_relative_violations,
                filter_func=filter_func,
                map_func=map_func,
                reduce_func=reduce_func,
                output_processors=output_processors,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_uniques_subset_constraint(
        self,
        columns: List[str],
        uniques: Collection[T],
        max_relative_violations: float = 0,
        filter_func: Optional[Callable[[List[T]], List[T]]] = None,
        compare_distinct: bool = False,
        map_func: Optional[Callable[[T], T]] = None,
        reduce_func: Optional[Callable[[Collection], Collection]] = None,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        output_processors: Optional[
            Union[OutputProcessor, List[OutputProcessor]]
        ] = output_processor_limit,
        cache_size=None,
    ):
        """Check if the data's unique values are contained in a given set of values.

        The ``UniquesSubset`` constraint asserts if the values contained in a column of
        a ``DataSource`` are part of a reference set of expected values, specified via
        ``uniques``.

        Null values in the columns ``columns`` are ignored. To assert the non-existence of them use
        the :meth:`~datajudge.requirements.WithinRequirement.add_null_absence_constraint`` helper method
        for ``WithinRequirement``.
        By default, the null filtering does not trigger if multiple columns are fetched at once.
        It can be configured in more detail by supplying a custom ``filter_func`` function.
        Some exemplary implementations are available as :func:`~datajudge.utils.filternull_element`,
        :func:`~datajudge.utils.filternull_never`, :func:`~datajudge.utils.filternull_element_or_tuple_all`,
        :func:`~datajudge.utils.filternull_element_or_tuple_any`.
        Passing ``None`` as the argument is equivalent to :func:`~datajudge.utils.filternull_element` but triggers a warning.
        The current default of :func:`~datajudge.utils.filternull_element`
        Cause (possibly often unintended) changes in behavior when the users adds a second column
        (filtering no longer can trigger at all).
        The default will be changed to :func:`~datajudge.utils.filternull_element_or_tuple_all` in future versions.
        To silence the warning, set ``filter_func`` explicitly.


        ``max_relative_violations`` indicates what fraction of rows of the given table
        may have values not included in the reference set of unique values. Please note
        that ``UniquesSubset`` and ``UniquesSuperset`` are not symmetrical in this regard.

        By default, the number of occurrences affects the computed fraction of violations.
        To disable this weighting, set `compare_distinct=True`.
        This argument does not have an effect on the test results for other `Uniques` constraints,
        or if `max_relative_violations` is 0.

        See ``Uniques`` for further details on ``map_func``, ``reduce_func``,
        and ``output_processors``.
        """

        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            uniques_constraints.UniquesSubset(
                ref,
                uniques=uniques,
                max_relative_violations=max_relative_violations,
                filter_func=filter_func,
                compare_distinct=compare_distinct,
                map_func=map_func,
                reduce_func=reduce_func,
                output_processors=output_processors,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_n_uniques_equality_constraint(
        self,
        columns: Optional[List[str]],
        n_uniques: int,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            uniques_constraints.NUniquesEquality(
                ref, n_uniques=n_uniques, name=name, cache_size=cache_size
            )
        )

    def add_categorical_bound_constraint(
        self,
        columns: List[str],
        distribution: Dict[T, Tuple[float, float]],
        default_bounds: Tuple[float, float] = (0, 0),
        max_relative_violations: float = 0,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """
        Check if the distribution of unique values in columns falls within the
        specified minimum and maximum bounds.

        The `CategoricalBoundConstraint` is added to ensure the distribution of unique values
        in the specified columns of a `DataSource` falls within the given minimum and maximum
        bounds defined in the `distribution` parameter.

        Parameters
        ----------
        columns : List[str]
            A list of column names from the `DataSource` to apply the constraint on.
        distribution : Dict[T, Tuple[float, float]]
            A dictionary where keys represent unique values and the corresponding
            tuple values represent the minimum and maximum allowed proportions of the respective
            unique value in the columns.
        default_bounds : Tuple[float, float], optional, default=(0, 0)
            A tuple specifying the minimum and maximum allowed proportions for all
            elements not mentioned in the distribution. By default, it's set to (0, 0), which means
            all elements not present in `distribution` will cause a constraint failure.
        max_relative_violations : float, optional, default=0
            A tolerance threshold (0 to 1) for the proportion of elements in the data that can violate the
            bound constraints without triggering the constraint violation.
        condition : Condition, optional
            An optional parameter to specify a `Condition` object to filter the data
            before applying the constraint.
        name : str, optional
            An optional parameter to provide a custom name for the constraint.

        Example
        -------
        This method can be used to test for consistency in columns with expected categorical
        values or ensure that the distribution of values in a column adheres to a certain
        criterion.

        Usage:

        ```
        requirement = WithinRequirement(data_source)
        requirement.add_categorical_bound_constraint(
            columns=['column_name'],
            distribution={'A': (0.2, 0.3), 'B': (0.4, 0.6), 'C': (0.1, 0.2)},
            max_relative_violations=0.05,
            name='custom_name'
        )
        ```
        """

        ref = DataReference(self.data_source, columns, condition)
        self._constraints.append(
            uniques_constraints.CategoricalBoundConstraint(
                ref,
                distribution=distribution,
                default_bounds=default_bounds,
                max_relative_violations=max_relative_violations,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_numeric_min_constraint(
        self,
        column: str,
        min_value: float,
        condition: Optional[Condition] = None,
        cache_size=None,
    ):
        """All values in column are greater or equal min_value."""
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            numeric_constraints.NumericMin(
                ref, min_value=min_value, cache_size=cache_size
            )
        )

    def add_numeric_max_constraint(
        self,
        column: str,
        max_value: float,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """All values in column are less or equal max_value."""
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            numeric_constraints.NumericMax(
                ref, max_value=max_value, name=name, cache_size=cache_size
            )
        )

    def add_numeric_between_constraint(
        self,
        column: str,
        lower_bound: float,
        upper_bound: float,
        min_fraction: float,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Assert that the column's values lie between ``lower_bound`` and ``upper_bound``.

        Note that both bounds are inclusive.

        Unless specified otherwise via the usage of a ``condition``, ``NULL`` values will
        be considered in the denominator of ``min_fraction``. ``NULL`` values will never be
        considered to lie in the interval [``lower_bound``, ``upper_bound``].
        """
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            numeric_constraints.NumericBetween(
                ref,
                min_fraction,
                lower_bound,
                upper_bound,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_numeric_mean_constraint(
        self,
        column: str,
        mean_value: float,
        max_absolute_deviation: float,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Assert the mean of the column deviates at most max_deviation from mean_value."""
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            numeric_constraints.NumericMean(
                ref,
                max_absolute_deviation,
                mean_value=mean_value,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_numeric_percentile_constraint(
        self,
        column: str,
        percentage: float,
        expected_percentile: float,
        max_absolute_deviation: Optional[float] = None,
        max_relative_deviation: Optional[float] = None,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Assert that the ``percentage``-th percentile is approximately ``expected_percentile``.

        The percentile is defined as the smallest value present in ``column`` for which
        ``percentage`` % of the values in ``column`` are less or equal. ``NULL`` values
        are ignored.

        Hence, if ``percentage`` is less than the inverse of the number of non-``NULL`` rows,
        ``None`` is received as the ``percentage`` -th percentile.

        ``percentage`` is expected to be provided in percent. The median, for example, would
        correspond to ``percentage=50``.

        At least one of ``max_absolute_deviation`` and ``max_relative_deviation`` must
        be provided.
        """
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            numeric_constraints.NumericPercentile(
                ref,
                percentage=percentage,
                expected_percentile=expected_percentile,
                max_absolute_deviation=max_absolute_deviation,
                max_relative_deviation=max_relative_deviation,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_date_min_constraint(
        self,
        column: str,
        min_value: str,
        use_lower_bound_reference: bool = True,
        column_type: str = "date",
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Ensure all dates to be superior than min_value.

        Use string format: min_value="'20121230'".

        For more information on ``column_type`` values, see ``add_column_type_constraint``.

        If ``use_lower_bound_reference``, the min of the first table has to be
        greater or equal to ``min_value``.
        If not ``use_upper_bound_reference``, the min of the first table has to
        be smaller or equal to ``min_value``.
        """
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            date_constraints.DateMin(
                ref,
                min_value=min_value,
                use_lower_bound_reference=use_lower_bound_reference,
                column_type=column_type,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_date_max_constraint(
        self,
        column: str,
        max_value: str,
        use_upper_bound_reference: bool = True,
        column_type: str = "date",
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Ensure all dates to be superior than max_value.

        Use string format: max_value="'20121230'".

        For more information on ``column_type`` values, see ``add_column_type_constraint``.

        If ``use_upper_bound_reference``, the max of the first table has to be
        smaller or equal to ``max_value``.
        If not ``use_upper_bound_reference``, the max of the first table has to
        be greater or equal to ``max_value``.
        """
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            date_constraints.DateMax(
                ref,
                max_value=max_value,
                use_upper_bound_reference=use_upper_bound_reference,
                column_type=column_type,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_date_between_constraint(
        self,
        column: str,
        lower_bound: str,
        upper_bound: str,
        min_fraction: float,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Use string format: lower_bound="'20121230'"."""
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            date_constraints.DateBetween(
                ref,
                min_fraction,
                lower_bound,
                upper_bound,
                cache_size=cache_size,
            )
        )

    def add_date_no_overlap_constraint(
        self,
        start_column: str,
        end_column: str,
        key_columns: Optional[List[str]] = None,
        end_included: bool = True,
        max_relative_n_violations: float = 0,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Constraint expressing that several date range rows may not overlap.

        The ``DataSource`` under inspection must consist of at least one but up
        to many ``key_columns``, identifying an entity, a ``start_column`` and an
        ``end_column``.

        For a given row in this ``DataSource``, ``start_column`` and ``end_column`` indicate a
        date range. Neither of those columns should contain NULL values. Also, it
        should hold that for a given row, the value of ``end_column`` is strictly greater
        than the value of ``start_column``.

        Note that the value of ``start_column`` is expected to be included in each date
        range. By default, the value of ``end_column`` is expected to be included as well -
        this can however be changed by setting ``end_included`` to ``False``.

        A 'key' is a fixed set of values in ``key_columns`` and represents an entity of
        interest. A priori, a key is not a primary key, i.e., a key can have and often
        has several rows. Thereby, a key will often come with several date ranges.

        Often, you might want the date ranges for a given key not to overlap.

        If ``key_columns`` is ``None`` or ``[]``, all columns of the table will be considered
        as composing the key.

        In order to express a tolerance for some violations of this non-overlapping
        property, use the ``max_relative_n_violations`` parameter. The latter expresses for
        what fraction of all key values, at least one overlap may exist.

        For illustrative examples of this constraint, please refer to its test cases.
        """

        relevant_columns = [start_column, end_column] + (
            key_columns if key_columns else []
        )
        ref = DataReference(self.data_source, relevant_columns, condition)
        self._constraints.append(
            date_constraints.DateNoOverlap(
                ref,
                key_columns=key_columns,
                start_columns=[start_column],
                end_columns=[end_column],
                end_included=end_included,
                max_relative_n_violations=max_relative_n_violations,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_date_no_overlap_2d_constraint(
        self,
        start_column1: str,
        end_column1: str,
        start_column2: str,
        end_column2: str,
        key_columns: Optional[List[str]] = None,
        end_included: bool = True,
        max_relative_n_violations: float = 0,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Express that several date range rows do not overlap in two date dimensions.

        The table under inspection must consist of at least one but up to many key columns,
        identifying an entity. Per date dimension, a ``start_column`` and an
        ``end_column`` should be provided.

        For a given row in this table, ``start_column1`` and ``end_column1``
        indicate a date range. Moreoever, for that same row, ``start_column2``
        and ``end_column2`` indicate a date range.
        These date ranges are expected to represent different date 'dimensions'.
        Example: A row indicates a forecasted value used in production. ``start_column1``
        and ``end_column1`` represent the timespan that was forecasted, e.g. the
        weather from next Saturday to next Sunday. ``end_column1`` and ``end_column2``
        might indicate the timespan when this forceast was used, e.g. from the
        previous Monday to Wednesday.

        Neither of those columns should contain ``NULL`` values. Also it should
        hold that for a given row, the value of ``end_column`` is strictly greater
        than the value of ``start_column``.

        Note that the values of ``start_column1`` and ``start_column2`` are expected to be
        included in each date range. By default, the values of ``end_column1`` and
        ``end_column2`` are expected to be included as well - this can however be changed
        by setting ``end_included`` to ``False``.

        A 'key' is a fixed set of values in key_columns and represents an entity of
        interest. A priori, a key is not a primary key, i.e., a key can have and often has
        several rows. Thereby, a key will often come with several date ranges.

        Often, you might want the date ranges for a given key not to overlap.

        If key_columns is ``None`` or ``[]``, all columns of the table will be considered as
        composing the key.

        In order to express a tolerance for some violations of this non-overlapping property,
        use the ``max_relative_n_violations`` parameter. The latter expresses for what fraction
        of all key_values, at least one overlap may exist.

        For illustrative examples of this constraint, please refer to its test cases.
        """
        relevant_columns = (
            [start_column1, end_column1, start_column2, end_column2] + key_columns
            if key_columns
            else []
        )
        ref = DataReference(
            self.data_source,
            relevant_columns,
            condition,
        )
        self._constraints.append(
            date_constraints.DateNoOverlap2d(
                ref,
                key_columns=key_columns,
                start_columns=[start_column1, start_column2],
                end_columns=[end_column1, end_column2],
                end_included=end_included,
                max_relative_n_violations=max_relative_n_violations,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_date_no_gap_constraint(
        self,
        start_column: str,
        end_column: str,
        key_columns: Optional[List[str]] = None,
        end_included: bool = True,
        max_relative_n_violations: float = 0,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """
        Express that date range rows have no gap in-between them.

        The table under inspection must consist of at least one but up to many key columns,
        identifying an entity. Additionally, a ``start_column`` and an ``end_column``,
        indicating start and end dates, should be provided.

        Neither of those columns should contain ``NULL`` values. Also, it should hold that
        for a given row, the value of ``end_column`` is strictly greater than the value of
        ``start_column``.

        Note that the value of ``start_column`` is expected to be included in each date range.
        By default, the value of ``end_column`` is expected to be included as well - this can
        however be changed by setting ``end_included`` to ``False``.

        A 'key' is a fixed set of values in ``key_columns`` and represents an entity of
        interest. A priori, a key is not a primary key, i.e., a key can have and often has
        several rows. Thereby, a key will often come with several date ranges.

        If`` key_columns`` is ``None`` or ``[]``, all columns of the table will be
        considered as composing the key.

        In order to express a tolerance for some violations of this gap property, use the
        ``max_relative_n_violations`` parameter. The latter expresses for what fraction
        of all key_values, at least one gap may exist.

        For illustrative examples of this constraint, please refer to its test cases.
        """
        relevant_columns = (
            ([start_column, end_column] + key_columns) if key_columns else []
        )
        ref = DataReference(self.data_source, relevant_columns, condition)
        self._constraints.append(
            date_constraints.DateNoGap(
                ref,
                key_columns=key_columns,
                start_columns=[start_column],
                end_columns=[end_column],
                max_relative_n_violations=max_relative_n_violations,
                legitimate_gap_size=1 if end_included else 0,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_functional_dependency_constraint(
        self,
        key_columns: List[str],
        value_columns: List[str],
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        output_processors: Optional[
            Union[OutputProcessor, List[OutputProcessor]]
        ] = output_processor_limit,
        cache_size=None,
    ):
        """
        Expresses a functional dependency, a constraint where the `value_columns` are uniquely determined by the `key_columns`.
        This means that for each unique combination of values in the `key_columns`, there is exactly one corresponding combination of values in the `value_columns`.

        The ``add_unique_constraint`` constraint is a special case of this constraint, where the `key_columns` are a primary key,
        and all other columns are included `value_columns`.
        This constraint allows for a more general definition of functional dependencies, where the `key_columns` are not necessarily a primary key.

        An additional configuration option (for details see the analogous parameter in for ``Uniques``-constraints)
        on how the output is sorted and how many counterexamples are shown is available as ``output_processors``.

        An additional configuration option (for details see the analogous parameter in for ``Uniques``-constraints)
        on how the output is sorted and how many counterexamples are shown is available as ``output_processors``.

        For more information on functional dependencies, see https://en.wikipedia.org/wiki/Functional_dependency.
        """
        relevant_columns = key_columns + value_columns
        ref = DataReference(self.data_source, relevant_columns, condition)
        self._constraints.append(
            miscs_constraints.FunctionalDependency(
                ref,
                key_columns=key_columns,
                output_processors=output_processors,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_numeric_no_gap_constraint(
        self,
        start_column: str,
        end_column: str,
        key_columns: Optional[List[str]] = None,
        legitimate_gap_size: float = 0,
        max_relative_n_violations: float = 0,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """
        Express that numeric interval rows have no gaps larger than some max value in-between them.
        The table under inspection must consist of at least one but up to many key columns,
        identifying an entity. Additionally, a ``start_column`` and an ``end_column``,
        indicating interval start and end values, should be provided.

        Neither of those columns should contain ``NULL`` values. Also, it should hold that
        for a given row, the value of ``end_column`` is strictly greater than the value of
        ``start_column``.

        ``legitimate_gap_size`` is the maximum tollerated gap size between two intervals.

        A 'key' is a fixed set of values in ``key_columns`` and represents an entity of
        interest. A priori, a key is not a primary key, i.e., a key can have and often has
        several rows. Thereby, a key will often come with several intervals.

        If`` key_columns`` is ``None`` or ``[]``, all columns of the table will be
        considered as composing the key.

        In order to express a tolerance for some violations of this gap property, use the
        ``max_relative_n_violations`` parameter. The latter expresses for what fraction
        of all key_values, at least one gap may exist.

        For illustrative examples of this constraint, please refer to its test cases.
        """
        relevant_columns = (
            ([start_column, end_column] + key_columns) if key_columns else []
        )
        ref = DataReference(self.data_source, relevant_columns, condition)
        self._constraints.append(
            numeric_constraints.NumericNoGap(
                ref,
                key_columns=key_columns,
                start_columns=[start_column],
                end_columns=[end_column],
                legitimate_gap_size=legitimate_gap_size,
                max_relative_n_violations=max_relative_n_violations,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_numeric_no_overlap_constraint(
        self,
        start_column: str,
        end_column: str,
        key_columns: Optional[List[str]] = None,
        end_included: bool = True,
        max_relative_n_violations: float = 0,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Constraint expressing that several numeric interval rows may not overlap.

        The ``DataSource`` under inspection must consist of at least one but up
        to many ``key_columns``, identifying an entity, a ``start_column`` and an
        ``end_column``.

        For a given row in this ``DataSource``, ``start_column`` and ``end_column`` indicate a
        numeric interval. Neither of those columns should contain NULL values. Also, it
        should hold that for a given row, the value of ``end_column`` is strictly greater
        than the value of ``start_column``.

        Note that the value of ``start_column`` is expected to be included in each interval.
        By default, the value of ``end_column`` is expected to be included as well -
        this can however be changed by setting ``end_included`` to ``False``.

        A 'key' is a fixed set of values in ``key_columns`` and represents an entity of
        interest. A priori, a key is not a primary key, i.e., a key can have and often
        has several rows. Thereby, a key will often come with several intervals.

        Often, you might want the intervals for a given key not to overlap.

        If ``key_columns`` is ``None`` or ``[]``, all columns of the table will be considered
        as composing the key.

        In order to express a tolerance for some violations of this non-overlapping
        property, use the ``max_relative_n_violations`` parameter. The latter expresses for
        what fraction of all key values, at least one overlap may exist.

        For illustrative examples of this constraint, please refer to its test cases.
        """

        relevant_columns = [start_column, end_column] + (
            key_columns if key_columns else []
        )
        ref = DataReference(self.data_source, relevant_columns, condition)
        self._constraints.append(
            numeric_constraints.NumericNoOverlap(
                ref,
                key_columns=key_columns,
                start_columns=[start_column],
                end_columns=[end_column],
                end_included=end_included,
                max_relative_n_violations=max_relative_n_violations,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_varchar_regex_constraint(
        self,
        column: str,
        regex: str,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        allow_none: bool = False,
        relative_tolerance: float = 0.0,
        aggregated: bool = True,
        n_counterexamples: int = 5,
        cache_size=None,
    ):
        """
        Assesses whether the values in a column match a given regular expression pattern.

        The option ``allow_none`` can be used in cases where the column is defined as
        nullable and contains null values.

        How the tolerance factor is calculated can be controlled with the ``aggregated``
        flag. When ``True``, the tolerance is calculated using unique values. If not, the
        tolerance is calculated using all the instances of the data.

        ``n_counterexamples`` defines how many counterexamples are displayed in an
        assertion text. If all counterexamples are meant to be shown, provide ``-1`` as
        an argument.

        When using this method, the regex matching will take place in memory. If instead,
        you would like the matching to take place in database which is typically faster and
        substantially more memory-saving, please consider using
        ``add_varchar_regex_constraint_db``.
        """
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            varchar_constraints.VarCharRegex(
                ref,
                regex,
                allow_none=allow_none,
                relative_tolerance=relative_tolerance,
                aggregated=aggregated,
                n_counterexamples=n_counterexamples,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_varchar_regex_constraint_db(
        self,
        column: str,
        regex: str,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        relative_tolerance: float = 0.0,
        aggregated: bool = True,
        n_counterexamples: int = 5,
        cache_size=None,
    ):
        """
        Assesses whether the values in a column match a given regular expression pattern.

        How the tolerance factor is calculated can be controlled with the ``aggregated``
        flag. When ``True``, the tolerance is calculated using unique values. If not, the
        tolerance is calculated using all the instances of the data.

        ``n_counterexamples`` defines how many counterexamples are displayed in an
        assertion text. If all counterexamples are meant to be shown, provide ``-1`` as
        an argument.

        When using this method, the regex matching will take place in database, which is
        only supported for Postgres, Sqllite and Snowflake. Note that for this
        feature is only for Snowflake when using sqlalchemy-snowflake >= 1.4.0. As an
        alternative, ``add_varchar_regex_constraint`` performs the regex matching in memory.
        This is typically slower and more expensive in terms of memory but available
        on all supported database mamangement systems.
        """
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            varchar_constraints.VarCharRegexDb(
                ref,
                regex=regex,
                relative_tolerance=relative_tolerance,
                aggregated=aggregated,
                n_counterexamples=n_counterexamples,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_varchar_min_length_constraint(
        self,
        column: str,
        min_length: int,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            varchar_constraints.VarCharMinLength(
                ref,
                min_length=min_length,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_varchar_max_length_constraint(
        self,
        column: str,
        max_length: int,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, [column], condition)
        self._constraints.append(
            varchar_constraints.VarCharMaxLength(
                ref,
                max_length=max_length,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_groupby_aggregation_constraint(
        self,
        columns: Sequence[str],
        aggregation_column: str,
        start_value: int,
        tolerance: float = 0,
        condition: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Check whether array aggregate corresponds to an integer range.

        The ``DataSource`` is grouped by ``columns``. Sql's ``array_agg`` function is then
        applied to the ``aggregate_column``.

        Since we expect ``aggregate_column`` to be a numeric column, this leads to
        a multiset of aggregated values. These values should correspond to the integers
        ranging from ``start_value`` to the cardinality of the multiset.

        In order to allow for slight deviations from this pattern, ``tolerance`` expresses
        the fraction of all grouped-by rows, which may be incomplete ranges.
        """

        ref = DataReference(self.data_source, list(columns), condition)
        self._constraints.append(
            groupby_constraints.AggregateNumericRangeEquality(
                ref,
                aggregation_column=aggregation_column,
                tolerance=tolerance,
                start_value=start_value,
                name=name,
                cache_size=cache_size,
            )
        )


class BetweenRequirement(Requirement):
    def __init__(
        self,
        data_source: DataSource,
        data_source2: DataSource,
        date_column: Optional[str] = None,
        date_column2: Optional[str] = None,
    ):
        self.data_source = data_source
        self.data_source2 = data_source2
        self.ref = DataReference(self.data_source)
        self.ref2 = DataReference(self.data_source2)
        self.date_column = date_column
        self.date_column2 = date_column2
        super().__init__()

    @classmethod
    def from_tables(
        cls,
        db_name1: str,
        schema_name1: str,
        table_name1: str,
        db_name2: str,
        schema_name2: str,
        table_name2: str,
        date_column: Optional[str] = None,
        date_column2: Optional[str] = None,
    ):
        return cls(
            data_source=TableDataSource(
                db_name=db_name1,
                schema_name=schema_name1,
                table_name=table_name1,
            ),
            data_source2=TableDataSource(
                db_name=db_name2,
                schema_name=schema_name2,
                table_name=table_name2,
            ),
            date_column=date_column,
            date_column2=date_column2,
        )

    @classmethod
    def from_raw_queries(
        cls,
        query1: str,
        query2: str,
        name1: str,
        name2: str,
        columns1: Optional[List[str]] = None,
        columns2: Optional[List[str]] = None,
        date_column: Optional[str] = None,
        date_column2: Optional[str] = None,
    ):
        """Create a ``BetweenRequirement`` based on raw query strings.

        The ``query1`` and ``query2`` parameters can be passed any query string returning
        rows, e.g. ``"SELECT * FROM myschema.mytable LIMIT 1337"`` or
        ``"SELECT id, name FROM table1 UNION SELECT id, name FROM table2"``.

        ``name1`` and ``name2`` will be used to represent the queries in error messages,
        respectively.

        If constraints rely on specific columns, these should be provided here via
        ``columns1`` and ``columns2`` respectively.
        """
        return cls(
            data_source=RawQueryDataSource(query1, name1, columns=columns1),
            data_source2=RawQueryDataSource(query2, name2, columns=columns2),
            date_column=date_column,
            date_column2=date_column2,
        )

    @classmethod
    def from_expressions(
        cls,
        expression1,
        expression2,
        name1: str,
        name2: str,
        date_column: Optional[str] = None,
        date_column2: Optional[str] = None,
    ):
        """Create a ``BetweenTableRequirement`` based on sqlalchemy expressions.

        Any sqlalchemy object implementing the ``alias`` method can be passed as an
        argument for the ``expression1`` and ``expression2`` parameters. This could,
        e.g. be a ``sqlalchemy.Table`` object or the result of a ``sqlalchemy.select``
        invocation.

        ``name1`` and ``name2`` will be used to represent the expressions in error messages,
        respectively.
        """
        return cls(
            data_source=ExpressionDataSource(expression1, name1),
            data_source2=ExpressionDataSource(expression2, name2),
            date_column=date_column,
            date_column2=date_column2,
        )

    def get_date_growth_rate(self, engine) -> float:
        if self.date_column is None or self.date_column2 is None:
            raise ValueError("Date growth can't be computed without date column.")
        date_growth_rate, _ = get_date_growth_rate(
            engine, self.ref, self.ref2, self.date_column, self.date_column2
        )
        return date_growth_rate

    def get_deviation_getter(
        self, fix_value: Optional[float], deviation: Optional[float]
    ):
        if fix_value is None and deviation is None:
            return ValueError("No valid gain/loss/deviation given.")
        if deviation is None:
            return lambda engine: fix_value
        if fix_value is None:
            return lambda engine: self.get_date_growth_rate(engine) + deviation
        return lambda engine: max(
            fix_value, self.get_date_growth_rate(engine) + deviation
        )

    def add_n_rows_equality_constraint(
        self,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, condition=condition1)
        ref2 = DataReference(self.data_source2, condition=condition2)
        self._constraints.append(
            nrows_constraints.NRowsEquality(
                ref, ref2=ref2, name=name, cache_size=cache_size
            )
        )

    def add_n_rows_max_gain_constraint(
        self,
        constant_max_relative_gain: Optional[float] = None,
        date_range_gain_deviation: Optional[float] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """#rows from first table <= #rows from second table * (1 + max_growth).

        See readme for more information on max_growth.
        """
        max_relative_gain_getter = self.get_deviation_getter(
            constant_max_relative_gain, date_range_gain_deviation
        )
        ref = DataReference(self.data_source, condition=condition1)
        ref2 = DataReference(self.data_source2, condition=condition2)
        self._constraints.append(
            nrows_constraints.NRowsMaxGain(
                ref,
                ref2,
                max_relative_gain_getter,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_n_rows_min_gain_constraint(
        self,
        constant_min_relative_gain: Optional[float] = None,
        date_range_gain_deviation: Optional[float] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """#rows from first table  >= #rows from second table * (1 + min_growth).

        See readme for more information on min_growth.
        """
        min_relative_gain_getter = self.get_deviation_getter(
            constant_min_relative_gain, date_range_gain_deviation
        )
        ref = DataReference(self.data_source, condition=condition1)
        ref2 = DataReference(self.data_source2, condition=condition2)
        self._constraints.append(
            nrows_constraints.NRowsMinGain(
                ref,
                ref2,
                min_relative_gain_getter,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_n_rows_max_loss_constraint(
        self,
        constant_max_relative_loss: Optional[float] = None,
        date_range_loss_deviation: Optional[float] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """#rows from first table >= #rows from second table * (1 - max_loss).

        See readme for more information on max_loss.
        """
        max_relative_loss_getter = self.get_deviation_getter(
            constant_max_relative_loss, date_range_loss_deviation
        )
        ref = DataReference(self.data_source, condition=condition1)
        ref2 = DataReference(self.data_source2, condition=condition2)
        self._constraints.append(
            nrows_constraints.NRowsMaxLoss(
                ref,
                ref2,
                max_relative_loss_getter,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_n_uniques_equality_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.NUniquesEquality(
                ref, ref2=ref2, name=name, cache_size=cache_size
            )
        )

    def add_n_uniques_max_gain_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        constant_max_relative_gain: Optional[float] = None,
        date_range_gain_deviation: Optional[float] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """#uniques or first table <= #uniques of second table* (1 + max_growth).

        #uniques in first table are defined based on columns1, #uniques in second
        table are defined based on columns2.

        See readme for more information on max_growth.
        """
        max_relative_gain_getter = self.get_deviation_getter(
            constant_max_relative_gain, date_range_gain_deviation
        )
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.NUniquesMaxGain(
                ref,
                ref2,
                max_relative_gain_getter,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_n_uniques_max_loss_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        constant_max_relative_loss: Optional[float] = None,
        date_range_loss_deviation: Optional[float] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """#uniques in first table <= #uniques in second table * (1 - max_loss).

        #uniques in first table are defined based on columns1, #uniques in second
        table are defined based on columns2.

        See readme for more information on max_loss.
        """
        max_relative_loss_getter = self.get_deviation_getter(
            constant_max_relative_loss, date_range_loss_deviation
        )
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.NUniquesMaxLoss(
                ref,
                ref2,
                max_relative_loss_getter,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_max_null_fraction_constraint(
        self,
        column1: str,
        column2: str,
        max_relative_deviation: float,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Assert that the fraction of ``NULL`` values of one is at most that of the other.

        Given that ``column2``\'s underlying data has a fraction ``q`` of ``NULL`` values, the
        ``max_relative_deviation`` parameter allows ``column1``\'s underlying data to have a
        fraction ``(1 + max_relative_deviation) * q`` of ``NULL`` values.
        """
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            miscs_constraints.MaxNullFraction(
                ref,
                ref2=ref2,
                max_relative_deviation=max_relative_deviation,
                cache_size=cache_size,
            )
        )

    def add_numeric_min_constraint(
        self,
        column1: str,
        column2: str,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            numeric_constraints.NumericMin(
                ref, ref2=ref2, name=name, cache_size=cache_size
            )
        )

    def add_uniques_equality_constraint(
        self,
        columns1: List[str],
        columns2: List[str],
        filter_func: Optional[Callable[[List[T]], List[T]]] = None,
        map_func: Optional[Callable[[T], T]] = None,
        reduce_func: Optional[Callable[[Collection], Collection]] = None,
        output_processors: Optional[
            Union[OutputProcessor, List[OutputProcessor]]
        ] = output_processor_limit,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Check if the data's unique values in given columns are equal.

        The ``UniquesEquality`` constraint asserts if the values contained in a column
        of a ``DataSource``'s columns, are strictly the ones of another ``DataSource``'s
        columns.

        Null values in the columns ``columns`` are ignored. To assert the non-existence of them use
        the :meth:`~datajudge.requirements.WithinRequirement.add_null_absence_constraint`` helper method
        for ``WithinRequirement``.
        By default, the null filtering does not trigger if multiple columns are fetched at once.
        It can be configured in more detail by supplying a custom ``filter_func`` function.
        Some exemplary implementations are available as :func:`~datajudge.utils.filternull_element`,
        :func:`~datajudge.utils.filternull_never`, :func:`~datajudge.utils.filternull_element_or_tuple_all`,
        :func:`~datajudge.utils.filternull_element_or_tuple_any`.
        Passing ``None`` as the argument is equivalent to :func:`~datajudge.utils.filternull_element` but triggers a warning.
        The current default of :func:`~datajudge.utils.filternull_element`
        Cause (possibly often unintended) changes in behavior when the users adds a second column
        (filtering no longer can trigger at all).
        The default will be changed to :func:`~datajudge.utils.filternull_element_or_tuple_all` in future versions.
        To silence the warning, set ``filter_func`` explicitly..

        See :class:`~datajudge.constraints.uniques.Uniques` for further parameter details on ``map_func``,
        ``reduce_func``, and ``output_processors``.
        """

        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.UniquesEquality(
                ref,
                ref2=ref2,
                filter_func=filter_func,
                map_func=map_func,
                reduce_func=reduce_func,
                output_processors=output_processors,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_uniques_superset_constraint(
        self,
        columns1: List[str],
        columns2: List[str],
        max_relative_violations: float = 0,
        filter_func: Optional[Callable[[List[T]], List[T]]] = None,
        map_func: Optional[Callable[[T], T]] = None,
        reduce_func: Optional[Callable[[Collection], Collection]] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        output_processors: Optional[
            Union[OutputProcessor, List[OutputProcessor]]
        ] = output_processor_limit,
        cache_size=None,
    ):
        """Check if unique values of columns are contained in the reference data.

        The ``UniquesSuperset`` constraint asserts that reference set of expected values,
        derived from the unique values in given columns of the reference ``DataSource``,
        is contained in given columns of a ``DataSource``.

        Null values in the columns ``columns`` are ignored. To assert the non-existence of them use
        the :meth:`~datajudge.requirements.WithinRequirement.add_null_absence_constraint`` helper method
        for ``WithinRequirement``.
        By default, the null filtering does not trigger if multiple columns are fetched at once.
        It can be configured in more detail by supplying a custom ``filter_func`` function.
        Some exemplary implementations are available as :func:`~datajudge.utils.filternull_element`,
        :func:`~datajudge.utils.filternull_never`, :func:`~datajudge.utils.filternull_element_or_tuple_all`,
        :func:`~datajudge.utils.filternull_element_or_tuple_any`.
        Passing ``None`` as the argument is equivalent to :func:`~datajudge.utils.filternull_element` but triggers a warning.
        The current default of :func:`~datajudge.utils.filternull_element`
        Cause (possibly often unintended) changes in behavior when the users adds a second column
        (filtering no longer can trigger at all).
        The default will be changed to :func:`~datajudge.utils.filternull_element_or_tuple_all` in future versions.
        To silence the warning, set ``filter_func`` explicitly..

        ``max_relative_violations`` indicates what fraction of unique values of the given
        ``DataSource`` are not represented in the reference set of unique values. Please
        note that ``UniquesSubset`` and ``UniquesSuperset`` are not symmetrical in this regard.

        One use of this constraint is to test for consistency in columns with expected
        categorical values.

        See :class:`~datajudge.constraints.uniques.Uniques` for further details on ``map_func``, ``reduce_func``,
        and ``output_processors``.
        """

        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.UniquesSuperset(
                ref,
                ref2=ref2,
                max_relative_violations=max_relative_violations,
                filter_func=filter_func,
                map_func=map_func,
                reduce_func=reduce_func,
                output_processors=output_processors,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_uniques_subset_constraint(
        self,
        columns1: List[str],
        columns2: List[str],
        max_relative_violations: float = 0,
        filter_func: Optional[Callable[[List[T]], List[T]]] = None,
        compare_distinct: bool = False,
        map_func: Optional[Callable[[T], T]] = None,
        reduce_func: Optional[Callable[[Collection], Collection]] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        output_processors: Optional[
            Union[OutputProcessor, List[OutputProcessor]]
        ] = output_processor_limit,
        cache_size=None,
    ):
        """Check if the given columns's unique values in are contained in reference data.

        The ``UniquesSubset`` constraint asserts if the values contained in given column of
        a ``DataSource`` are part of the unique values of given columns of another
        ``DataSource``.

        Null values in the columns ``columns`` are ignored. To assert the non-existence of them use
        the :meth:`~datajudge.requirements.WithinRequirement.add_null_absence_constraint`` helper method
        for ``WithinRequirement``.
        By default, the null filtering does not trigger if multiple columns are fetched at once.
        It can be configured in more detail by supplying a custom ``filter_func`` function.
        Some exemplary implementations are available as :func:`~datajudge.utils.filternull_element`,
        :func:`~datajudge.utils.filternull_never`, :func:`~datajudge.utils.filternull_element_or_tuple_all`,
        :func:`~datajudge.utils.filternull_element_or_tuple_any`.
        Passing ``None`` as the argument is equivalent to :func:`~datajudge.utils.filternull_element` but triggers a warning.
        The current default of :func:`~datajudge.utils.filternull_element`
        Cause (possibly often unintended) changes in behavior when the users adds a second column
        (filtering no longer can trigger at all).
        The default will be changed to :func:`~datajudge.utils.filternull_element_or_tuple_all` in future versions.
        To silence the warning, set ``filter_func`` explicitly.
        ``max_relative_violations`` indicates what fraction of rows of the given table
        may have values not included in the reference set of unique values. Please note
        that ``UniquesSubset`` and ``UniquesSuperset`` are not symmetrical in this regard.

        By default, the number of occurrences affects the computed fraction of violations.
        To disable this weighting, set ``compare_distinct=True``.
        This argument does not have an effect on the test results for other :class:`~datajudge.constraints.uniques.Uniques` constraints,
        or if ``max_relative_violations`` is 0.

        See :class:`~datajudge.constraints.uniques.Uniques` for further details on ``map_func``, ``reduce_func``,
        and ``output_processors``.
        """

        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            uniques_constraints.UniquesSubset(
                ref,
                ref2=ref2,
                max_relative_violations=max_relative_violations,
                compare_distinct=compare_distinct,
                filter_func=filter_func,
                map_func=map_func,
                reduce_func=reduce_func,
                output_processors=output_processors,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_numeric_max_constraint(
        self,
        column1: str,
        column2: str,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            numeric_constraints.NumericMax(
                ref, ref2=ref2, name=name, cache_size=cache_size
            )
        )

    def add_numeric_mean_constraint(
        self,
        column1: str,
        column2: str,
        max_absolute_deviation: float,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            numeric_constraints.NumericMean(
                ref,
                max_absolute_deviation,
                ref2=ref2,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_numeric_percentile_constraint(
        self,
        column1: str,
        column2: str,
        percentage: float,
        max_absolute_deviation: Optional[float] = None,
        max_relative_deviation: Optional[float] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Assert that the ``percentage``-th percentile is approximately equal.

        The percentile is defined as the smallest value present in ``column1`` / ``column2``
        for which ``percentage`` % of the values in ``column1`` / ``column2`` are
        less or equal. ``NULL`` values are ignored.

        Hence, if ``percentage`` is less than the inverse of the number of non-``NULL``
        rows, ``None`` is received as the ``percentage``-th percentile.

        ``percentage`` is expected to be provided in percent. The median, for example,
        would correspond to ``percentage=50``.

        At least one of ``max_absolute_deviation`` and ``max_relative_deviation`` must
        be provided.
        """
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            numeric_constraints.NumericPercentile(
                ref,
                percentage=percentage,
                max_absolute_deviation=max_absolute_deviation,
                max_relative_deviation=max_relative_deviation,
                ref2=ref2,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_date_min_constraint(
        self,
        column1: str,
        column2: str,
        use_lower_bound_reference: bool = True,
        column_type: str = "date",
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Ensure date min of first table is greater or equal date min of second table.

        The used columns of both tables need to be of the same type.

        For more information on ``column_type`` values, see ``add_column_type_constraint``.

        If ``use_lower_bound_reference``, the min of the first table has to be
        greater or equal to the min of the second table.
        If not ``use_upper_bound_reference``, the min of the first table has to
        be smaller or equal to the min of the second table.
        """
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            date_constraints.DateMin(
                ref,
                ref2=ref2,
                use_lower_bound_reference=use_lower_bound_reference,
                column_type=column_type,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_date_max_constraint(
        self,
        column1: str,
        column2: str,
        use_upper_bound_reference: bool = True,
        column_type: str = "date",
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Compare date max of first table to date max of second table.

        The used columns of both tables need to be of the same type.

        For more information on ``column_type`` values, see ``add_column_type_constraint``.

        If ``use_upper_bound_reference``, the max of the first table has to be
        smaller or equal to the max of the second table.
        If not ``use_upper_bound_reference``, the max of the first table has to
        be greater or equal to the max of the second table.
        """
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            date_constraints.DateMax(
                ref,
                ref2=ref2,
                use_upper_bound_reference=use_upper_bound_reference,
                column_type=column_type,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_varchar_min_length_constraint(
        self,
        column1: str,
        column2: str,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            varchar_constraints.VarCharMinLength(
                ref, ref2=ref2, name=name, cache_size=cache_size
            )
        )

    def add_varchar_max_length_constraint(
        self,
        column1: str,
        column2: str,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        ref = DataReference(self.data_source, [column1], condition1)
        ref2 = DataReference(self.data_source2, [column2], condition2)
        self._constraints.append(
            varchar_constraints.VarCharMaxLength(
                ref, ref2=ref2, name=name, cache_size=cache_size
            )
        )

    def add_column_subset_constraint(self, name: Optional[str] = None, cache_size=None):
        """Columns of first table are subset of second table."""
        self._constraints.append(
            column_constraints.ColumnSubset(
                self.ref, ref2=self.ref2, name=name, cache_size=cache_size
            )
        )

    def add_column_superset_constraint(
        self, name: Optional[str] = None, cache_size=None
    ):
        """Columns of first table are superset of columns of second table."""
        self._constraints.append(
            column_constraints.ColumnSuperset(
                self.ref, ref2=self.ref2, name=name, cache_size=cache_size
            )
        )

    def add_column_type_constraint(
        self,
        column1: str,
        column2: str,
        name: Optional[str] = None,
        cache_size=None,
    ):
        "Check that the columns have the same type."
        ref1 = DataReference(self.data_source, [column1])
        ref2 = DataReference(self.data_source2, [column2])
        self._constraints.append(
            column_constraints.ColumnType(
                ref1, ref2=ref2, name=name, cache_size=cache_size
            )
        )

    def add_row_equality_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        max_missing_fraction: float,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """At most ``max_missing_fraction`` of rows in T1 and T2 are absent in either.

        In other words,
        :math:`\\frac{|T1 - T2| + |T2 - T1|}{|T1 \\cup T2|} \\leq` ``max_missing_fraction``.
        Rows from T1 are indexed in ``columns1``, rows from T2 are indexed in ``columns2``.
        """
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            row_constraints.RowEquality(
                ref,
                ref2,
                lambda engine: max_missing_fraction,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_row_subset_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        constant_max_missing_fraction: Optional[float],
        date_range_loss_fraction: Optional[float] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """At most ``max_missing_fraction`` of rows in T1 are not in T2.

        In other words,
        :math:`\\frac{|T1-T2|}{|T1|} \\leq` ``max_missing_fraction``.
        Rows from T1 are indexed in columns1, rows from T2 are indexed in ``columns2``.

        In particular, the operation ``|T1-T2|`` relies on a sql ``EXCEPT`` statement. In
        contrast to ``EXCEPT ALL``, this should lead to a set subtraction instead of
        a multiset subtraction. In other words, duplicates in T1 are treated as
        single occurrences.
        """
        max_missing_fraction_getter = self.get_deviation_getter(
            constant_max_missing_fraction, date_range_loss_fraction
        )
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            row_constraints.RowSubset(
                ref,
                ref2,
                max_missing_fraction_getter,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_row_superset_constraint(
        self,
        columns1: Optional[List[str]],
        columns2: Optional[List[str]],
        constant_max_missing_fraction: float,
        date_range_loss_fraction: Optional[float] = None,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """At most ``max_missing_fraction`` of rows in T2 are not in T1.

        In other words,
        :math:`\\frac{|T2-T1|}{|T2|} \\leq` ``max_missing_fraction``.
        Rows from T1 are indexed in ``columns1``, rows from T2 are indexed in
        ``columns2``.
        """
        max_missing_fraction_getter = self.get_deviation_getter(
            constant_max_missing_fraction, date_range_loss_fraction
        )
        ref = DataReference(self.data_source, columns1, condition1)
        ref2 = DataReference(self.data_source2, columns2, condition2)
        self._constraints.append(
            row_constraints.RowSuperset(
                ref,
                ref2,
                max_missing_fraction_getter,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_row_matching_equality_constraint(
        self,
        matching_columns1: List[str],
        matching_columns2: List[str],
        comparison_columns1: List[str],
        comparison_columns2: List[str],
        max_missing_fraction: float,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        cache_size=None,
    ):
        """Match tables in matching_columns, compare for equality in comparison_columns.

        This constraint is similar to the nature of the ``RowEquality``
        constraint. Just as the latter, this constraint divides the
        cardinality of an intersection by the cardinality of a union.
        The difference lies in how the set are created. While ``RowEquality``
        considers all rows of both tables, indexed in columns,
        ``RowMatchingEquality`` considers only rows in both tables having values
        in ``matching_columns`` present in both tables. At most ``max_missing_fraction``
        of such rows can be missing in the intersection.

        Alternatively, this can be thought of as counting mismatches in
        ``comparison_columns`` after performing an inner join on ``matching_columns``.
        """
        ref = DataReference(
            self.data_source, matching_columns1 + comparison_columns1, condition1
        )
        ref2 = DataReference(
            self.data_source2, matching_columns2 + comparison_columns2, condition2
        )
        self._constraints.append(
            row_constraints.RowMatchingEquality(
                ref,
                ref2,
                matching_columns1,
                matching_columns2,
                comparison_columns1,
                comparison_columns2,
                lambda engine: max_missing_fraction,
                name=name,
                cache_size=cache_size,
            )
        )

    def add_ks_2sample_constraint(
        self,
        column1: str,
        column2: str,
        condition1: Optional[Condition] = None,
        condition2: Optional[Condition] = None,
        name: Optional[str] = None,
        significance_level: float = 0.05,
        cache_size=None,
    ):
        """
        Apply the so-called two-sample Kolmogorov-Smirnov test to the distributions of the two given columns.
        The constraint is fulfilled, when the resulting p-value of the test is higher than the significance level
        (default is 0.05, i.e., 5%).
        The signifance_level must be a value between 0.0 and 1.0.
        """

        if not column1 or not column2:
            raise ValueError(
                "Column names have to be given for this test's functionality."
            )

        if significance_level <= 0.0 or significance_level > 1.0:
            raise ValueError(
                "The requested significance level has to be in ``(0.0, 1.0]``. Default is 0.05."
            )

        ref = DataReference(self.data_source, [column1], condition=condition1)
        ref2 = DataReference(self.data_source2, [column2], condition=condition2)
        self._constraints.append(
            stats_constraints.KolmogorovSmirnov2Sample(
                ref,
                ref2,
                significance_level,
                name=name,
                cache_size=cache_size,
            )
        )
