"""
Testing functionality for geopandas objects.
"""
from __future__ import annotations

from typing import cast

import pandas as pd

from pandas._libs.lib import (
    NoDefault,
    no_default,
)
import pandas._libs.testing as _testing
from pandas._libs.missing import is_matching_na
from pandas._testing.asserters import (
    assert_categorical_equal, 
    assert_interval_array_equal,
    _get_tol_from_less_precise
)

from pandas.core.dtypes.common import is_categorical_dtype
from pandas import (
    Index,
    IntervalIndex,
    MultiIndex,
    PeriodIndex,
)
from pandas.core.indexes.numeric import NumericIndex

from pandas.core.algorithms import (
    safe_sort,
    take_nd,
)

from geopandas import GeoDataFrame, GeoSeries, _vectorized
from geopandas.array import GeometryDtype
from geopandas.testing import _geom_equals_mask, _geom_almost_equals_mask

try:
    # added from pandas 0.20
    from pandas.testing import assert_frame_equal
except ImportError:
    from pandas.util.testing import assert_frame_equal



def geoseries_equal(
    left,
    right,
    check_dtype=True,
    check_index_type=False,
    check_series_type=True,
    check_less_precise=False,
    check_geom_type=False,
    check_crs=True,
    normalize=False,
):
    """
    Test util for checking that two GeoSeries are equal.

    Parameters
    ----------
    left, right : two GeoSeries
    check_dtype : bool, default False
        If True, check geo dtype [only included so it's a drop-in replacement
        for try assert_series_equal].
    check_index_type : bool, default False
        Check that index types are equal.
    check_series_type : bool, default True
        Check that both are same type (*and* are GeoSeries). If False,
        will attempt to convert both into GeoSeries.
    check_less_precise : bool, default False
        If True, use geom_almost_equals. if False, use geom_equals.
    check_geom_type : bool, default False
        If True, check that all the geom types are equal.
    check_crs: bool, default True
        If `check_series_type` is True, then also check that the
        crs matches.
    normalize: bool, default False
        If True, normalize the geometries before comparing equality.
        Typically useful with ``check_less_precise=True``, which uses
        ``geom_almost_equals`` and requires exact coordinate order.
    """
    if len(left) != len(right):
        return False

    if check_dtype:
        if not isinstance(left.dtype, GeometryDtype) or not isinstance(right.dtype, GeometryDtype):
            return False

    if check_index_type and not isinstance(left.index, type(right.index)):
        return False

    if check_series_type:
        if not isinstance(left, GeoSeries) or not isinstance(left, type(right)):
            return False

        if check_crs and left.crs != right.crs:
            return False
    else:
        if not isinstance(left, GeoSeries):
            left = GeoSeries(left)
        if not isinstance(right, GeoSeries):
            right = GeoSeries(right, index=left.index)

    if not left.index.equals(right.index):
        return False

    if check_geom_type:
        if not (left.type == right.type).all():
            return False

    if normalize:
        left = GeoSeries(_vectorized.normalize(left.array.data))
        right = GeoSeries(_vectorized.normalize(right.array.data))

    return _check_equality(left, right, check_less_precise)


def _check_equality(left, right, check_less_precise):
    if check_less_precise:
        equal = _geom_almost_equals_mask(left, right)
    else:
        equal = _geom_equals_mask(left, right)

    return equal.all()
        

def geodataframe_equal(
    left,
    right,
    check_dtype=True,
    check_index_type="equiv",
    check_column_type="equiv",
    check_frame_type=True,
    check_like=False,
    check_less_precise=False,
    check_geom_type=False,
    check_crs=True,
    normalize=False,
):
    """
    Check that two GeoDataFrames are equal/
    Parameters
    ----------
    left, right : two GeoDataFrames
    check_dtype : bool, default True
        Whether to check the DataFrame dtype is identical.
    check_index_type, check_column_type : bool, default 'equiv'
        Check that index types are equal.
    check_frame_type : bool, default True
        Check that both are same type (*and* are GeoDataFrames). If False,
        will attempt to convert both into GeoDataFrame.
    check_like : bool, default False
        If true, ignore the order of rows & columns
    check_less_precise : bool, default False
        If True, use geom_almost_equals. if False, use geom_equals.
    check_geom_type : bool, default False
        If True, check that all the geom types are equal.
    check_crs: bool, default True
        If `check_frame_type` is True, then also check that the
        crs matches.
    normalize: bool, default False
        If True, normalize the geometries before comparing equality.
        Typically useful with ``check_less_precise=True``, which uses
        ``geom_almost_equals`` and requires exact coordinate order.
    """
    # instance validation
    if check_frame_type:
        if not isinstance(left, GeoDataFrame) or not isinstance(left, type(right)):
            return False

        if check_crs:
            # no crs can be either None or {}
            if not left.crs and not right.crs:
                pass
            else:
                if left.crs != right.crs:
                    return False
    else:
        if not isinstance(left, GeoDataFrame):
            left = GeoDataFrame(left)
        if not isinstance(right, GeoDataFrame):
            right = GeoDataFrame(right)

    # shape comparison
    if left.shape != right.shape:
        return False

    if check_like:
        left, right = left.reindex_like(right), right
    

    # column comparison
    if not index_equal(
        left.columns, right.columns, exact=check_column_type, obj="GeoDataFrame.columns"
    ):
        return False

    # geometry comparison
    for col, dtype in left.dtypes.items():
        if isinstance(dtype, GeometryDtype):
            if not geoseries_equal(
                left[col],
                right[col],
                normalize=normalize,
                check_dtype=check_dtype,
                check_less_precise=check_less_precise,
                check_geom_type=check_geom_type,
                check_crs=check_crs,
            ):
                return False

    try:
        # drop geometries and check remaining columns
        left2 = left.drop([left._geometry_column_name], axis=1)
        right2 = right.drop([right._geometry_column_name], axis=1)
        assert_frame_equal(
            left2,
            right2,
            check_dtype=check_dtype,
            check_index_type=check_index_type,
            check_column_type=check_column_type,
            obj="GeoDataFrame",
        )
        return True
    except AssertionError:
        return False


def index_equal(
    left: Index,
    right: Index,
    exact: bool | str = "equiv",
    check_names: bool = True,
    check_less_precise: bool | int | NoDefault = no_default,
    check_exact: bool = True,
    check_categorical: bool = True,
    check_order: bool = True,
    rtol: float = 1.0e-5,
    atol: float = 1.0e-8,
    obj: str = "Index",
) -> bool:
    """
    Check that left and right Index are equal.

    Parameters
    ----------
    left : Index
    right : Index
    exact : bool or {'equiv'}, default 'equiv'
        Whether to check the Index class, dtype and inferred_type
        are identical. If 'equiv', then RangeIndex can be substituted for
        Int64Index as well.
    check_names : bool, default True
        Whether to check the names attribute.
    check_less_precise : bool or int, default False
        Specify comparison precision. Only used when check_exact is False.
        5 digits (False) or 3 digits (True) after decimal points are compared.
        If int, then specify the digits to compare.

        .. deprecated:: 1.1.0
           Use `rtol` and `atol` instead to define relative/absolute
           tolerance, respectively. Similar to :func:`math.isclose`.
    check_exact : bool, default True
        Whether to compare number exactly.
    check_categorical : bool, default True
        Whether to compare internal Categorical exactly.
    check_order : bool, default True
        Whether to compare the order of index entries as well as their values.
        If True, both indexes must contain the same elements, in the same order.
        If False, both indexes must contain the same elements, but in any order.

        .. versionadded:: 1.2.0
    rtol : float, default 1e-5
        Relative tolerance. Only used when check_exact is False.

        .. versionadded:: 1.1.0
    atol : float, default 1e-8
        Absolute tolerance. Only used when check_exact is False.

        .. versionadded:: 1.1.0
    obj : str, default 'Index'
        Specify object name being compared, internally used to show appropriate
        assertion message.

    Examples
    --------
    >>> from pandas import testing as tm
    >>> a = pd.Index([1, 2, 3])
    >>> b = pd.Index([1, 2, 3])
    >>> tm.assert_index_equal(a, b)
    """
    __tracebackhide__ = True

    def _check_types(left, right, obj="Index") -> bool:
        if not exact:
            return True

        if not class_equal(left, right, exact=exact, obj=obj) or \
            not attr_equal("inferred_type", left, right, obj=obj):
                return False

        # Skip exact dtype checking when `check_categorical` is False
        if is_categorical_dtype(left.dtype) and is_categorical_dtype(right.dtype):
            if check_categorical:
                if not attr_equal("dtype", left, right, obj=obj) or \
                    not index_equal(left.categories, right.categories, exact=exact):
                        return False
            return True

        return attr_equal("dtype", left, right, obj=obj)

    def _get_ilevel_values(index, level):
        # accept level number only
        unique = index.levels[level]
        level_codes = index.codes[level]
        filled = take_nd(unique._values, level_codes, fill_value=unique._na_value)
        return unique._shallow_copy(filled, name=index.names[level])

    if check_less_precise is not no_default:
        # https://github.com/python/mypy/issues/7642
        # error: Argument 1 to "_get_tol_from_less_precise" has incompatible
        # type "Union[bool, int, NoDefault]"; expected "Union[bool, int]"
        rtol = atol = _get_tol_from_less_precise(
            check_less_precise  # type: ignore[arg-type]
        )

    # instance validation
    if not _check_isinstance(left, right, Index):
        return False

    # class / dtype comparison
    if not _check_types(left, right, obj=obj):
        return False

    # level comparison
    if left.nlevels != right.nlevels:
        return False

    # length comparison
    if len(left) != len(right):
        return False

    # If order doesn't matter then sort the index entries
    if not check_order:
        left = Index(safe_sort(left))
        right = Index(safe_sort(right))

    # MultiIndex special comparison for little-friendly error messages
    if left.nlevels > 1:
        left = cast(MultiIndex, left)
        right = cast(MultiIndex, right)

        for level in range(left.nlevels):
            # cannot use get_level_values here because it can change dtype
            llevel = _get_ilevel_values(left, level)
            rlevel = _get_ilevel_values(right, level)

            lobj = f"MultiIndex level [{level}]"
            if not index_equal(
                llevel,
                rlevel,
                exact=exact,
                check_names=check_names,
                check_exact=check_exact,
                rtol=rtol,
                atol=atol,
                obj=lobj,
            ) or not _check_types(left.levels[level], right.levels[level], obj=obj):
                return False

    # skip exact index checking when `check_categorical` is False
    if check_exact and check_categorical:
        if not left.equals(right):
            return False
        skip = False
    else:
        skip = True
    
    try:
        if not skip:
            # if we have "equiv", this becomes True
            exact_bool = bool(exact)
            _testing.assert_almost_equal(
                left.values,
                right.values,
                rtol=rtol,
                atol=atol,
                check_dtype=exact_bool,
                obj=obj,
                lobj=left,
                robj=right,
            )

        # metadata comparison
        if check_names:
            if not attr_equal("names", left, right, obj=obj):
                return False
        if isinstance(left, PeriodIndex) or isinstance(right, PeriodIndex):
            if not attr_equal("freq", left, right, obj=obj):
                return False
        if isinstance(left, IntervalIndex) or isinstance(right, IntervalIndex):
            assert_interval_array_equal(left._values, right._values)

        if check_categorical:
            if is_categorical_dtype(left.dtype) or is_categorical_dtype(right.dtype):
                assert_categorical_equal(left._values, right._values, obj=f"{obj} category")
        return True
    except AssertionError:
        return False

def class_equal(left, right, exact: bool | str = True, obj="Input"):
    """
    Checks classes are equal.
    """
    __tracebackhide__ = True

    def repr_class(x):
        if isinstance(x, Index):
            # return Index as it is to include values in the error message
            return x

        return type(x).__name__

    if type(left) == type(right):
        return True

    if exact == "equiv":
        # accept equivalence of NumericIndex (sub-)classes
        if isinstance(left, NumericIndex) and isinstance(right, NumericIndex):
            return True

    return False


def attr_equal(attr: str, left, right, obj: str = "Attributes"):
    """
    Check attributes are equal. Both objects must have attribute.

    Parameters
    ----------
    attr : str
        Attribute name being compared.
    left : object
    right : object
    obj : str, default 'Attributes'
        Specify object name being compared, internally used to show appropriate
        assertion message
    """
    __tracebackhide__ = True

    left_attr = getattr(left, attr)
    right_attr = getattr(right, attr)

    if left_attr is right_attr:
        return True
    elif is_matching_na(left_attr, right_attr):
        # e.g. both np.nan, both NaT, both pd.NA, ...
        return True

    try:
        result = left_attr == right_attr
    except TypeError:
        # datetimetz on rhs may raise TypeError
        result = False
    if (left_attr is pd.NA) ^ (right_attr is pd.NA):
        result = False
    elif not isinstance(result, bool):
        result = result.all()
    
    return result


def _check_isinstance(left, right, cls):
    return isinstance(left, cls) and isinstance(right, cls)
