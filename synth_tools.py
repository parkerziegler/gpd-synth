import pandas as pd
import geopandas as gpd

from geopandas import GeoDataFrame as GDF

from collections import defaultdict
from itertools import product
from typing import Iterable, Optional


def synth_first(synth: Iterable[Optional[object]]) -> Optional[object]:
    return next(filter(bool, synth), None)


def print_all_synth(synth: Iterable[Optional[object]]) -> object:
    print("Warning, print_all may take a loooong time")
    any(map(print, filter(bool, synth)))
    print("Done.")


def slim(res: GDF, target: GDF) -> None | GDF:
    try:
        return res[target.columns]
    except KeyError:
        return None

def check_candidate(result: pd.DataFrame, target: pd.DataFrame) -> bool:
    slimmed = slim(result, target)
    return target.equals(slimmed)


def sjoin(l: GDF, r: GDF) -> Iterable[GDF]:
    query_preds = l.sindex.valid_query_predicates & r.sindex.valid_query_predicates
    for h in ("left", "right", "inner"):
        for p in query_preds:
            yield gpd.sjoin(l, r, how=h, predicate=p), ('sjoin', h, p)


def with_target(synthesizer, t: GDF):
    for res, signature in synthesizer:
        yield check_candidate(res, t) and signature


def cols_by_dtype(frame: pd.DataFrame) -> dict[type, set[str]]:
    out = defaultdict(set)
    for k, v in dict(frame.dtypes).items():
        out[v].add(k)
    return dict(out)


def merge(l: pd.DataFrame, r: pd.DataFrame):
    l_types = cols_by_dtype(l)
    r_types = cols_by_dtype(r)
    for k, l_v in l_types.items():
        if r_v := r_types.get(k, None):
            for l_col, r_col in product(l_v, r_v):
                # 'cross' throws weird exception
                for h in ("left", "right", "inner", "outer"):  
                    yield pd.merge(
                            l, r, 
                            how=h, 
                            left_on=l_col, right_on=r_col,
                        ), ('merge', h, l_col, r_col)


def binop(l: pd.DataFrame, r: pd.DataFrame):
    merge_gen = merge(l, r)
    if not isinstance(l, GDF) or not isinstance(r, GDF):
        yield from merge_gen
        return

    sjoin_gen = sjoin(l, r)
    for t0, t1 in zip(merge_gen, sjoin_gen):
        yield t0
        yield t1
    done = ()
    test = next(merge_gen, done)
    if test is done:
        yield from sjoin_gen
    else:
        yield test
        yield from merge_gen