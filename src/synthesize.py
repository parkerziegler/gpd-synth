from pandas import DataFrame
from geopandas import GeoDataFrame
from typing import Callable, Generator, TypeAlias
from itertools import product, combinations

from grammar import GrammarRule, Merge, SJoin, GDF, Dissolve
from synth_input import GdfBindings


CandidateGen:    TypeAlias = Generator[GrammarRule, None, None]
LazySynthesizer: TypeAlias = Callable[[GdfBindings], CandidateGen]


def program(gdfs: GdfBindings) -> CandidateGen:
    yield from univariate(gdfs)
    yield from bivariate(gdfs)


def univariate(gdfs: GdfBindings) -> CandidateGen:
    'Generates the simplest programs first, including equivalents.'
    for gdf_name in gdfs.keys():
        yield GDF(gdf_name)

    for gdf_name, gdf in gdfs.items():
        for col in gdf.columns:
            if col != 'geometry':
                yield Dissolve(gdf_name, col)


def bivariate(gdfs: GdfBindings) -> CandidateGen:
    merge_gen = merge(gdfs)
    sjoin_gen = sjoin(gdfs)
    for m_candidate, s_candidate in zip(merge_gen, sjoin_gen):
        yield m_candidate
        yield s_candidate
    yield from sjoin_gen
    yield from merge_gen


def make_candidate_filter(gdfs: GdfBindings, target: DataFrame) -> Callable[[GrammarRule], bool]:
    'Returns a predicate that checks if a `program` over `gdfs` matches `target`'
    return lambda program: GeoDataFrame.equals(program.interpret(gdfs), target)


def lazy_synth(gdfs: GdfBindings, synthesizer: LazySynthesizer, target: DataFrame) -> CandidateGen:
    'An iterator over all programs over `gdfs` that match `target`'
    checker = make_candidate_filter(gdfs, target)
    return filter(checker, synthesizer(gdfs))


def lazy_synthesize(gdfs: GdfBindings, target: DataFrame) -> None:
    'Lazy counterpart to `synthesize`'
    print(next(lazy_synth(gdfs, program, target), 'No program found!'))


def binding_pairs(gdfs: GdfBindings):
    'Returns a generator over all unique (unordered) pairs of DataFrames in gdfs'
    return combinations(gdfs.items(), 2)


def merge(gdfs: GdfBindings):
    'Generates all valid pd.merge calls over gdfs'
    for (l_name, _), (r_name, _) in binding_pairs(gdfs):
        l_types = gdfs.get_cols_dtype(l_name)
        r_types = gdfs.get_cols_dtype(r_name)
        for k, l_v in l_types.items():
            if r_v := r_types.get(k, None):
                for l_col, r_col in product(l_v, r_v):
                    # 'cross' throws weird exception
                    for h in ("left", "right", "inner", "outer"):  
                        yield Merge(l_name, r_name, how=h, \
                            left_on=l_col, right_on=r_col)


def sjoin(gdfs: GdfBindings):
    'Generates all valid gpd.sjoin calls over gdfs'
    for (l, l_frame), (r, r_frame) in binding_pairs(gdfs):
        if type(l_frame) == GeoDataFrame and type(r_frame) == GeoDataFrame:
            query_preds = l_frame.sindex.valid_query_predicates & \
                r_frame.sindex.valid_query_predicates
            for h in ("left", "right", "inner"):
                for p in query_preds:
                    yield SJoin(l, r, how=h, predicate=p)
