from pandas import DataFrame
from geopandas import GeoDataFrame
from geopandas.testing import assert_geodataframe_equal
from typing import Callable, Generator, TypeAlias
from itertools import product, combinations

from synth_bindings import GdfBindings
from grammar import Candidate, Merge, SJoin, GDF, Dissolve


CandidateGen: TypeAlias = Generator[Candidate, None, None]


def program(gdfs: GdfBindings) -> CandidateGen:
    'Generates all valid programs (simplest first) over `gdfs`'
    # generate just the inputs as outputs
    yield from map(GDF, gdfs.keys())
    # generate univariate programs
    yield from univariate(gdfs)
    # generate bivariate programs
    yield from bivariate(gdfs)


def univariate(gdfs: dict[str, DataFrame]) -> CandidateGen:
    'Generates all valid univariate programs over `gdfs`'
    for gdf_name, gdf in gdfs.items():
        for col in gdf.columns:
            if col != 'geometry':  # geometry cannot be dissolved
                yield Dissolve(gdf_name, by=col)


def bivariate(gdfs: GdfBindings) -> CandidateGen:
    ''' Generates all valid bivariate programs over `gdfs`

        Due to external implementations, ordering is pseudo-random
    '''
    # interleave results to attempt to maximize search space coverage
    merge_gen = merge(gdfs)
    sjoin_gen = sjoin(gdfs)
    # zip stops when one of its sources ends
    for m_candidate, s_candidate in zip(merge_gen, sjoin_gen):
        yield m_candidate
        yield s_candidate
    # yield whatever remains of whichever still has values left
    yield from sjoin_gen
    yield from merge_gen


def make_candidate_filter(gdfs: GdfBindings, target: DataFrame) -> Callable[[Candidate], bool]:
    'Returns a predicate that checks if a `program` over `gdfs` matches `target`'
    def _filter(program):
        try:
            assert_geodataframe_equal(program.interpret(gdfs), target, check_geom_type=True)
            return True
        except AssertionError:
            return False
    return _filter


def synth_matching(
    synthesizer: Callable[[GdfBindings], CandidateGen], 
    gdfs: GdfBindings, 
    target: DataFrame,
) -> CandidateGen:
    'An iterator over all programs over `gdfs` that match `target`'
    checker = make_candidate_filter(gdfs, target)
    return filter(checker, synthesizer(gdfs))


def lazy_synthesize(gdfs: dict[str, DataFrame], target: DataFrame) -> None | Candidate:
    ''' Generates the minimum number of programs to get the first that 
        matches `target` over `gdfs`
    '''
    if not isinstance(gdfs, GdfBindings):
        gdfs = GdfBindings(gdfs)
    out = next(synth_matching(program, gdfs, target), None)
    print(out or 'No program found!')
    return out

def lazy_synthesize_gen(gdfs: dict[str, DataFrame], target: DataFrame):
    ''' Generates the minimum number of programs to get the first that 
        matches `target` over `gdfs`
    '''
    if not isinstance(gdfs, GdfBindings):
        gdfs = GdfBindings(gdfs)

    checker = make_candidate_filter(gdfs, target)
    for count, candidate in enumerate(program(gdfs)):
        found = checker(candidate)
        yield [count, candidate, found]
        if found:
            break


def synthesize_all(gdfs: dict[str, DataFrame], target: DataFrame) -> list[Candidate]:
    ''' Generates all programs over `gdfs` that match `target`

        Warning: May be *exceedingly* slow! User beware!
    '''
    if not isinstance(gdfs, GdfBindings):
        gdfs = GdfBindings(gdfs)
    
    print('Synthesizing all may be very slow!')
    out = list()
    for p in synth_matching(program, gdfs, target):
        print('\t', p)
        out.append(p)
    print('Done.')
    return out


def binding_pairs(gdfs: GdfBindings):
    'Returns a generator over all unique (unordered) pairs of DataFrames in gdfs'
    return combinations(gdfs.items(), 2)


def merge(gdfs: GdfBindings) -> CandidateGen:
    ''' Generates all valid `pd.merge` programs over `gdfs`

        Due to external implementations, ordering is pseudo-random
    '''
    for (l_name, _), (r_name, _) in binding_pairs(gdfs):
        # get the dicts of dtype -> [ column_name ] that hold
        # the columns of each frame sorted by their dtype
        l_types = gdfs.get_cols_dtype(l_name)
        r_types = gdfs.get_cols_dtype(r_name)

        # for each type that both dicts have...
        for k, l_v in l_types.items():
            if r_v := r_types.get(k, None):
                # ... take every pair of mergeable columns and 
                # generate a Merge for it
                for l_col, r_col in product(l_v, r_v):
                    # Note: 'cross' throws weird exception...
                    for h in ("left", "right", "inner", "outer"):  
                        c = Merge(l_name, r_name, how=h, left_on=l_col, right_on=r_col)
                        yield c
                        yield c.dual()


def sjoin(gdfs: GdfBindings) -> CandidateGen:
    ''' Generates all valid `gpd.sjoin` programs over `gdfs`

        Due to external implementations, ordering is pseudo-random
    '''
    # for every combo of GeoDataFrames...
    for (l, l_frame), (r, r_frame) in binding_pairs(gdfs):
        if type(l_frame) == GeoDataFrame and type(r_frame) == GeoDataFrame:
            # get the valid query predicates...
            query_preds = l_frame.sindex.valid_query_predicates & \
                r_frame.sindex.valid_query_predicates
            # and generate SJoins
            for h in ("left", "right", "inner"):
                for p in query_preds:
                    c = SJoin(l, r, how=h, predicate=p)
                    yield c
                    yield c.dual()
