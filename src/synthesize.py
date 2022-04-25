from geopandas.testing import assert_geodataframe_equal

from pandas import DataFrame
from geopandas import GeoDataFrame
from typing import Callable, Generator, TypeAlias
from itertools import product, combinations

from grammar import GrammarRule, Merge, SJoin, GDF, Dissolve
from synth_input import GdfBindings


CandidateGen:    TypeAlias = Generator[GrammarRule, None, None]
LazySynthesizer: TypeAlias = Callable[[GdfBindings], CandidateGen]


# grow applies every non-terminal in the grammar to every possible combination of
# subexpressions drawn from program_list.
def grow(program_list):
    expansion = []
    # Our grammar needs to have nonterminals in order for this to be relevant!

    return expansion


# Evaluate a candidate expression given the dictionary of input GeoDataFrames.
# This is the core function responsible for running a synthesized program on our inputs.
def evaluate_program(program, input_gdfs):
    return program.interpret(input_gdfs)


# Check whether two GeoDataFrames are equivalent.
def gdfs_equal(left, right):
    try:
        assert_geodataframe_equal(left, right, check_geom_type=True)

        return True
    except:
        return False


# Check whether a candidate program is observationally equivalent to an already
# synthesized program.
def already_accounted_for(program_list, program, input_gdfs):
    output = evaluate_program(program, input_gdfs)

    for existing_program in program_list:
        if gdfs_equal(output, evaluate_program(existing_program, input_gdfs)):
            return True

    return False


# Eliminate all programs that already have an observational equivalent in our candidate program_list.
def elim_equivalents(program_list, input_gdfs):
    pruned_list = []

    for program in program_list:
        if already_accounted_for(pruned_list, program, input_gdfs):
            continue

        pruned_list.append(program)

    return pruned_list


# The core synthesis function. We use enumerative synthesis with observational equivalence pruning.
#
# In this algorithm, we're recursively building up larger and larger programs
# based on our grammar, eargely pruning those that behave identically on all inputs.
def synthesize(gdfs, target):
    program_list = []

    for gdf_name in gdfs.keys():
        program_list.append(GDF(gdf_name))

    for gdf_name, gdf in gdfs.items():
        for col in gdf.columns:
            if col != "geometry":
                program_list.append(Dissolve(gdf_name, col))

    # Binary operations here.

    print(
        "Candidates\n",
        "\n".join([str(program) for program in program_list]),
    )

    program_list = elim_equivalents(program_list, gdfs)

    # grow will go here if we end up having non-terminals.

    for program in program_list:
        if gdfs_equal(evaluate_program(program, gdfs), target):
            print("\nFound it!", program)
            return program

    print("No program found!")


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


def make_candidate_filter(gdfs: GdfBindings, target: DataFrame):
    'Returns a predicate that checks if a `program` over `gdfs` matches `target`'
    def candidate_filter(program: DataFrame) -> bool:
        return gdfs_equal(evaluate_program(program, gdfs), target)
    return candidate_filter


def lazy_synth(gdfs: GdfBindings, synthesizer: LazySynthesizer, target: DataFrame) -> CandidateGen:
    'An iterator over all programs over `gdfs` that match `target`'
    checker = make_candidate_filter(gdfs, target)
    return filter(checker, synthesizer(gdfs))


def lazy_synthesize(gdfs: GdfBindings, target: DataFrame) -> None:
    'Lazy counterpart to `synthesize`'
    print(next(lazy_synth(gdfs, univariate, target), 'No program found!'))


def binding_pairs(gdfs: GdfBindings):
    yield from combinations(gdfs.items(), 2)


def merge(gdfs: GdfBindings):
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
    for (l, l_frame), (r, r_frame) in binding_pairs(gdfs):
        if type(l_frame) == GeoDataFrame and type(r_frame) == GeoDataFrame:
            query_preds = l_frame.sindex.valid_query_predicates & \
                r_frame.sindex.valid_query_predicates
            for h in ("left", "right", "inner"):
                for p in query_preds:
                    yield SJoin(l, r, how=h, predicate=p)
