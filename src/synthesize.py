from geopandas.testing import assert_geodataframe_equal

from grammar import GDF, Dissolve


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
                program_list.append(Dissolve(GDF(gdf_name), col))

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


from pandas import DataFrame
from geopandas import GeoDataFrame
from typing import Callable, Generator
from grammar import GrammarRule

GdfBindings = dict[str, DataFrame]
CandidateGen = Generator[GrammarRule, None, None]
LazySynthesizer = Callable[[GdfBindings], CandidateGen]

def lazy_univariate(gdfs: GdfBindings) -> CandidateGen:
    'Generates the simplest programs first, including equivalents.'
    for gdf_name in gdfs.keys():
        yield GDF(gdf_name)

    for gdf_name, gdf in gdfs.items():
        for col in gdf.columns:
            if col != 'geometry':
                yield Dissolve(GDF(gdf_name), col)


def make_candidate_filter(gdfs: GdfBindings, target: DataFrame):
    'Returns a predicate that checks if a `program` over `gdfs` matches `target`'
    def candidate_filter(program: DataFrame) -> bool:
        return gdfs_equal(evaluate_program(program, gdfs), target)
    return candidate_filter

def lazy_synth(gdfs: GdfBindings, synthesizer: LazySynthesizer, target: DataFrame):
    'An iterator over all programs over `gdfs` that match `target`'
    checker = make_candidate_filter(gdfs, target)
    return filter(checker, synthesizer(gdfs))

def lazy_synthesize(gdfs: GdfBindings, target: DataFrame):
    'Lazy counterpart to `synthesize`'
    print(next(lazy_synth(gdfs, lazy_univariate, target), 'No program found!'))
