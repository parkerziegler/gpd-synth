import pandas as pd
import geopandas as gpd

from typing import Protocol
from dataclasses import KW_ONLY, dataclass

from pandas import DataFrame


# The grammar for our synthesis engine.
#
# Each part of the grammar is represented as a separate class with __init__, __repr__, and interpret methods.
# __repr__ provides the string representation to use to display the synthesized program.
# interpret provides the actual code to execute behind the scenes when evaluating the synthesized program.


class GrammarRule(Protocol):
    def interpret(self, gdfs: dict[str, 'GrammarRule']) -> gpd.GeoDataFrame:
        raise NotImplementedError('Not Yet Implemented')
    
    def __repr__(self) -> str:
        raise NotImplementedError('Not Yet Implemented')


# Optimization Note: On the use of @dataclass
#   Dataclasses are about 5x faster to instantiate than
#   standard classes, but about 1.25x slower to read attributes from.
#   
#   For our purposes, we are generating many, many instances, and accessing
#   each field once. Also, instantiation in any layout takes far longer than
#   reading attributes from any layout. 


@dataclass(frozen=True, repr=False)
class GDF(GrammarRule):
    name: str

    def __repr__(self):
        return self.name

    def interpret(self, gdfs):
        return gdfs[self.name]


@dataclass(frozen=True, repr=False)
class Dissolve(GrammarRule):
    gdf: GDF
    by: str

    def __repr__(self):
        return f'{self.gdf}.dissolve(by="{self.by}")'

    def interpret(self, gdfs):
        gdf = self.gdf.interpret(gdfs)
        return gdf.dissolve(by=self.by)


@dataclass(frozen=True, repr=False)
class SJoin(GrammarRule):
    left: str
    right: str
    _: KW_ONLY
    how: str
    predicate: str

    def __repr__(self):
        return f"gpd.sjoin({self.left}, {self.right}, \
            how={self.how}, predicate={self.predicate})"

    def interpret(self, gdfs):
        return gpd.sjoin(gdfs[self.left], gdfs[self.right], \
            how=self.how, predicate=self.predicate)


@dataclass(frozen=True, repr=False)
class Merge(GrammarRule):
    left: str
    right: str
    _: KW_ONLY
    how: str
    left_on: str
    right_on: str

    def __repr__(self) -> str:
        return f'pd.merge({self.left}, {self.right}, how={self.how}, \
                left_on={self.left_on}, right_on={self.right_on})'
    
    def interpret(self, gdfs: dict[str, 'GrammarRule']) -> DataFrame:
        return pd.merge(gdfs[self.left], gdfs[self.right], how=self.how, \
                left_on=self.left_on, right_on=self.right_on)
