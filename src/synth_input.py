from pandas import DataFrame
from collections import defaultdict


ColumnTypes = dict[type, set[str]]


def cols_by_dtype(frame: DataFrame) -> ColumnTypes:
    out = defaultdict(set)
    for k, v in dict(frame.dtypes).items():
        out[v].add(k)
    return dict(out)


class GdfBindings(dict[str, DataFrame]):
    def __init__(self):
        super().__init__()
        self.frame_col_dtypes: dict[str, ColumnTypes] = dict()

    def get_cols_dtype(self, name: str) -> ColumnTypes:
        frame = self[name]
        if coltypes := frame.get(name, None):
            return coltypes
        out = cols_by_dtype(frame)
        self.frame_col_dtypes[name] = out
        return out
