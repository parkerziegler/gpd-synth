import geopandas as gpd

# The grammar for our synthesis engine.
#
# Each part of the grammar is represented as a separate class with __init__, __repr__, and interpret methods.
# __repr__ provides the string representation to use to display the synthesized program.
# interpret provides the actual code to execute behind the scenes when evaluating the synthesized program.
class GDF:
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return f"{self.name}"

    def interpret(self, gdfs):
        return gdfs[self.name]


class Dissolve:
    def __init__(self, gdf, by):
        self.gdf = gdf
        self.by = by

    def __repr__(self):
        return str(self.gdf) + ".dissolve(by=" + f'"{self.by}"' + ")"

    def interpret(self, gdfs):
        gdf = self.gdf.interpret(gdfs)
        return gdf.dissolve(by=self.by)


class SJoin:
    def __init__(self, left, right):
        self.left = left
        self.right = right

    def __repr__(self):
        return "gpd.sjoin(" + str(self.left) + ", " + str(self.right) + ")"

    def interpret(self, gdfs):
        return gpd.sjoin(gdfs[self.left], gdfs[self.right])
