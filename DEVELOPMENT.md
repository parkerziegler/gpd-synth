# Local Development

Follow [the instructions](https://geopandas.org/en/stable/getting_started/install.html#creating-a-new-environment) for installing `geopandas` in a local [`conda`](https://www.anaconda.com/) environment.

```sh
conda create -n geo_env
conda activate geo_env
conda config --env --add channels conda-forge
conda config --env --set channel_priority strict
conda install python=3 geopandas
```

Once you have `geopandas` installed, also ensure you have `jupyterlab` installed in the same `conda` environment.

```sh
conda install jupyterlab
```

You should now be able to start up Jupyter Lab and compose a notebook using `geopandas`!

```sh
jupyter lab
```
