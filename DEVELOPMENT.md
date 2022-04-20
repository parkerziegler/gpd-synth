# Local Development

## Installation with `conda`

`gpd-synth` assumes you have `conda` installed. If you don't, follow the installation instructions on the [`conda`](https://www.anaconda.com/) website. We use `conda` in lieu of `pip` to align with the official [`geopandas`](https://geopandas.org/en/stable/getting_started/install.html) recommendation.

Once you have `conda` installed, you can install required dependencies and create a `conda` environment like so:

```sh
conda create --name <your_env_name> --file requirements.txt
```

With this environment created, you can activate it at any time using the following command:

```sh
conda activate <your_env_name>
```

With dependencies installed and your `conda` environment activated, you're ready to start working on `gpd-synth`! You can also spin up Jupyter Lab and compose a new notebook in the `examples` directory using the following command:

```sh
jupyter lab
```

Happy hacking!
