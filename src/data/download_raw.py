import logging
from pathlib import Path
import pandas as pd


def download_raw() -> pd.DataFrame:
    """downloads the mushroom  dataset from https://archive.ics.uci.edu/ml/machine-learning-databases/mushroom/
    
    Returns:
        pd.DataFrame -- raw agaricus-lepiota.data with column names
    """
    logger = logging.getLogger(__name__)

    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/mushroom/"

    col_names = [
        "class",
        "cap shape",
        "cap surface",
        "cap color",
        "bruised",
        "odor",
        "gill attachment",
        "gill spacing",
        "gill size",
        "gill color",
        "stalk shape",
        "stalk root",
        "stalk surface above ring",
        "stalk surface below ring",
        "stalk color above ring",
        "stalk color below ring",
        "veil type",
        "veil color",
        "ring number",
        "ring type",
        "spore print color",
        "population",
        "habitat",
    ]
    logger.info(f"downloading dataset from {url}")
    return pd.read_csv(url + "agaricus-lepiota.data", header=None, names=col_names)


if __name__ == "__main__":
    log_fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    logging.basicConfig(level=logging.INFO, format=log_fmt)

    project_dir = Path(__file__).resolve().parents[2]
    df = download_raw()
    df.to_csv(project_dir / "data" / "raw" / "mushrooms.csv", index=False)
