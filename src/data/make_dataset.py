""" main data pipeline for transforming the wine dataset """
from pathlib import Path
from metaflow import FlowSpec, step, IncludeFile

PROJECT_DIR = Path(__file__).resolve().parents[2]


class MushroomFlow(FlowSpec):
    """
    A flow to read in the mushroom dataset -- see src.data.download_raw.py
    https://archive.ics.uci.edu/ml/datasets/mushrooms
    """

    raw_data_path = PROJECT_DIR / "data" / "raw" / "mushrooms.csv"
    raw_mushroom_data = IncludeFile(
        "mushroom_data",
        help="Mushroom records drawn from The Audubon Society Field Guide to North American Mushrooms (1981).",
        default=raw_data_path,
    )

    @step
    def start(self):
        """
        Read data/raw/mushrooms.csv into a pandas df
        """
        import pandas as pd
        from io import StringIO

        self.dataframe = pd.read_csv(StringIO(self.raw_mushroom_data))
        self.next(self.rename_cols)

    @step
    def rename_cols(self):
        """
        Give the mushroom data some computer friendlier column names
        Save to "data/interim/mushroom_nice_cols.csv"
        """
        self.dataframe.columns = [
            col.lower().replace(" ", "_") for col in self.dataframe.columns
        ]

        save_path = PROJECT_DIR / "data" / "interim" / "mushroom_nice_cols.csv"
        self.dataframe.to_csv(save_path, index=False)

        self.next(self.one_hot_encode)
    
    @step
    def one_hot_encode(self):
        """
        Convert the categorical columns to one hot encoding.
        We'll do it by hand this time.
        """
        # col 1 = class
        for col in self.dataframe.columns[1:]:
            possible_vals = self.dataframe[col].unique()
            for v in possible_vals:
                self.dataframe[f"{col}__{v}"] = self.dataframe[col].apply(lambda x: 1 if x == v else 0)
            self.dataframe = self.dataframe.drop(col, axis="columns")

        save_path = PROJECT_DIR / "data" / "interim" / "mushroom_one_hot.csv"
        self.dataframe.to_csv(save_path, index=False)
        self.next(self.end)

    @step
    def end(self):
        """
        Save the final version to "data/processed/mushroom_final.csv"
        End the flow.
        """
        save_path = PROJECT_DIR / "data" / "processed" / "mushroom_final.csv"
        self.dataframe.to_csv(save_path, index=False)


if __name__ == "__main__":
    MushroomFlow()