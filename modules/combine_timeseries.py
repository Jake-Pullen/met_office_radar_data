import pandas as pd


class CombineTimeseries:
    def __init__(self, config, locations):
        self.config = config
        self.locations = locations
        self.grouped_locations = {}
        self.build_location_groups()

    def build_location_groups(self):
        for location in self.locations:
            group = location[4]  # output group is at index 4
            if group not in self.grouped_locations:
                self.grouped_locations[group] = []
            self.grouped_locations[group].append(location)

    def combine_csv_files(self):
        for group, loc_list in self.grouped_locations.items():
            combined_df = None
            for loc in loc_list:
                csv_to_load = f"./csv_files/{loc[0]}_timeseries_data.csv"
                df = pd.read_csv(csv_to_load, index_col=0)
                if combined_df is None:
                    combined_df = df
                else:
                    combined_df = combined_df.join(df, how="inner")
            output_file = (
                f"{self.config.COMBINED_FOLDER}/group_{group}_timeseries_data.csv"
            )
            combined_df.to_csv(output_file)
