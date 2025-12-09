import polars as pd
import os
import logging

class CombineTimeseries:
    def __init__(self, config, locations):
        self.config = config
        self.locations = locations
        self.grouped_locations = {}
        self.build_location_groups()

    def build_location_groups(self):
        for location in self.locations:
            group = location[3]  # zone number
            if group not in self.grouped_locations:
                self.grouped_locations[group] = []
            self.grouped_locations[group].append(location)
        logging.info(f'Count of zones: {len(self.grouped_locations)}')

    # def combine_csv_files(self):
    #     to_delete = []
    #     for group, loc_list in self.grouped_locations.items():
    #         output_file =f"{self.config.COMBINED_FOLDER}/zone_{group}_timeseries_data.csv"
    #         combined_df = None
    #         for loc in loc_list:
    #             csv_to_load = f"{self.config.CSV_TOP_FOLDER}/{loc[0]}_timeseries_data.csv"
    #             df = pd.read_csv(csv_to_load, streaming=True)
    #             if combined_df is None:
    #                 combined_df = df
    #             else:
    #                 combined_df = combined_df.join(df, on='datetime')

    #             if self.config.delete_csv_after_combining:
    #                 to_delete.append(csv_to_load)

    #         sorted_df = combined_df.sort('datetime')
    #         print(f'writing file to {output_file}')
    #         sorted_df.write_csv(output_file)

    #     if len(to_delete) > 0:
    #         for path in to_delete:
    #             print(f'deleting {path}')
    #             os.remove(path)

    def combine_csv_files(self):  
        to_delete = []  
        for group, loc_list in self.grouped_locations.items():  
            output_file = f"{self.config.COMBINED_FOLDER}/zone_{group}_timeseries_data.csv"  
            
            # Use LazyFrame for memory-efficient processing
            lazy_dfs = []
            for loc in loc_list:  
                csv_to_load = f"{self.config.CSV_TOP_FOLDER}/{loc[0]}_timeseries_data.csv"  
                df = pd.scan_csv(csv_to_load)  # Lazy read
                lazy_dfs.append(df)

                if self.config.delete_csv_after_combining:
                    to_delete.append(csv_to_load)

            # Combine with LazyFrame operations
            combined_lazy = pd.concat(lazy_dfs, how='align').collect(streaming=True)  # Collect at the end

            sorted_df = combined_lazy.sort('datetime')
            print(f'writing file to {output_file}')  
            sorted_df.write_csv(output_file)  
    
        if len(to_delete) > 0:  
            for path in to_delete:  
                print(f'deleting {path}')  
                os.remove(path)