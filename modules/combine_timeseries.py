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
