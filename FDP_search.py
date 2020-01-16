import FDP_SPARQL_crawler
import Conditions

"""
Interface to search FDP content
"""

class FDP_search:
    # Init crawler
    CRAWLER = None
    # Init Conditions
    CONDITIONS = None

    def __init__(self, fdp_url, use_clause_endpoint, location_endpoint):
        self.CRAWLER = FDP_SPARQL_crawler.FDP_SPARQL_crawler(fdp_url, location_endpoint)
        self.CONDITIONS = Conditions.Conditions(use_clause_endpoint)

    def get_dataset_matches_search_query(self, metadata):
        # Get dataset conditions
        dataset_conditions = self.CONDITIONS.get_train_dataset_request(metadata)
        datasets_matches_search = self.CRAWLER.get_dataset(dataset_conditions)
        print(datasets_matches_search)
        return datasets_matches_search

    def get_dataset_matches_use_conditons(self, datasets_matches_search, metadata):
        use_conditions = self.CONDITIONS.get_train_use_intention(metadata)
        datasets_matches_use_conditions = []

        for dataset in datasets_matches_search:
            dataset_conditions = use_conditions[:]
            if self.CRAWLER.does_useclause_matches(dataset, dataset_conditions):
                datasets_matches_use_conditions.append(dataset)

        return datasets_matches_use_conditions

    def get_dataset_matches_location_conditons(self, datasets, metadata):
        use_conditions = self.CONDITIONS.get_train_dataset_location(metadata)
        datasets_matches_location_conditions = []

        for dataset in datasets:
            dataset_conditions = use_conditions[:]
            if self.CRAWLER.does_uselocation_matches(dataset, dataset_conditions):
                datasets_matches_location_conditions.append(dataset)

        return datasets_matches_location_conditions

    def get_dataset_matches_date_conditons(self, datasets):
        datasets_matches_date_conditions = []

        for dataset in datasets:
            if self.CRAWLER.does_dates_matches(dataset):
                datasets_matches_date_conditions.append(dataset)

        return datasets_matches_date_conditions