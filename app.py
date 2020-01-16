from flask import Flask, request
import FDP_search

import rdflib
from rdflib import URIRef, Literal, XSD
import uuid

app = Flask(__name__)
app.config["fdpUrl"] = "https://demofdp1.fairdata.solutions/fdp"
app.config["useClauseEndpoint"] = "http://136.243.4.200:8892/repositories/use-clause"
app.config["locationEndpoint"] = "http://136.243.4.200:8892/repositories/vwdata-location"

FDP_SEARCH = FDP_search.FDP_search(app.config["fdpUrl"], app.config["useClauseEndpoint"], app.config["locationEndpoint"])

@app.route("/", methods=['GET'])
def home():
    return "Not implemented yet"


@app.route("/runTrain", methods=['POST'])
def run_train():
    return "runTrain method not implemented yet"

@app.route("/getDataAccessInterface", methods=['POST'])
def get_data_access_interface():
    payload = request.get_data()
    if payload:
        return "a"
        # metadata = payload.decode('utf-8')
        # print("metadata : " + metadata)
        # fdp_uri = app.config["fdpUrl"]
        # print("fdp uri : " + fdp_uri)
        # # Init crawler
        # crawler = FDP_SPARQL_crawler.FDP_SPARQL_crawler()
        # # Init Conditions
        # cond = Conditions.Conditions(app.config["useClauseEndpoint"])
        # # Get dataset conditions
        # dataset_conditions = cond.get_train_dataset_request(metadata)
        # datasets = crawler.get_dataset(URIRef(fdp_uri), dataset_conditions)
        # print(datasets)
        #
        # use_conditions = cond.get_train_use_intention(metadata)
        # distribution_conditions = cond.get_train_distribution_request(metadata)
        #
        #
        #
        # graph = rdflib.Graph()
        #
        # for dataset in datasets:
        #
        #
        #     dataset_conditions = use_conditions[:]
        #     distribution_interface_conditions = distribution_conditions[:]
        #     if crawler.does_useclause_matches(dataset, dataset_conditions):
        #         distributions = crawler.get_distributions(dataset, distribution_interface_conditions)
        #
        #         # TODO check use of template lib here
        #         for access_url, access_predicate in distributions.items():
        #             graph.add( (URIRef(dataset), URIRef(access_predicate), URIRef(access_url)) )
        #     else:
        #         err_msg = "No access interface found. The dataset and train use conditions are different"
        #         graph.add((URIRef(dataset), URIRef("http://purl.org/dc/terms/description"), Literal(err_msg)))
    #
    # return graph.serialize(format='n3');


@app.route("/getDataset", methods=['POST'])
def get_dataset():
    payload = request.get_data()
    if payload:
        metadata = payload.decode('utf-8')
        print("metadata : " + metadata)

        datasets_matches_search = FDP_SEARCH.get_dataset_matches_search_query(metadata)
        datasets_matches_use_conditions = FDP_SEARCH.get_dataset_matches_use_conditons(datasets_matches_search,
                                                                                       metadata)
        datasets_matches_location_conditions = FDP_SEARCH.get_dataset_matches_location_conditons\
                (datasets_matches_use_conditions, metadata)
        datasets_matches_date_conditions = FDP_SEARCH.get_dataset_matches_date_conditons\
            (datasets_matches_location_conditions)

        result_uri = URIRef("http://rdf.biosemantics.org/resources/pht-station/dataset/search/result/"
                            + str(uuid.uuid4()))

        dataset_maches = URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/numberOfDatasetMachesQuery")
        dataset_maches_useconditon = \
            URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/numberOfDatasetMachesUseConditions")

        dataset_maches_location_conditon = \
            URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/numberOfDatasetMachesLocationConditions")

        dataset_maches_date_conditon = \
            URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/numberOfDatasetMachesConsentExpirtDate")

        rdf_value = URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#value')

        graph = rdflib.Graph()
        graph.add((result_uri, dataset_maches, Literal(len(datasets_matches_search), datatype=XSD.integer)))
        graph.add((result_uri, dataset_maches_useconditon,
                   Literal(len(datasets_matches_use_conditions), datatype=XSD.integer)))
        graph.add((result_uri, dataset_maches_location_conditon,
                   Literal(len(datasets_matches_location_conditions), datatype=XSD.integer)))
        graph.add((result_uri, dataset_maches_date_conditon,
                   Literal(len(datasets_matches_date_conditions), datatype=XSD.integer)))

        for dataset in datasets_matches_date_conditions:
            graph.add((result_uri, rdf_value, URIRef(dataset)))

    return graph.serialize(format='n3');

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) #run app in debug mode on port 5000