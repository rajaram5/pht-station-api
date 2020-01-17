from flask import Flask, request
import FDP_search
import Station_vocabulary

import rdflib
from rdflib import URIRef, Literal, XSD, RDF
import uuid

app = Flask(__name__)
app.config["fdpUrl"] = "https://demofdp1.fairdata.solutions/fdp"
app.config["useClauseEndpoint"] = "http://136.243.4.200:8892/repositories/use-clause"
app.config["locationEndpoint"] = "http://136.243.4.200:8892/repositories/vwdata-location"

FDP_SEARCH = FDP_search.FDP_search(app.config["fdpUrl"], app.config["useClauseEndpoint"],
                                   app.config["locationEndpoint"])

@app.route("/", methods=['GET'])
def home():
    return "Not implemented yet"


@app.route("/runTrain", methods=['POST'])
def run_train():
    return "runTrain method not implemented yet"

@app.route("/getDataAccessInterface", methods=['POST'])
def get_data_access_interface():
    return "Not implemented yet"


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

        graph = rdflib.Graph()
        graph.add((result_uri, Station_vocabulary.QUERY_MATCHES_PREDICATE,
                   Literal(len(datasets_matches_search), datatype=XSD.integer)))
        graph.add((result_uri, Station_vocabulary.USE_INTENTION_MATCHES_PREDICATE,
                   Literal(len(datasets_matches_use_conditions), datatype=XSD.integer)))
        graph.add((result_uri, Station_vocabulary.CONSENT_EXPIRY_DATE_MATCHES_PREDICATE,
                   Literal(len(datasets_matches_date_conditions), datatype=XSD.integer)))
        graph.add((result_uri, Station_vocabulary.LOCATION_CONDITION_MATCHES_PREDICATE,
                   Literal(len(datasets_matches_location_conditions), datatype=XSD.integer)))
        graph.add((result_uri, Station_vocabulary.DATASET_AVAILABLE_PREDICATE,
                   Literal(len(datasets_matches_date_conditions), datatype=XSD.integer)))

        for dataset in datasets_matches_date_conditions:
            graph.add((result_uri, RDF.value, URIRef(dataset)))

    return graph.serialize(format='n3');

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) #run app in debug mode on port 5000