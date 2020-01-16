from rdflib import URIRef, Literal
import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.plugins.sparql import prepareQuery

class Conditions:

    # specify optional data use conditions at each FDP level
    REPO_CONDITIONS = [(None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                        URIRef('http://www.re3data.org/schema/3-0#Repository'))]


    CATALOG_CONDITIONS = [(None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                           URIRef('http://www.w3.org/ns/dcat#Catalog'))]


    DATASET_CONDITIONS = [(None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                           URIRef('http://www.w3.org/ns/dcat#Dataset'))]

    DISTRIBUTION_CONDITIONS = [(None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                                URIRef('http://www.w3.org/ns/dcat#Distribution'))]

    USE_CLAUSE_ENDPOINT = None

    def __init__(self, use_endpoint):
        self.USE_CLAUSE_ENDPOINT = use_endpoint

    def get_train_use_intention(self, metadata):

        use_conditions = []

        graph = rdflib.Graph()
        graph.parse(data=metadata, format="text/turtle")
        # Read train requirement query
        query = open('queries/get_train_use_intention.rq', 'r').read()
        q = prepareQuery(query)

        for row in graph.query(q):
            print(row)
            if row[0]:
                class_url = str(row[0])
                for url in self._get_use_intention_class_mapping(class_url):
                    condition = (None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'), URIRef(url))
                    use_conditions.append(condition)

        print(use_conditions)
        return use_conditions

    def get_train_dataset_location(self, metadata):

        conditions = []

        graph = rdflib.Graph()
        graph.parse(data=metadata, format="text/turtle")
        # Read train origin query
        query = open('queries/get_train_location_use_intention.rq', 'r').read()
        q = prepareQuery(query)

        for row in graph.query(q):
            print(row)
            if row[0]:
                location_url = str(row[0])
                condition = (None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#value'), URIRef(location_url))
                conditions.append(condition)
        print(conditions)
        return conditions


    def _get_use_intention_class_mapping(self, class_url):

        class_urls = [class_url]

        if self.USE_CLAUSE_ENDPOINT:
            sparql = SPARQLWrapper(self.USE_CLAUSE_ENDPOINT)

            # Run get patient count query
            query = open('queries/get_use_intention_class_mappings.rq', 'r').read()

            query = query.replace("CLASS_URL", class_url)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            for result in results["results"]["bindings"]:
                if result["useIntentionMappings"]["value"]:
                    class_url = result["useIntentionMappings"]["value"]
                    print(str(class_url))
                    class_urls.append(class_url)
        else:
            print("Config mapping endpoint")

        return class_urls



    def get_train_dataset_request(self, metadata):
        graph = rdflib.Graph()
        graph.parse(data=metadata, format="text/turtle")
        # Get conditions from train metadata
        rep_conditions = self._get_repository_train_request(graph)
        cat_conditions = self._get_catalog_train_request(graph)
        data_conditions = self._get_dataset_train_request(graph)
        return [rep_conditions, cat_conditions, data_conditions]


    def get_train_distribution_request(self, metadata):
        graph = rdflib.Graph()
        graph.parse(data=metadata, format="text/turtle")
        # Get conditions from train metadata
        dist_conditions = self._get_distribution_train_request(graph)
        return [dist_conditions]


    def _get_repository_train_request(self, graph):
        rep_conditions = self.REPO_CONDITIONS
        return rep_conditions

    def _get_catalog_train_request(self, graph):
        cat_conditions = self.CATALOG_CONDITIONS
        return cat_conditions

    def _get_dataset_train_request(self, graph):
        data_conditions = self.DATASET_CONDITIONS
        # Read train requirement query
        query = open('queries/get_train_requirements.rq', 'r').read()
        q = prepareQuery(query)

        dataset_type = rdflib.URIRef("http://www.w3.org/ns/dcat#Dataset")

        for row in graph.query(q, initBindings={'isAbout': dataset_type}):
            print(row)
            if row[0] == dataset_type:
                requirementPredicate = row[1]
                requirementObject = row[2]
                condition = (None, requirementPredicate, requirementObject)
                data_conditions.append(condition)

        print(data_conditions)
        return data_conditions

    def _get_distribution_train_request(self, graph):
        dist_conditions = self.DISTRIBUTION_CONDITIONS
        # Read train requirement query
        query = open('queries/get_train_requirements.rq', 'r').read()
        q = prepareQuery(query)

        distribution_type = rdflib.URIRef("http://www.w3.org/ns/dcat#Distribution")


        sparql_endpoint = rdflib.URIRef("http://www.example.com/train-ontology-placeholder/sparqlEndpoint")
        train_data_access_predicate = rdflib.URIRef\
            ("http://www.example.com/train-ontology-placeholder/dataAccessInterface")

        for row in graph.query(q, initBindings={'isAbout': distribution_type}):
            print(row)
            if row[0] == distribution_type:
                requirementPredicate = row[1]
                requirementObject = row[2]
                if ((requirementPredicate == train_data_access_predicate) and (requirementObject == sparql_endpoint)):
                    condition = (None, URIRef('http://www.w3.org/ns/dcat#accessURL'), None)
                    dist_conditions.append(condition)
                    condition = (None, URIRef('http://www.w3.org/ns/dcat#mediaType'), Literal('text/turtle'))
                    dist_conditions.append(condition)

        print(dist_conditions)
        return dist_conditions

