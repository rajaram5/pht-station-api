from rdflib import URIRef, Literal
import rdflib
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.plugins.sparql import prepareQuery

class Conditions:

    # specify optional data use conditions at each FDP level
    repo_conditions = [(None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                            URIRef('http://www.re3data.org/schema/3-0#Repository'))]


    catalog_conditions = [(None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                    URIRef('http://www.w3.org/ns/dcat#Catalog'))]


    dataset_conditions = [(None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                    URIRef('http://www.w3.org/ns/dcat#Dataset'))]

    distribution_conditions = [(None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                    URIRef('http://www.w3.org/ns/dcat#Distribution'))]

    use_clause_endpoint = None
    def __init__(self, use_endpoint):
        self.use_clause_endpoint = use_endpoint

    def getTrainDatasetUseIntention(self, metadata):

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
                for url in self._get_use_intention_class_mappings(class_url):
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


    def _get_use_intention_class_mappings(self, class_url):

        class_urls = [class_url]

        if self.use_clause_endpoint:
            sparql = SPARQLWrapper(self.use_clause_endpoint)

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



    def getTrainDatasetRequestConditions(self, metadata):
        graph = rdflib.Graph()
        graph.parse(data=metadata, format="text/turtle")
        # Get conditions from train metadata
        rep_conditions = self._getRepositoryTrainRequestConditions(graph)
        cat_conditions = self._getCatlogTrainRequestConditions(graph)
        data_conditions = self._getDatasetTrainRequestConditions(graph)
        return [rep_conditions, cat_conditions, data_conditions]


    def getTrainDistributionRequestConditions(self, metadata):
        graph = rdflib.Graph()
        graph.parse(data=metadata, format="text/turtle")
        # Get conditions from train metadata
        dist_conditions = self._getDistributionTrainRequestConditions(graph)
        return [dist_conditions]


    def _getRepositoryTrainRequestConditions(self, graph):
        rep_conditions = self.repo_conditions
        return rep_conditions

    def _getCatlogTrainRequestConditions(self, graph):
        cat_conditions = self.catalog_conditions
        return cat_conditions

    def _getDatasetTrainRequestConditions(self, graph):
        data_conditions = self.dataset_conditions
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

    def _getDistributionTrainRequestConditions(self, graph):
        dist_conditions = self.distribution_conditions
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

