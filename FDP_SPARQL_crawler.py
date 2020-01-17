import rdflib
from rdflib import RDF, URIRef
from SPARQLWrapper import SPARQLWrapper, JSON
from datetime import datetime

"""
TODO:
refactoring and testing
proper logging rather than stdout prints
generalize to other end-point type or other RDF distribution types

LIMITATIONS:
returns only a single end-point even if multiple are available
"""

class FDP_SPARQL_crawler:

    # FDP semantics (alternative implementation: crawl any link for dcat:Dataset)
    FDP_ROUTE = ['http://www.re3data.org/schema/3-0#dataCatalog',
                 'http://www.w3.org/ns/dcat#dataset',
                 'http://www.w3.org/ns/dcat#distribution']


    LOCATION_ENDPOINT = None
    FDP_URL = None

    def __init__(self, fdp_url, location_endpoint):
        self.FDP_URL = fdp_url
        self.LOCATION_ENDPOINT = location_endpoint


    def test_sparql_access(self, urls):
        """Return the first of the urls that gives a SPARQL response """
        sparql = SPARQLWrapper(str(urls[0]))
        sparql.setQuery("select * where {?s ?p ?o} limit 10")
        try:
            if 'application/sparql-results+xml' in sparql.query().info()['content-type']:
                print('found SPARQL end point ' + str(urls[0]))
                return str(urls[0])
        except:
            return None
        return None


    def get_distribution(self, urls, conditions):
        """Apply a minimal set of FDP/DCAT semantics to crawl through a FDP and
        find and return any available SPARQL end-points."""

        graph = rdflib.Graph()
        # Load distribution content to graph
        for url in urls:
            graph = self._get_graph(url)

        # Get childern layers as graphs
        graphs = self._get_object_graphs(graph, "http://www.w3.org/ns/dcat#distribution")
        condition = conditions.pop(0)

        # Empty gobal graph
        graph.remove((None, None, None))
        for g in graphs:
            if self._does_graph_matches_conditions(g, condition):
                # Add content of childern layer which matches use condition
                    graph =  graph + g
            else:
                print('mismatched condition')
                g.close()

        distributions = {}

        """
        Get endpoint url from the gobal graph. Note dat we return the last endpoint. In case of
        multiple endpoints URLs we ignore the rest
        """
        for sub, pred, obj in graph.triples( (None,  URIRef('http://www.w3.org/ns/dcat#accessURL'), None) ):
            dataset = list(graph.objects(sub,  URIRef('http://purl.org/dc/terms/isPartOf')))
            if dataset[0] and obj:
                distributions[str(dataset[0])] = str(obj)

        for sub, pred, obj in graph.triples( (None,  URIRef('http://www.w3.org/ns/dcat#downloadURL'), None) ):
            dataset = list(graph.objects(sub,  URIRef('http://purl.org/dc/terms/isPartOf')))
            if dataset[0] and obj:
                distributions[str(dataset[0])] = str(obj)

        graph.close()
        return distributions



    def get_distributions(self, dataset_url, conditions):
        """Apply a minimal set of FDP/DCAT semantics to crawl through a FDP and
        find and return any available SPARQL end-points."""

        graph = rdflib.Graph()
        # Load distribution content to graph
        graph = self._get_graph(dataset_url)

        # Get childern layers as graphs
        graphs = self._get_object_graphs(graph, "http://www.w3.org/ns/dcat#distribution")
        condition = conditions.pop(0)

        # Empty gobal graph
        graph.remove((None, None, None))
        for g in graphs:
            if self._does_graph_matches_conditions(g, condition):
                # Add content of childern layer which matches use condition
                    graph =  graph + g
            else:
                print('mismatched condition')
                g.close()

        distributions = {}

        """
        Get endpoint url from the gobal graph. Note dat we return the last endpoint. In case of
        multiple endpoints URLs we ignore the rest
        """
        for sub, pred, obj in graph.triples( (None,  URIRef('http://www.w3.org/ns/dcat#accessURL'), None) ):
            dataset = list(graph.objects(sub,  URIRef('http://purl.org/dc/terms/isPartOf')))
            if dataset[0] and obj:
                distributions[str(obj)] = 'http://www.w3.org/ns/dcat#accessURL'

        for sub, pred, obj in graph.triples( (None,  URIRef('http://www.w3.org/ns/dcat#downloadURL'), None) ):
            dataset = list(graph.objects(sub,  URIRef('http://purl.org/dc/terms/isPartOf')))
            if dataset[0] and obj:
                distributions[str(obj)] = 'http://www.w3.org/ns/dcat#downloadURL'

        graph.close()
        return distributions



    def does_useclause_matches(self, dataset, trainuseconditions):

        graph = self._get_graph(dataset)

        for condition in trainuseconditions:
            # Get triples matches condition
            c_list = list(graph.triples(condition))

            if len(c_list) == 0:
                print('mismatched condition')
            else:
                return True

        return False


    def does_uselocation_matches(self, dataset, train_location_conditions):

        graph = self._get_graph(dataset)

        location_condition_in_ds = (None, URIRef('http://www.w3.org/1999/02/22-rdf-syntax-ns#type'),
                                    URIRef('http://purl.obolibrary.org/obo/DUO_0000022'))


        # Check if dataset has geographical restriction
        c_list = list(graph.triples(location_condition_in_ds))

        '''
        if no geographical restriction triples found in the dataset then that dataset can be accessed by train from
        all origin 
        '''
        if len(c_list) == 0:
            return True


        restriction_uri = None

        for location_restriction in graph.subjects(RDF.type, URIRef("http://purl.obolibrary.org/obo/DUO_0000022")):
            restriction_uri = location_restriction

        location_uri = None
        for uri in graph.objects(restriction_uri, RDF.value):
            location_uri = uri

        for uri in self._get_location_mapping(str(location_uri)):
            if uri != location_uri:
                graph.add((restriction_uri, RDF.value, URIRef(uri)))

        for condition in train_location_conditions:
            # Get triples matches condition
            c_list = list(graph.triples(condition))

            if len(c_list) == 0:
                print('mismatched condition')
            else:
                return True

        return False


    def does_dates_matches(self, dataset):

        graph = self._get_graph(dataset)

        date_condition_in_ds = (None, URIRef('https://w3id.org/GConsent#hasExpiry'), None)


        # Check if dataset has consent expiry
        c_list = list(graph.triples(date_condition_in_ds))

        '''
        if no consent expiry restriction triples found in the dataset then that dataset can be accessed by train from
        all time 
        '''
        if len(c_list) == 0:
            return True

        for date in graph.objects(None, URIRef("https://w3id.org/GConsent#hasExpiry")):
            present_time = datetime.now()
            present_time = present_time.replace(tzinfo=None)

            ds_expiry_time = datetime.fromisoformat(str(date))
            ds_expiry_time = ds_expiry_time.replace(tzinfo=None)
            if ds_expiry_time >= present_time:
                return True
            else:
                return False



    def get_dataset(self, conditions):
        """Apply a minimal set of FDP/DCAT semantics to crawl through a FDP and
        find and return any available SPARQL end-points."""

        # Load FDP content to graph
        graph = self._get_graph(self.FDP_URL)

        # Check if fdp content mactches use condition, if not return None
        if not self._does_graph_matches_conditions(graph, conditions.pop(0)):
            return None

        """
        Loop through FDP metadata layers to get datasets
        """

        # FDP semantics (alternative implementation: crawl any link for dcat:Dataset)
        fdp_dataset_route = ['http://www.re3data.org/schema/3-0#dataCatalog', 'http://www.w3.org/ns/dcat#dataset']

        datasets = []

        for predicate in fdp_dataset_route:
            # Get childern layers as graphs
            graphs = self._get_object_graphs(graph, predicate)
            condition = conditions.pop(0)

            # Empty gobal graph
            graph.remove((None, None, None))
            for g in graphs:
                if self._does_graph_matches_conditions(g, condition):
                    # Add content of childern layer which matches use condition
                     graph =  graph + g
                else:
                    print('mismatched condition')
                    g.close()

        for dataset in graph.subjects(RDF.type, URIRef("http://www.w3.org/ns/dcat#Dataset")):
            print(str(dataset))
            datasets.append(str(dataset))

        graph.close()
        return datasets





    def _does_graph_matches_conditions(self, graph, conditions):

        """
        Check if the content of the graph matches conditions. Return true if content matches conditions

        :param graph: RDF graph
        :param conditions: List of conditions
        :return:    True or False
        """

        if conditions == []:
            return True

        for condition in conditions:
            # Get triples matches condition
            c_list = list(graph.triples(condition))

            if len(c_list) == 0:
                return False
        return True


    def _get_object_graphs(self, graph, predicate):
        """
        For a given predicate get all objects of the predicate and load content of the object url(s) to graph(s).

        :param graph:   RDF graph
        :param predicate: Predicate url
        :return:    List of graph(s)
        """
        urls = list(graph.objects(None,URIRef(predicate)))
        graphs = []

        for url in urls:
            g = self._get_graph(url)
            graphs.append(g)

        return graphs


    def _get_graph(self, url):
        """
        Load content of a url to graph

        :param url: Content url
        :return: Graph
        """
        print("Load content of : " + url)
        graph = rdflib.Graph()
        graph.load(url)
        #time.sleep(3)
        return graph


    def _get_location_mapping(self, location_url):

        location_urls = [location_url]

        if self.LOCATION_ENDPOINT:
            sparql = SPARQLWrapper(self.LOCATION_ENDPOINT)

            # Run get patient count query
            query = open('queries/get_location_list.rq', 'r').read()

            query = query.replace("LOCATION_URL", location_url)
            sparql.setQuery(query)
            sparql.setReturnFormat(JSON)
            results = sparql.query().convert()

            for result in results["results"]["bindings"]:
                if result["location"]["value"]:
                    location_url = result["location"]["value"]
                    print(str(location_url))
                    location_urls.append(location_url)
        else:
            print("Config mapping endpoint")

        return location_urls