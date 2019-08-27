import rdflib
from rdflib import RDF, URIRef
from SPARQLWrapper import SPARQLWrapper

"""
TODO:
refactoring and testing
proper logging rather than stdout prints
generalize to other end-point type or other RDF distribution types

LIMITATIONS:
returns only a single end-point even if multiple are available
"""

class FDP_SPARQL_crawler:

    #gg = rdflib.Graph()

    # FDP semantics (alternative implementation: crawl any link for dcat:Dataset)
    fdp_route = ['http://www.re3data.org/schema/3-0#dataCatalog',
                 'http://www.w3.org/ns/dcat#dataset',
                 'http://www.w3.org/ns/dcat#distribution']


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
            graph = self._getGraph(url)

        # Get childern layers as graphs
        graphs = self._getObjectGraphs(graph, "http://www.w3.org/ns/dcat#distribution")
        condition = conditions.pop(0)

        # Empty gobal graph
        graph.remove((None, None, None))
        for g in graphs:
            if self._doesGraphMatchesConditions(g, condition):
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
        graph = self._getGraph(dataset_url)

        # Get childern layers as graphs
        graphs = self._getObjectGraphs(graph, "http://www.w3.org/ns/dcat#distribution")
        condition = conditions.pop(0)

        # Empty gobal graph
        graph.remove((None, None, None))
        for g in graphs:
            if self._doesGraphMatchesConditions(g, condition):
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



    def does_useclause_of_train_dataset_match(self, dataset, trainuseconditions):

        graph = self._getGraph(dataset)

        for condition in trainuseconditions:
            # Get triples matches condition
            c_list = list(graph.triples(condition))

            if len(c_list) == 0:
                print('mismatched condition')
            else:
                return True

        return False



    def get_dataset(self, url, conditions):
        """Apply a minimal set of FDP/DCAT semantics to crawl through a FDP and
        find and return any available SPARQL end-points."""

        # Load FDP content to graph
        graph = self._getGraph(url)

        # Check if fdp content mactches use condition, if not return None
        if not self._doesGraphMatchesConditions(graph, conditions.pop(0)):
            return None

        """
        Loop through FDP metadata layers to get datasets
        """

        # FDP semantics (alternative implementation: crawl any link for dcat:Dataset)
        fdp_dataset_route = ['http://www.re3data.org/schema/3-0#dataCatalog', 'http://www.w3.org/ns/dcat#dataset']

        datasets = []

        for predicate in fdp_dataset_route:
            # Get childern layers as graphs
            graphs = self._getObjectGraphs(graph, predicate)
            condition = conditions.pop(0)

            # Empty gobal graph
            graph.remove((None, None, None))
            for g in graphs:
                if self._doesGraphMatchesConditions(g, condition):
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





    def _doesGraphMatchesConditions(self, graph, conditions):

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


    def _getObjectGraphs(self, graph, predicate):
        """
        For a given predicate get all objects of the predicate and load content of the object url(s) to graph(s).

        :param graph:   RDF graph
        :param predicate: Predicate url
        :return:    List of graph(s)
        """
        urls = list(graph.objects(None,URIRef(predicate)))
        graphs = []

        for url in urls:
            g = self._getGraph(url)
            graphs.append(g)

        return graphs


    def _getGraph(self, url):
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



