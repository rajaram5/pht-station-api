from rdflib import URIRef

QUERY_MATCHES_PREDICATE = \
    URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/numberOfDatasetMatchesSearchQuery")

USE_INTENTION_MATCHES_PREDICATE = \
    URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/numberOfDatasetMatchesUseIntentionConditions")

LOCATION_CONDITION_MATCHES_PREDICATE = \
            URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/numberOfDatasetMatchesLocationConditions")

CONSENT_EXPIRY_DATE_MATCHES_PREDICATE = \
            URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/numberOfDatasetMatchesConsentExpirtDate")

DATASET_AVAILABLE_PREDICATE = \
            URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/numberOfDatasetAvailableToTrain")

DATASET_USE_STATUS_PREDICATE = \
            URIRef("http://rdf.biosemantics.org/resources/pht-station/voca/datasetUseStatusForThisTrain")