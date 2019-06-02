# Benchmark Queries

This repository helps to query and compare Knowledge graphs based on the QALD queries transferred in 5 different ontologies:
* DBpedia (based on the original QALD queries)
* Wikidata
* YAGO
* MusicBrainz
* LinkedMDB

We focus on three languages:
* English
* Spanish
* Arabic

The languages can be easily adapted in the *GoldLabelGetter* and *CompareLabels* classes.

## Data Files
The data for the comparison is in the following files:
* *qald-9-train-multilingual.json* from https://github.com/ag-sc/QALD/blob/master/9/data/qald-9-train-multilingual.json
* *Query-translation.tsv*: The queries in the 5 different ontologies, random order. The ID file is the same question ID as QALD. *X* denotes queries that could not transfered. Queries are limited to the ones that return labels. DBpedia queries from QALD return English labels instead of entity URI.

## Code
The code is in the *code* folder. To run the code, execute the *code/run.py* file from inside the code folder. Following a brief description of the classes used in the run file.
* *GoldLabelGetter*: The code will first run the DBpedia queries to get the answers based on the QALD queries.
* *KGLabelGetter*: Gets the labels for all queries from the queries in the Query-translation.tsv file. The endpoints are university internal, they need to be replaced. 
* *CompareLabels*: With the list of all query results, the answers are compared to the answers based on the QALD dataset. 
