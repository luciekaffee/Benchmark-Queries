import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import json

def send_query(query, endpoint):
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    try:
        result = sparql.query().convert()
    except:
        result = []

    return result

classes = {}

df = pd.read_csv('../Query-translation.tsv', sep='\t').dropna(axis=0, how='all')
wikidata = dict(zip(df['ID'].astype(int), df['Wikidata']))
dbpedia = dict(zip(df['ID'].astype(int), df['DBpedia']))
musicbrainz = dict(zip(df['ID'].astype(int), df['MusicBrainz']))
linkedmdb = dict(zip(df['ID'].astype(int), df['LinkedMDB']))
yago = dict(zip(df['ID'].astype(int), df['YAGO']))


print 'Start Wikidata'

classes['wikidata'] = {}
for id, query in wikidata.iteritems():
    classes['wikidata'][id] = []
    if query == 'X' or not isinstance(query, basestring):
        continue
    q = query.replace('itemLabel', 'class').replace('SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }', '. ?item wdt:P31 ?class .')
    q = q.replace('..', '.').replace('. .', '.')
    if not 'LIMIT' in q:
        q = q + ' LIMIT 10'
    result = send_query(q, 'https://query.wikidata.org/sparql')
    if not result:
        print q
        continue
    for r in result["results"]["bindings"]:
        if not 'class' in r:
            continue
        else:
            classes['wikidata'][id].append(r["class"]["value"])


def get_classes_kgs(kg, endpoint):
    classes = {}
    for id, query in kg.iteritems():
        classes[id] = []
        if query == 'X' or not isinstance(query, basestring):
            continue
        if not 'PREFIX' in query:
            prefixes = 'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  PREFIX wdt: <http://www.wikidata.org/prop/direct/>  ' \
                       'PREFIX wd: <http://www.wikidata.org/entity/>  PREFIX p: <http://www.wikidata.org/prop/> PREFIX ps: <http://www.wikidata.org/prop/statement/>' \
                       ' PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dbo: <http://dbpedia.org/ontology/> PREFIX dbp: <http://dbpedia.org/property/> ' \
                       'PREFIX res: <http://dbpedia.org/resource/> PREFIX yago: <http://dbpedia.org/class/yago/>'
            query = prefixes + '\n' + query
        q = query.replace('SELECT DISTINCT ?label', 'SELECT DISTINCT ?class').replace('; rdfs:label ?label. FILTER langMatches( lcase(lang(?label)), "en" )', '. ?uri rdf:type ?class .')
        q = q.replace('SELECT ?label', 'SELECT DISTINCT ?class').replace('?uri rdfs:label ?label. FILTER langMatches( lcase(lang(?label)), "eng" )', ' ?uri rdf:type ?class .')
        #if not '. ?uri rdf:type ?class' in q or '.?uri rdf:type ?class' in q:
         #   q = q.replace('?uri rdf:type ?class', '. ?uri rdf:type ?class' )
        q = q.replace('?uri rdfs:label ?label. FILTER langMatches( lcase(lang(?label)), "en" )', '?uri rdf:type ?class .')
        q = q.replace('?uri rdfs:label ?label.', '?uri rdf:type ?class .').replace('GROUP BY ?label', 'GROUP BY ?class')
        q = q.replace('purl:title ?label', 'rdf:type ?class').replace('foaf:name ?label', 'rdf:type ?label').replace('skos:altLabel ?label', 'rdf:type ?class')
        q = q.replace('. .', '.').replace('..', '.')
        if not 'LIMIT' in q:
            q = q + ' LIMIT 10'
        result = send_query(q, endpoint)
        if not result:
            print q
            continue
        for r in result["results"]["bindings"]:
            if not 'class' in r:
                continue
            else:
                classes[id].append(r["class"]["value"])
    return classes

print 'Start DBpedia'
classes['DBpedia'] = get_classes_kgs(dbpedia, 'http://node1.research.tib.eu:4001/sparql')

print 'Start YAGO'
classes['YAGO'] = get_classes_kgs(yago, 'http://node3.research.tib.eu:4011/sparql')

print 'Start LinkedMDB'
classes['linkedmdb'] = get_classes_kgs(linkedmdb, 'http://node3.research.tib.eu:11887/sparql')

print 'Start MusicBrainz'
classes['MusicBrainz'] = get_classes_kgs(musicbrainz, 'http://node3.research.tib.eu:4012/sparql')


with open('classes-10.json', 'w+') as outfile:
    json.dump(classes, outfile)



