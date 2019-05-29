import json
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON


def send_query(query):
    sparql = SPARQLWrapper("http://dbpedia.org/sparql")
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()

def create_answers(qald_answers_uri, language_code):
    qald_answers = {}
    for id, uris in qald_answers_uri.iteritems():
        for uri in uris:
            if not uri.startswith('http'):
                qald_answers[id] = uri
                continue
            query = """
                SELECT DISTINCT ?label WHERE {
                <%s> rdfs:label ?label. 
                FILTER langMatches( lcase(lang(?label)), "%s" )
                }  """ % (uri, language_code)
            results = send_query(query)
            labels = []
            for result in results["results"]["bindings"]:
                labels.append(result["label"]["value"])

            if id not in qald_answers:
                qald_answers[id] = []

            if not labels:
                print uri
                qald_answers[id].append(uri)
            else:
                qald_answers[id].append(labels[0])
    return qald_answers


df = pd.read_csv('Query-translation.tsv', sep='\t').dropna(axis=0, how='all')
query_ids = [int(i) for i in df['ID'].tolist()]

# get queries for each KG
wikidata = dict(zip(df['ID'].astype(int), df['Wikidata']))
dbpedia = dict(zip(df['ID'].astype(int), df['DBpedia']))
musicbrainz = dict(zip(df['ID'].astype(int), df['MusicBrainz']))
linkedmdb = dict(zip(df['ID'].astype(int), df['LinkedMDB']))
yago = dict(zip(df['ID'].astype(int), df['YAGO']))

qald_answers_uri = {}
with open('qald-9-train-multilingual.json') as infile:
    questions = json.load(infile)['questions']
    for q in questions:
        if int(q['id']) in query_ids:
            qald_answers_uri[int(q['id'])] = []
            for answers in q['answers']:
                for a in answers['results']['bindings']:
                    if 'uri' not in a and 'string' in a:
                        qald_answers_uri[int(q['id'])].append(a['string']['value'])
                    elif 'uri' in a:
                        qald_answers_uri[int(q['id'])].append(a['uri']['value'])
                    else:
                        print q['id'], a

qald_answers_en = create_answers(qald_answers_uri, 'en')

with open('qald_answers_en.json', 'w') as outfile:
    outfile.write(json.dump(qald_answers_en))


qald_answers_es = create_answers(qald_answers_uri, 'es')

with open('qald_answers_es.json', 'w') as outfile:
    outfile.write(json.dump(qald_answers_es))

qald_answers_ar = create_answers(qald_answers_uri, 'ar')

with open('qald_answers_ar.json', 'w') as outfile:
    outfile.write(json.dump(qald_answers_ar))