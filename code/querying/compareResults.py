import json
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON

class GoldLabelGetter:

    def __init__(self):
        self.counter = 0

    def send_query(self, query):
        sparql = SPARQLWrapper("http://dbpedia.org/sparql")
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()

    def create_answers(self, qald_answers_uri, language_code):
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
                results = self.send_query(query)
                labels = []
                for result in results["results"]["bindings"]:
                    labels.append(result["label"]["value"])

                if id not in qald_answers:
                    qald_answers[id] = []

                if not labels:
                    #print uri
                    self.counter += 1
                    qald_answers[id].append(uri)
                else:
                    qald_answers[id].append(labels[0])
        return qald_answers

    def get_answer_uris(self, query_ids):
        qald_answers_uri = {}
        with open('../qald-9-train-multilingual.json') as infile:
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
        return qald_answers_uri


    def run(self):
        df = pd.read_csv('../Query-translation.tsv', sep='\t').dropna(axis=0, how='all')
        query_ids = [int(i) for i in df['ID'].tolist()]

        qald_answers_uri = self.get_answer_uris(query_ids)

        self.counter = 0
        print "START ENGLISH"
        qald_answers_en = self.create_answers(qald_answers_uri, 'en')
        print self.counter

        with open('qald_answers_en.json', 'w') as outfile:
            json.dump(qald_answers_en, outfile)

        self.counter = 0
        print "START SPANISH"
        qald_answers_es = self.create_answers(qald_answers_uri, 'es')
        print self.counter

        with open('qald_answers_es.json', 'w') as outfile:
            json.dump(qald_answers_es, outfile)

        self.counter = 0
        print "START ARABIC"
        qald_answers_ar = self.create_answers(qald_answers_uri, 'ar')
        print self.counter

        with open('qald_answers_ar.json', 'w') as outfile:
            json.dump(qald_answers_ar, outfile)

        return [qald_answers_en, qald_answers_es, qald_answers_ar]



class KGLabelGetter:
    # get queries for each KG
    def __init__(self):
        df = pd.read_csv('../Query-translation.tsv', sep='\t').dropna(axis=0, how='all')
        self.wikidata = dict(zip(df['ID'].astype(int), df['Wikidata']))
        self.dbpedia = dict(zip(df['ID'].astype(int), df['DBpedia']))
        self.musicbrainz = dict(zip(df['ID'].astype(int), df['MusicBrainz']))
        self.linkedmdb = dict(zip(df['ID'].astype(int), df['LinkedMDB']))
        self.yago = dict(zip(df['ID'].astype(int), df['YAGO']))

    def send_query(self, query, endpoint):
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()

    def get_answers_dbpedia(self, language):
        endpoint = "http://dbpedia.org/sparql"
        answers = {}
        for id, query in self.dbpedia.iteritems():
            answers[id] = []
            results = []
            if language == 'en':
                results = self.send_query(query, endpoint)
            else:
                lang = '"' + language + '"'
                query = query.replace('"en"', lang)
                results = self.send_query(query, endpoint)
            for result in results["results"]["bindings"]:
                answers[id].append(result["label"]["value"])
        return answers

    def run(self, qald_answers_en, qald_answers_es, qald_answers_ar):
        self.get_answers_dbpedia('en')


