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

        with open('qald_answers_en.json', 'w+') as outfile:
            print 'write to file en'
            json.dump(qald_answers_en, outfile)

        self.counter = 0
        print "START SPANISH"
        qald_answers_es = self.create_answers(qald_answers_uri, 'es')
        print self.counter

        with open('qald_answers_es.json', 'w+') as outfile:
            print 'write to file es'
            json.dump(qald_answers_es, outfile)

        self.counter = 0
        print "START ARABIC"
        qald_answers_ar = self.create_answers(qald_answers_uri, 'ar')
        print self.counter

        with open('qald_answers_ar.json', 'w+') as outfile:
            print 'write to file ar'
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
        prefixes = 'PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>  PREFIX wdt: <http://www.wikidata.org/prop/direct/>  ' \
                   'PREFIX wd: <http://www.wikidata.org/entity/>  PREFIX p: <http://www.wikidata.org/prop/> PREFIX ps: <http://www.wikidata.org/prop/statement/>' \
                   ' PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> PREFIX dbo: <http://dbpedia.org/ontology/> PREFIX dbp: <http://dbpedia.org/property/> ' \
                   'PREFIX res: <http://dbpedia.org/resource/>'
        query = prefixes + '\n' + query
        sparql = SPARQLWrapper(endpoint)
        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        return sparql.query().convert()

    def get_answers(self, language, endpoint):
        answers = {}
        for id, query in self.dbpedia.iteritems():
            answers[id] = []
            if query == 'X':
                continue
            results = []
            if language == 'en':
                results = self.send_query(query, endpoint)
            else:
                lang = '"' + language + '"'
                query = query.replace('"en"', lang)
                results = self.send_query(query, endpoint)
            for result in results["results"]["bindings"]:
                if not ['label'] in result:
                    continue
                else:
                    answers[id].append(result["label"]["value"])
        return answers

    def get_answers_wikidata(self, language, endpoint):
        answers = {}
        for id, query in self.dbpedia.iteritems():
            answers[id] = []
            if query == 'X':
                continue
            results = []
            if language == 'en':
                results = self.send_query(query, endpoint)
            else:
                lang = '"' + language + '"'
                query = query.replace('"en"', lang)
                results = self.send_query(query, endpoint)
            for result in results["results"]["bindings"]:
                if not 'itemLabel' in result:
                    continue
                else:
                    answers[id].append(result["itemLabel"]["value"])
        return answers

    def run(self):
        kg_answers = {}
        # English
        print 'Get answers English'
        # DBpedia: http://node1.research.tib.eu:4001/sparql
        dbpedia_en = self.get_answers('en', 'http://node1.research.tib.eu:4001/sparql')
        # Wikidata: http://node3.research.tib.eu:4010/sparql | https://query.wikidata.org/sparql
        wikidata_en = self.get_answers_wikidata('en', 'http://node3.research.tib.eu:4010/sparql')
        # MusicBrainz: http://node3.research.tib.eu:4012/sparql
        musicbrainz_en = self.get_answers('en', 'http://node3.research.tib.eu:4012/sparql')
        # LinkedMDB: http://node4.research.tib.eu:11887/sparql
        linkedmdb_en = self.get_answers('en', 'http://node4.research.tib.eu:11887/sparql')
        # YAGO: http://node3.research.tib.eu:4011/sparql
        yago_en = self.get_answers('en', 'http://node3.research.tib.eu:4011/sparql')

        kg_answers['en'] = {'DBpedia': dbpedia_en, 'Wikidata': wikidata_en, 'MusicBrainz': musicbrainz_en, 'LinkedMDB': linkedmdb_en, 'YAGO': yago_en}

        # Spanish
        print 'Get answers Spanish'
        # DBpedia: http://node1.research.tib.eu:4001/sparql
        dbpedia_es = self.get_answers('es', 'http://node1.research.tib.eu:4001/sparql')
        # Wikidata: http://node3.research.tib.eu:4010/sparql | https://query.wikidata.org/sparql
        wikidata_es = self.get_answers_wikidata('es', 'http://node3.research.tib.eu:4010/sparql')
        # MusicBrainz: http://node3.research.tib.eu:4012/sparql
        musicbrainz_es = self.get_answers('es', 'http://node3.research.tib.eu:4012/sparql')
        # LinkedMDB: http://node4.research.tib.eu:11887/sparql
        linkedmdb_es = self.get_answers('es', 'http://node4.research.tib.eu:11887/sparql')
        # YAGO: http://node3.research.tib.eu:4011/sparql
        yago_es = self.get_answers('es', 'http://node3.research.tib.eu:4011/sparql')

        kg_answers['es'] =  {'DBpedia': dbpedia_es, 'Wikidata': wikidata_es, 'MusicBrainz': musicbrainz_es, 'LinkedMDB': linkedmdb_es, 'YAGO': yago_es}

        # Arabic
        print 'Get answers Arabic'
        # DBpedia: http://node1.research.tib.eu:4001/sparql
        dbpedia_ar = self.get_answers('ar', 'http://node1.research.tib.eu:4001/sparql')
        # Wikidata: http://node3.research.tib.eu:4010/sparql | https://query.wikidata.org/sparql
        wikidata_ar = self.get_answers_wikidata('ar', 'http://node3.research.tib.eu:4010/sparql')
        # MusicBrainz: http://node3.research.tib.eu:4012/sparql
        musicbrainz_ar = self.get_answers('ar', 'http://node3.research.tib.eu:4012/sparql')
        # LinkedMDB: http://node4.research.tib.eu:11887/sparql
        linkedmdb_ar = self.get_answers('ar', 'http://node4.research.tib.eu:11887/sparql')
        # YAGO: http://node3.research.tib.eu:4011/sparql
        yago_ar = self.get_answers('ar', 'http://node3.research.tib.eu:4011/sparql')

        kg_answers['ar'] = {'DBpedia': dbpedia_ar, 'Wikidata': wikidata_ar, 'MusicBrainz': musicbrainz_ar, 'LinkedMDB': linkedmdb_ar, 'YAGO': yago_ar}

        return kg_answers

class CompareLabels():

    def compareLists(self, a, b):
        counter = 0
        l = []
        for i in a:
            if i in b:
                l.append(True)
            else:
                l.append(False)

    def run(self, qald_answers_en, qald_answers_es, qald_answers_ar, kg_answers):
        result = {}
        for name, kg in kg_answers['en']:
            print name
            result[name] = {}
            for id, answer in qald_answers_en.iteritems():
                kg_answer = kg[id]
                # check if answers are the same
                if set(kg_answer) == set(answer):
                    if not 'correct' in result['name']:
                        result['name']['correct'] = 1
                    else:
                        result['name']['correct'] += 1
                # check if kg has more answers than qald
                if len(kg_answer > answer):
                    if not 'length_more' in result[name]:
                        result[name]['length_more'] = 1
                    else:
                        result[name]['length_more'] += 1
                print result





