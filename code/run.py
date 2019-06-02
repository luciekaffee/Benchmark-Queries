from querying.compareResults import *

glg = GoldLabelGetter()
kglg = KGLabelGetter()
cr = CompareLabels()

#qald_answers_en, qald_answers_es, qald_answers_ar = glg.run()
qald_answers_en = {}
qald_answers_es = {}
qald_answers_ar = {}
with open('qald_answers_en.json') as infile:
    qald_answers_en = json.load(infile)
with open('qald_answers_es.json') as infile:
    qald_answers_es = json.load(infile)
with open('qald_answers_ar.json') as infile:
    qald_answers_ar = json.load(infile)

kg_answers = kglg.run()
#results = cr.run(qald_answers_en, qald_answers_es, qald_answers_ar, kg_answers)
