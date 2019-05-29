from querying.compareResults import *

glg = GoldLabelGetter()
kglg = KGLabelGetter()
cr = CompareLabels()

qald_answers_en, qald_answers_es, qald_answers_ar = glg.run()
kg_answers = kglg.run()
results = cr.run(qald_answers_en, qald_answers_es, qald_answers_ar, kg_answers)
