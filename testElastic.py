from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

es = Elasticsearch([{'host':'localhost','port':9201,'scheme':'http'}], basic_auth=('elastic','sTwyGuV85pJiKQpwM+Cz'))
# es =Elasticsearch('localhost')

print(es)

if es.ping():
    print("connected to elastic")
else:
    print("failed")

