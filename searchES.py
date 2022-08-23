import time
import tensorflow as tf
import tensorflow_hub as hub
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

def connect2ES():

    es = Elasticsearch([{'host':'localhost','port':9201,'scheme':'http'}], basic_auth=('elastic','sTwyGuV85pJiKQpwM+Cz'))
    if es.ping():
        print("Elatsic search connection successfull")
    else:
        print("Connection Failed")
    
    print("***************************************************************")
    return es


# searching by keywords

def searchByKeyword(es,q):

    b={
        'query':{
            'match':{
                'title':q
            }
        }
    }

    res=es.search(index='questions-index',body=b)
    print("Search Results Using Keywords\n")
    for hit in res['hits']['hits']:
        print(str(hit['_score'])+"\t"+hit['_source']['title'])
    print("******************************************************")


# searching using USE vectors

def searchUsingSemantics(embed,es,sent):
    query_vector=tf.make_ndarray(tf.make_tensor_proto(embed([sent]))).tolist()[0]

    b = {"query" : {
                "script_score" : {
                    "query" : {
                        "match_all": {}
                    },
                    "script" : {
                        "source": "cosineSimilarity(params.query_vector, 'title_vector') + 1.0",
                        "params": {"query_vector": query_vector}
                    }
                }
             }
        }

    res = es.search(index='questions-index',body=b)

    print("Search Results Using Semantic Similarity\n")

    for hit in res['hits']['hits']:
        print(str(hit['_score'])+"\t"+hit['_source']['title'])
    
    print("******************************************************")


if __name__=="__main__":

    es = connect2ES()

    embed = hub.load("E:\Semantic search\\universal-sentence-encoder_4\\")


    while(1):

        query = input("Enter a Query : ")

        start = time.time()

        if query=="end":
            break
        
        print("Query is : ",query)
        searchByKeyword(es,query)
        searchUsingSemantics(embed,es,query)

        end=time.time()
        print(end-start)



