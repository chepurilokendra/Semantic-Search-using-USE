import csv
import json
import time
import sys
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import csv
import tensorflow as tf
import tensorflow_hub as hub
# import jsonify
# import flask

#Connecting to elasticsearch in localhost

es = Elasticsearch([{'host':'localhost','port':9201,'scheme':'http'}], basic_auth=('elastic','sTwyGuV85pJiKQpwM+Cz'))

print(es)

if es.ping():
    print("connected to elastic")
else:
    print("failed")


# creating a index in elasticsearch (equivalent to database in sql)

b={"mappings": {
        "properties": {
            "title" : {
                        "type":"text"
                    },
            "title_vector" :{
                "type":"dense_vector",
                "dims":512

                             }
                        }
                 }

    }

#creating an index    
ret = es.indices.create(index='questions-index',ignore=400, body =b)
print(ret)
# print(flask.jsonify(ret))


# loading Universal Sentence Encoder model
embed = hub.load("E:\Semantic search\\universal-sentence-encoder_4\\")


#Num of questions indexed
NUM_QUESTIONS_INDEXED = 200000

#COLUMN NAMES : Id,OwnerUserId,CreationDate,ClosedData,Score,Title,Body
cnt=0

# reading questions from questions.csv file one by one
with open("E:\Semantic search\Stackoverflow\Questions.csv",encoding="latin1") as csvfile:
    readCSV = csv.reader(csvfile,delimiter=',')
    next(readCSV,None)  #skipping headers
    for row in readCSV:
        doc_id = row[0]
        title = row[5]
        vec = tf.make_ndarray(tf.make_tensor_proto(embed([title]))).tolist()[0]

        d={
            'title':title,
            'title_vector' :vec,
        }

        res = es.index(index="questions-index",id=doc_id, body =d)

        cnt=cnt+1

        if cnt%100 ==0:
            print(cnt)
        if cnt == NUM_QUESTIONS_INDEXED:
            break
    print("Indexing Completed")


