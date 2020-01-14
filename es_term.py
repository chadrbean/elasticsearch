from elasticsearch import Elasticsearch
import json
import sys

def run_query():
    total = 0
    with open(sys.argv[1], "r") as f:
        country = f.readlines()
        for i in country:
            i = i.strip().upper()
            es = Elasticsearch(["10.138.0.47", "10.138.0.49", "10.138.0.35"])
            q = {
            "query": {
                "term" : { "country.keyword" : i }
                }
            }
            search_object = q
            result = es.search(
                index='li_marketing',
                body=json.dumps(search_object),
                size=1
            )
            print('{},{}'.format(i, result["hits"]["total"]))
            total += result["hits"]["total"]
        print(total)




print(json.dumps(run_query()))
