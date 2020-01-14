from elasticsearch import Elasticsearch
import json
import sys
import csv


def find_industries():

    es = Elasticsearch(['10.138.15.223', '10.138.15.220', '10.138.15.228'])
    # q = {"size": 0,"aggs" : {"industries" : {"terms" : { "field" : "linkedin.industry.keyword"}}}
    q = {"size": 0,"aggs" : {"industries" : {"terms" : { "field" : "linkedin.industry.keyword","size" : 10000}}}}

    search_object = q
    result = es.search(
        index='company-v1',
        body=json.dumps(search_object),
        request_timeout=30
    )
    return(result)

def create_csv():
    found_industries = find_industries()
    fieldnames = ['Industries', 'Count']
    with open("csv_out_files/company_industries_out.csv", "w") as fw:
        csvw = csv.DictWriter(fw, fieldnames=fieldnames, delimiter='|')
        csvw.writeheader()
        for i in found_industries["aggregations"]["industries"]["buckets"]:
            print(i["key"])
            print(i["doc_count"])
            csvw.writerow({"Industries":i["key"], "Count":i["doc_count"]})
    
    
       
 



store_data = create_csv()
print(store_data)
