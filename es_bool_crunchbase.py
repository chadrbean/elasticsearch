from elasticsearch import Elasticsearch
import json
import sys
import csv



def find_linkedin_count():
    es = Elasticsearch(['Ip1', 'Ip2', 'ip3'])
    q = {"query":{"exists":{"field":"linkedin"}}}
    search_object = q
    result = es.count(
        index='company-v1',
        body=json.dumps(search_object),
        request_timeout=30
    )
    return(result)
def find_cb_count():
    es = Elasticsearch(['Ip1', 'Ip2', 'ip3'])
    q = {"query":{"exists":{"field":"crunchbase"}}}
    search_object = q
    result = es.count(
        index='company-v1',
        body=json.dumps(search_object),
        request_timeout=30
    )
    return(result)

def find_neither():
    es = Elasticsearch(['Ip1', 'Ip2', 'ip3'])
    q = {"query":{"bool":{"must":[{"bool":{"must_not":{"exists":{"field":"crunchbase"}}}},{"bool":{"must_not":{"exists":{"field":"linkedin"}}}}]}}}

    
    search_object = q
    result = es.count(
        index='company-v1',
        body=json.dumps(search_object),
        request_timeout=30
    )
    return(result)

def create_csv():
    with open("csv_out_files/company_LI_CB_Count.csv", "w") as fw:
        found_linkedin_count = find_linkedin_count()
        fieldnames = ["Type", 'Count']
        csvw = csv.DictWriter(fw, fieldnames=fieldnames, delimiter='|')
        csvw.writeheader()
        csvw.writerow({"Type":"Linkedin", "Count":found_linkedin_count["count"]})
        found_linkedin_count = find_cb_count()
        csvw.writerow({"Type":"Crunchbase", "Count":found_linkedin_count["count"]})
        found_find_neither = find_neither()
        csvw.writerow({"Type":"No LI or CB Info", "Count":found_find_neither["count"]})
    
    


store_data = create_csv()
print(store_data)
