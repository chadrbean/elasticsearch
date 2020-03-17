import sys
import json
import csv
from elasticsearch import Elasticsearch

es = Elasticsearch(['Ip1', 'Ip2', 'ip3'])

def build_query(domain=None, titles=[]):
    q = {
        "query": {
            "bool": {
                "must": [
                    {
                        "match": {}
                    },
                    {
                        "bool": {
                            "should": []
                        }
                    }
                ]
            }
        }
    }
    if not domain:
        return
    q["query"]["bool"]["must"][0]["match"]["email_data.domain"] = domain
    for title in titles:
        q["query"]["bool"]["must"][1]["bool"]["should"].append({
            "match": {
                "experience.title": {
                    "query": title
                }
            }
        })
    return q

titles = ["founder", "ceo", "cto", "executive", "director", "president", "vp", "head", "product", "coo", "cio", "cmo", "cdo"]
track_emails = []
with open(sys.argv[1]) as f, open(sys.argv[2], 'w') as o:
    csvr = csv.reader(f)
    csvw = csv.writer(o)
    for row in csvr:
        q = build_query(domain=row[1], titles=titles)
        es_hits = es.search(index="sp-email", body=q)
        for hit in es_hits["hits"]["hits"]:
            s = hit["_source"]
            if "email_data" in s:
                for e in s["email_data"]:
                    if any("google.com" in mx for mx in e["mx"]) or any("outlook.com" in mx for mx in e["mx"]):
                        for email in e["emails"]:
                            if email not in track_emails:
                                
                                track_emails.append(email)
                                original_row = row 
                                email_q = {
                                    "query": {
                                        "term": { "email.keyword": email }
                                    }
                                }
                                email_results = es.search(index="*-validation", body=email_q)["hits"]["hits"]
                                if email_results and "_source" in email_results[0] and ("set-cookie" in email_results[0]["_source"] or "Set-Cookie" in email_results[0]["_source"]):       
                                    # for exp in s["experience"]:
                                    if e["domain"] == row[1]:
                                        add_emailplusdata = [email, s["first_name"], s["last_name"], s["username"]]
                                        if "title" in s["experience"][0] and s["experience"][0]["title"]:
                                            add_emailplusdata.extend([s["experience"][0]["title"]])
                                        if "location" in s["experience"][0] and s["experience"][0]["location"]:
                                            add_emailplusdata.extend([s["experience"][0]["location"]])
                                        
                                        new_row = []
                                        new_row.extend(original_row)
                                        new_row.extend(add_emailplusdata)
                                        csvw.writerow(new_row)
