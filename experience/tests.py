from django.test import TestCase
import json
import os
import psycopg2
import keyring
from models.report import Report
from models.reportset import ReportSet
from models.batch import Batch

# Create your tests here.

os.environ['DJANGO_SETTINGS_MODULE'] = 'first_django.settings'


def test_reports():
    with open("./test_data/report_single.json", "r") as j:
        data = json.load(j)

    report = Report(**data)
    return(report.output)


def test_reportSet():
    with open("./test_data/reportset.json", "r") as j:
        data = json.load(j)

    conn = psycopg2.connect(f"dbname='cmsommerville' user='cmsommerville' host='localhost' password='{keyring.get_password('postgres', 'cmsommerville')}'")

    data["conn"] = conn
    data["id"] = 1

    reportSet = ReportSet(**data)
    return(reportSet.reportList)

def test_batch():
    with open("./test_data/batch.json", "r") as j:
        data = json.load(j)

    batch = Batch(**data)
    batch.run()
    return(batch)



if __name__ == "__main__":

    batch = test_batch()
    #print(batch)
