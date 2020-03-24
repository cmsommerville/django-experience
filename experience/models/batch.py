import pyodbc
import os
import datetime
import json
import keyring
from django.template.loader import get_template, render_to_string
from django.utils.text import slugify
from django.conf import settings
from experience.models.reportset import ReportSet
from experience.models.reportWrapper import ReportWrapper
import psycopg2


class Batch():

	def __init__(self, batchdata, postgres = False, DSN = None, connectionString = None, html_template = "experience/base.html", *args, **kwargs):

		now = datetime.datetime.now()
		self.batchTimeStamp = now.strftime("%y%m%d-%H%M%S")
		self.html_template = html_template

		if isinstance(batchdata, list):
			self.batchData = batchdata
		else:
			raise Exception("Please pass configuration in the format {'batchdata':[]}")

		# connect to ODBC connection
		if (DSN is not None) and (kwargs.get("KEYRING_SERVICE") is not None):
			if kwargs.get("UID") is None:
				UID = os.getenv("USER")
			else:
				UID = kwargs.get("UID")

			service = keyring.get_password(kwargs.get("KEYRING_SERVICE", ""), UID)
			self.conn = pyodbc.connect(f"DSN={DSN};UID={UID};PWD={service}")
		elif DSN is not None:
			self.conn = pyodbc.connect(f"DSN={DSN}")
		elif connectionString is not None:
			self.conn = pyodbc.connect(connectionString)
		elif postgres:
			self.conn = psycopg2.connect(f"dbname='cmsommerville' user='cmsommerville' host='localhost' password='{keyring.get_password('postgres', 'cmsommerville')}'")
		else:
			raise Exception("Must pass an ODBC DSN or connection string")

		self.batchLog = []


	def __str__(self):
		return(str({
			"batchTimeStamp": self.batchTimeStamp,
			"conn": self.conn,
			"batchData": self.batchData,
			"batchLog": self.batchLog
		}))

	def renderReports(self, reportList, title_prefix = None, default_title = "Unknown Group"):
		logList = []

		for wrapper in reportList:

			if wrapper.status == "Failure":
				logList.append(wrapper.display())
				continue

			report = wrapper.report

			try:
				if title_prefix is None:
					report_filename = slugify(report.output.get("title", default_title)) + ".html"
				else:
					report_filename = '-'.join([title_prefix, slugify(report.output.get("title", default_title))]) + ".html"

				rendered_report = render_to_string(self.html_template, report.output)

				try:
					fileloc = os.path.join(settings.REPORTS_DIR, report_filename)
				except:
					dir = os.path.dirname(__file__)
					dir = os.path.dirname(dir)
					dir = os.path.dirname(dir)
					dir = os.path.join(dir, "reports")
					fileloc = os.path.join(dir, report_filename)

				with open(fileloc, "w") as f:
					f.write(rendered_report)

				wrapper.set_status("Success")
				wrapper.set_link(fileloc)
				logList.append(wrapper.display())
			except err:
				wrapper.set_status("Failure", reason = "Could not render report", err = str(err))
				logList.append(wrapper.display())

		return(logList)

	def run(self):
		batchLog = []

		# loop through each item in the batch
		for i, rs in enumerate(self.batchData):
			# for each report set, append the connection to the JSON
			rs["conn"] = self.conn
			rs["id"] = i

			try:
				# create report set object
				# this will create the report list
				report_set = ReportSet(**rs)
				# get the report list
				reportList = report_set.reportList

				# pass report list into renderer method
				logList = self.renderReports(reportList, title_prefix = self.batchTimeStamp)


				# append the log list for the report list to the entire batch log
				batchLog.extend(logList)
			except:
				wrapper = ReportWrapper(**{
					"reportSetID": i,
					"reportID": -1,
					"status": "Failure",
					"reportKey": str(rs)[0:30] + "...",
					"reportKeyType": "Group Numbers",
					"title": "Unknown Groups"
				})
				batchLog.append(wrapper.display())

		# write the batch log to a file

		try:
			fileloc = os.path.join(settings.REPORTS_DIR, '-'.join([self.batchTimeStamp, "LOG.json"]))
		except:
			dir = os.path.dirname(__file__)
			dir = os.path.dirname(dir)
			dir = os.path.dirname(dir)
			dir = os.path.join(dir, "reports")
			fileloc = os.path.join(dir, '-'.join([self.batchTimeStamp, "LOG.json"]))

		with open(fileloc, "w") as f:
			json.dump({"log": batchLog}, f)

		self.batchLog = batchLog
