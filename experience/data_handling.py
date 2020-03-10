import datetime
import sys
import json
import pandas as pd
import pyodbc
import psycopg2
import os
import re
import keyring
from django.template.loader import render_to_string
from django.utils.text import slugify
from django.conf import settings

class ExperienceReport():

	def __init__(self,
		DSN = None,
		conn_string = None,
		config = None,
		postgres = False,
		grpnum = None,
		fromdate = None,
		thrudate = None,
		title = None,
		*args,
		**kwargs):

		if config is not None:
			if isinstance(config, list):
				self.request_data = config
			else:
				self.request_data = [config]
		elif ((grpnum is not None) & (fromdate is not None) & (thrudate is not None)):
			self.request_data = [{"grpnum": grpnum, "fromdate": fromdate, "thrudate": thrudate}]

			if title is not None:
				self.request_data["title"]
		else:
			raise Exception ("Must enter either a configuration file or a group number, from date, and thru date")

		# create connection
		if DSN is not None:
			self.conn = pyodbc.connect(f"DSN={DSN}")
		elif conn_string is not None:
			self.conn = pyodbc.connect(conn_string)
		elif postgres:
			self.conn = psycopg2.connect(f"dbname='cmsommerville' user='cmsommerville' host='localhost' password='{keyring.get_password('postgres', 'cmsommerville')}'")

	def cleanGroupNames(self, grpname):
		# remove slashes and single quotes
		title = re.sub('[\\/\']', ' ', grpname)
		# remove state code in parentheses
		title = re.sub('\([A-Za-z]{2}\)', '', title)

		return(title)

	def getInputs(self, grpnum, fromdate = None, thrudate = None, *args, **kwargs):
		"""
			Extracts required data elements out of dictionary. Creates the SQL statement that will be executed.

			---
			INPUT:

			grpnum: a single string or an array of strings specifying which group(s) to query

			fromdate: a string representation of the report from date

			thrudate: a string representation of the report thru date

			---
			OUTPUT:

			sql: SQL statement that can be executed

			parms: a dictionary of other parameters used to print the report

		"""


		#queryTemplate = "EXEC Act_PricingTN.mcr_ClaimExperience('{0}', '{1}', '{2}')"
		queryTemplate = """
SELECT E.*
FROM ACT_ACTUARIALDB.TBL_EXPERIENCE AS E
INNER JOIN ACT_ACTUARIALDB.PXGRPCTRL AS P ON
	E.GRPNUM = P.GRPNUM
INNER JOIN
	(SELECT DISTINCT GRPUID
	FROM ACT_ACTUARIALDB.PXGRPCTRL
	WHERE GRPNUM IN ('{0}')) AS G
ON
	P.GRPUID = G.GRPUID
WHERE
	EXPYR >= EXTRACT(YEAR FROM CAST('{1}' AS DATE)) AND
	EXPYR <= EXTRACT(YEAR FROM CAST('{2}' AS DATE))
			"""
		sql_grpname = """
SELECT P.GRPUID, P.PRMSYSTEM, P.PRMGRPNUM, P.PRMGRPNAME
FROM ACT_ACTUARIALDB.PXGRPCTRL AS P
INNER JOIN
	(SELECT DISTINCT GRPUID
	FROM ACT_ACTUARIALDB.PXGRPCTRL
	WHERE GRPNUM IN ('{0}') AND SYSTEM IN ('WYN', 'GEN')) AS F
ON
	P.GRPUID = F.GRPUID AND
	P.PRMGRPNUM = P.GRPNUM AND
	P.PRMSYSTEM = P.SYSTEM
		"""

		today = datetime.date.today()

		# default run separate parameter to False
		runSeparate = kwargs.get("runSeparate", False)
		list_grpnum = grpnum
		dt_from = fromdate
		dt_thru = thrudate

		# turn grpnum parameter into list if not a list to start
		if isinstance(list_grpnum, list) == False:
			list_grpnum = [grpnum]

		# create dictionary of groups with cleaned group names
		sql_grpname_resolved = sql_grpname.format(*tuple(['\',\''.join(list_grpnum)]))
		df_groups = pd.read_sql(sql_grpname_resolved, self.conn)
		df_groups.columns = [x.upper() for x in df_groups.columns.tolist()]
		df_groups["PRMGRPNAME"] = df_groups["PRMGRPNAME"].apply(lambda x: self.cleanGroupNames(x))

		group_dict = {}
		for grp in set(df_groups["PRMGRPNUM"].values.tolist()):
			group_dict[grp] = df_groups[df_groups["PRMGRPNUM"] == grp].to_dict("records")


		# set title
		if kwargs.get("title") is None:
			if len(list_grpnum) == 1:
				title = group_dict[list_grpnum[0]][0].get("PRMGRPNAME", "Unknown Group")
			else:
				title = "Multiple Groups"
		else:
			title = kwargs.get("title")


		# set thru date and from date
		if dt_thru is None:
			if (today.month in [2, 3, 4]):
				dt_thru = datetime.date(today.year - 1, 12, 31)
			elif (today.month in [5, 6, 7]):
				dt_thru = datetime.date(today.year, 3, 31)
			elif (today.month in [8, 9, 10]):
				dt_thru = datetime.date(today.year, 6, 30)
			elif (today.month in [11, 12]):
				dt_thru = datetime.date(today.year, 9, 30)
			elif today.month == 1:
				dt_thru = datetime.date(today.year - 1, 9, 30)

			dt_from = datetime.date(dt_thru.year - 2, 1, 1)

		if dt_from is None:
			dt_from = datetime.date(dt_thru.year - 2, 1, 1)


		parms = {
			"grpnum": list_grpnum,
			"fromdate": dt_from,
			"thrudate": dt_thru,
			"title": title,
			"runSeparate": runSeparate,
			"group_dict": group_dict
		}

		sql = queryTemplate.format(*tuple(['\',\''.join(list_grpnum), str(dt_from), str(dt_thru)]))

		return(sql, parms)




	def queryAllData(self, sql, cursor, *args, **kwargs):

		"""
			Query the database
		"""
		df = pd.DataFrame()

		try:
			# query data
			cursor.execute(sql)
			df = cursor.fetchall()
			df = pd.DataFrame.from_records(df)
			df.columns = [x[0].upper() for x in cursor.description]

		except:
			raise Exception("Could not query data")

		return(df)


	def formatData(self, df, parms = None, *args, **kwargs):
		"""
			Accepts a Pandas dataframe. Returns a dictionary formatted for HTML injection
		"""
		df = df.sort_values("EXPYR")
		df["LOSSRATIO"] = df["INCCLAIMS"] / df["EARNEDPREM"]
		data = []
		list_lob = list(set(df["LOB"]))
		for lob in list_lob:
			d = {}
			d["name"] = df[df["LOB"] == lob]["LOBDESC"].values[0]
			d["Year"] = df[df["LOB"] == lob]["EXPYR"].values.tolist()
			d["EarnedPremium"] = ['${:,.2f}'.format(y) for y in df[df["LOB"] == lob]["EARNEDPREM"].values.tolist()]
			d["IncurredClaims"] = ['${:,.2f}'.format(y) for y in df[df["LOB"] == lob]["INCCLAIMS"].values.tolist()]
			d["LossRatio"] = ['{:,.2%}'.format(y) for y in df[df["LOB"] == lob]["LOSSRATIO"].values.tolist()]
			data.append(d)
		output = {"data": data, "parms": parms}
		return(output)



	def renderReport(self, df, parms = None, *args, **kwargs):
		"""
			Injects the data into the HTML template file
		"""

		html_template = "experience/base.html"
		title = parms.get("title")
		fromdate = parms.get("fromdate")
		thrudate = parms.get("thrudate")
		runSeparate = parms.get("runSeparate", False)

		try:
			# create consolidated report
			experience = df.groupby(["LOB", "LOBDESC", "EXPYR"])["EARNEDPREM", "INCCLAIMS"].sum().reset_index()
			report_parms = {
				"title": title,
				"fromdate": fromdate,
				"thrudate": thrudate
			}
			dict_exp = self.formatData(experience, parms)
			rendered_report = render_to_string(html_template, dict_exp)

			with open(os.path.join(settings.REPORTS_DIR, "output.html"), "w") as f:
			    f.write(rendered_report)
		except:
			raise Exception("Could not create consolidated experience report")

		try:
			if runSeparate:
				grp_list = list(set(df["GRPNUM"]))
				group_dict = parms.get("group_dict")

				for grp in grp_list:
					grpname = group_dict.get(grp).get("PRMGRPNAME")
					report_parms = {
						"title": grpname,
						"fromdate": fromdate,
						"thrudate": thrudate
					}

					experience = df[df["GRPNUM"] == grp].groupby(["LOBDesc", "IncurredDate"])["EarnedPremium", "IncurredClaims", "InforceCerts"].sum().reset_index()
					dict_exp = self.formatData(experience, report_parms)
					# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
					#
					#	render and export HTML here
					#
					# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		except:
			raise Exception("Could not create separate experience reports")


	def reportLoop(self):

		cursor = self.conn.cursor()

		try:
			for data in self.request_data:

				sql, parms = self.getInputs(**data)
				df = self.queryAllData(sql, cursor)

				#if parms.get("runSeparate"):
				#	pass

				self.renderReport(df, parms)

		finally:
			cursor.close()




if __name__ == "__main__":

	# read configuration

	with open(sys.argv[1], "r") as j:
		config = json.load(j)

	settings.configure()

	experience_request = ExperienceReport(**config)
	experience_request.reportLoop()
	#print(experience_request.request_data)
