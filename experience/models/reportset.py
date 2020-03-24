import re
import pandas as pd
import datetime
from django.utils.text import slugify
from experience.models.report import Report
from experience.models.reportWrapper import ReportWrapper

class ReportSet():

	def __init__(self,
		id,
		grpnums,
		conn,
		fromdate = None,
		thrudate = None,
		title = None,
		runSeparate = False,
		reportType = 'B',
		*args,
		**kwargs):

		"""
		Defines a Report Set.

		A report set can be a single report with one or more groups.

		If the runSeparate parameter is True, then a Report Set defines the the combined report plus each individual report.

		Input
		-----
		id (int): a unique identifier for the report set

		grpnums (str): a list containing all the group numbers that will go into the experience or utilization report

		conn (object): a database connection object

		fromdate (str): the start date of the experience report

		thrudate (str): the end date of the experience report

		title (str): the title that will be displayed in the report

		runSeparate (bool): indicates whether each group in the group number list will be run as an individual report

		reportType (str): must take one of three values

			"U": utilization report only

			"E": experience report only

			"B": both

		"""

		self.id = id

		if isinstance(grpnums, list):
			self.grpnums_input = grpnums
		else:
			self.grpnums_input = list(grpnums)

		self.runSeparate = runSeparate
		self.conn = conn

		if reportType.upper() in ["B", "U", "E"]:
			self.reportType = reportType
		else:
			raise Exception("reportType must be one of the values, 'B'oth, 'E'xperience, or 'U'tilization")

		if title is not None:
			self.title = title
		elif len(self.grpnums_input) > 1:
			self.title = "Multiple Groups"
		else:
			self.title = "Single Group"

		# set thru date and from date
		today = datetime.datetime.today()
		if thrudate is None:
			if (today.month in [2, 3, 4]):
				self.thrudate = str(datetime.date(today.year - 1, 12, 31))
			elif (today.month in [5, 6, 7]):
				self.thrudate = str(datetime.date(today.year, 3, 31))
			elif (today.month in [8, 9, 10]):
				self.thrudate = str(datetime.date(today.year, 6, 30))
			elif (today.month in [11, 12]):
				self.thrudate = str(datetime.date(today.year, 9, 30))
			elif today.month == 1:
				self.thrudate = str(datetime.date(today.year - 1, 9, 30))

			thrudt = datetime.datetime.strptime(self.thrudate, "%Y-%m-%d")
			fromdate = str(datetime.date(thrudt.year - 2, 1, 1))
		else:
			self.thrudate = thrudate

		if fromdate is None:
			thrudt = datetime.datetime.strptime(self.thrudate, "%Y-%m-%d")
			self.fromdate = str(datetime.date(thrudt.year - 2, 1, 1))
		else:
			self.fromdate = fromdate

		# this is the main worker function call
		# this creates a list of Report objects within this ReportSet
		self.reportList = self.createReportList(conn)

	def cleanGroupNames(self, grpname):
		"""
			Remove special characters
		"""
		# remove slashes and single quotes
		title = re.sub('[\\/\']', ' ', grpname)
		# remove state code in parentheses
		title = re.sub('\([A-Za-z]{2}\)', '', title)

		return(title)




	def __getGroupData(self, grpnums, conn):
		"""
			Get group information for all groups in report

			Returns a dataframe of the group data
		"""

		sql = """
SELECT P.SYSTEM, P.GRPNUM, P.GRPUID, P.PRMSYSTEM, P.PRMGRPNUM, P.PRMGRPNAME
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

		sql = sql.format(*tuple(["','".join(grpnums)]))
		df_groups = pd.read_sql(sql, conn)
		df_groups.columns = [x.upper() for x in df_groups.columns.tolist()]
		df_groups["PRMGRPNAME"] = df_groups["PRMGRPNAME"].apply(lambda x: self.cleanGroupNames(x))

		self.df_groups = df_groups
		return(df_groups)


	def __getExperienceData(self, grpnums, fromdate, thrudate, conn):
		"""
			Get experience data with GRPUID
		"""

		sql = """
SELECT P.GRPUID, E.*
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

		if isinstance(grpnums, list):
			grpnums_query_input = "','".join(grpnums)
		else:
			grpnums_query_input = grpnums

		sql = sql.format(*tuple([grpnums_query_input, fromdate, thrudate]))
		df_experience = pd.read_sql(sql, conn)
		df_experience.columns = [x.upper() for x in df_experience.columns.tolist()]

		self.df_experience = df_experience
		return(df_experience)



	def __getUtilizationData(self, grpnums, fromdate, thrudate, conn):
		"""
			Get experience data with GRPUID
		"""

		sql = """
SELECT P.GRPUID, U.*
FROM ACT_ACTUARIALDB.TBL_UTILIZATION AS U
INNER JOIN ACT_ACTUARIALDB.PXGRPCTRL AS P ON
	U.GRPNUM = P.GRPNUM
INNER JOIN
	(SELECT DISTINCT GRPUID
	FROM ACT_ACTUARIALDB.PXGRPCTRL
	WHERE GRPNUM IN ('{0}')) AS G
ON
	P.GRPUID = G.GRPUID
WHERE
	EXTRACT(YEAR FROM CAST('{1}' AS DATE)) > 0 AND EXTRACT(YEAR FROM CAST('{2}' AS DATE)) > 0
		"""

		if isinstance(grpnums, list):
			grpnums_query_input = "','".join(grpnums)
		else:
			grpnums_query_input = grpnums

		sql = sql.format(*tuple([grpnums_query_input, fromdate, thrudate]))
		df_utilization = pd.read_sql(sql, conn)
		df_utilization.columns = [x.upper() for x in df_utilization.columns.tolist()]

		self.df_utilization = df_utilization
		return(df_utilization)


	def createReportList(self, conn):

		reportList = []
		data_input = {}
		log = []

		try:
			_ = self.__getGroupData(self.grpnums_input, conn)

			if self.reportType in ["B", "E"]:
				_ = self.__getExperienceData(self.grpnums_input, self.fromdate, self.thrudate, conn)
				exp = self.df_experience.groupby(["LOB", "LOBDESC", "EXPYR"], as_index = False).sum()
				data_input["experience"] = exp

			if self.reportType in ["U", "B"]:
				_ = self.__getUtilizationData(self.grpnums_input, self.fromdate, self.thrudate, conn)
				util = self.df_utilization.groupby(["LOB", "LOBDESC", "BENEFITDESCRIPTION"], as_index = False).sum()
				data_input["utilization"] = util

			report = Report(data = data_input, title = self.title, groupData = self.df_groups, fromdate = self.fromdate, thrudate = self.thrudate)

			wrapper = ReportWrapper(**{
				"reportSetID": self.id,
				"reportID": -1,
				"report": report,
				"status": "Success",
				"reportKey": self.grpnums_input,
				"reportKeyType": "Group Numbers",
				"title": self.title
			})

			reportList.append(wrapper)


		except Exception as err:

			wrapper = ReportWrapper(**{
				"reportSetID": self.id,
				"reportID": -1,
				"status": "Failure",
				"reportKey": self.grpnums_input,
				"reportKeyType": "Group Numbers",
				"title": self.title,
				"reason": "Could not run REPORTSET",
				"error": str(err)
			})
			reportList.append(wrapper)

			return(reportList)

		# -----------------------------------------------
		# if the main section was successful, continue
		# -----------------------------------------------

		if self.runSeparate:

			grpuids = list(set(self.df_groups["GRPUID"]))

			for i, grpuid in enumerate(grpuids):

				data_input = {}

				try:
					df_groups = self.df_groups[self.df_groups["GRPUID"] == grpuid]

				except Exception as err:
					wrapper = ReportWrapper(**{
						"reportSetID": self.id,
						"reportID": i,
						"status": "Failure",
						"reportKey": grpuid,
						"reportKeyType": "Group UID",
						"title": "Unknown",
						"reason": "Could not run REPORTSET",
						"error": str(err)
					})
					reportList.append(wrapper)
					continue

				try:
					title = df_groups[(df_groups["GRPNUM"] == df_groups["PRMGRPNUM"]) & (df_groups["SYSTEM"] == df_groups["PRMSYSTEM"])]["PRMGRPNAME"].values[0]
				except Exception as err:
					wrapper = ReportWrapper(**{
						"reportSetID": self.id,
						"reportID": i,
						"status": "Failure",
						"reportKey": grpuid,
						"reportKeyType": "Group UID",
						"title": "Unknown Group",
						"reason": "Could not find TITLE in single run",
						"error": str(err)
					})
					reportList.append(wrapper)
					continue

				try:
					if self.reportType in ["B", "E"]:
						exp = self.df_experience[self.df_experience["GRPUID"] == grpuid].groupby(["LOB", "LOBDESC", "EXPYR"], as_index = False).sum()
						data_input["experience"] = exp
				except Exception as err:
					wrapper = ReportWrapper(**{
						"reportSetID": self.id,
						"reportID": i,
						"status": "Failure",
						"reportKey": grpuid,
						"reportKeyType": "Group UID",
						"title": title,
						"reason": "Could not find EXPERIENCE in single run",
						"error": str(err)
					})
					reportList.append(wrapper)
					continue

				try:
					if self.reportType in ["U", "B"]:
						util = self.df_utilization[self.df_utilization["GRPUID"] == grpuid].groupby(["LOB", "LOBDESC", "BENEFITDESCRIPTION"], as_index = False).sum()
						data_input["utilization"] = util
				except Exception as err:
					wrapper = ReportWrapper(**{
						"reportSetID": self.id,
						"reportID": i,
						"status": "Failure",
						"reportKey": grpuid,
						"reportKeyType": "Group UID",
						"title": title,
						"reason": "Could not find UTILIZATION in single run",
						"error": str(err)
					})
					reportList.append(wrapper)
					continue

				try:
					report = Report(data = data_input, title = title, groupData = df_groups, fromdate = self.fromdate, thrudate = self.thrudate)

				except Exception as err:
					wrapper = ReportWrapper(**{
						"reportSetID": self.id,
						"reportID": i,
						"status": "Failure",
						"reportKey": grpuid,
						"reportKeyType": "Group UID",
						"title": title,
						"reason": "Could not create single run report",
						"error": str(err)
					})
					reportList.append(wrapper)
					continue

				else:
					wrapper = ReportWrapper(**{
						"reportSetID": self.id,
						"reportID": i,
						"report": report,
						"status": "Success",
						"reportKey": grpuid,
						"reportKeyType": "Group UID",
						"title": title
					})
					reportList.append(wrapper)


		return(reportList)
