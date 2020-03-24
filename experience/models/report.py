import pandas as pd

class Report():

	def __init__(self, data, title, fromdate, thrudate, groupData, *args, **kwargs):
		"""
		Defines an object for a single report.

		Input:
		------
		data: a dictionary containing at least one of the following key-value pairs

			experience: a data frame containing all the experience data

			utilization: a data frame containing all the utilization data

		title: a title for the report

		fromdate: the report from date (used for printing only)

		thrudate: the report thru date (used for printing only)

		groupData: contains the group level data for all the groups in the report

		"""

		hasUtilizationData = False
		hasExperienceData = False

		self.title = title
		self.df_groups = pd.DataFrame(groupData)

		self.output = {
			"groups": self.df_groups.to_dict("records"),
			"fromdate": fromdate,
			"thrudate": thrudate,
			"title": title
		}

		if data.get("experience") is not None:
			self.df_experience = pd.DataFrame(data.get("experience"))
			self.output["experience"] = self.formatExperienceData()
			hasExperienceData = True

		if data.get("utilization") is not None:
			self.df_utilization = pd.DataFrame(data.get("utilization"))
			self.output["utilization"] = self.formatUtilizationData()
			hasUtilizationData = True


		if (not hasUtilizationData and not hasExperienceData):
			raise Exception("Input data does not contain experience or utilization")

	def formatExperienceData(self):
		"""
			Formats experience data for HTML injection
		"""
		df = self.df_experience.sort_values(["LOB", "EXPYR"])
		df["LOSSRATIO"] = df["INCCLAIMS"] / df["EARNEDPREM"]
		data = []
		list_lob = list(set(df["LOB"]))
		list_lob.sort()
		for lob in list_lob:
			d = {}
			d["name"] = df[df["LOB"] == lob]["LOBDESC"].values[0]
			d["Year"] = df[df["LOB"] == lob]["EXPYR"].values.tolist()
			d["EarnedPremium"] = ['${:,.2f}'.format(y) for y in df[df["LOB"] == lob]["EARNEDPREM"].values.tolist()]
			d["IncurredClaims"] = ['${:,.2f}'.format(y) for y in df[df["LOB"] == lob]["INCCLAIMS"].values.tolist()]
			d["LossRatio"] = ['{:,.2%}'.format(y) for y in df[df["LOB"] == lob]["LOSSRATIO"].values.tolist()]
			data.append(d)
		return(data)

	def formatUtilizationData(self, minUtilization = 5, other_text = "Other"):
		"""
			Formats experience data for HTML injection
		"""
		# format the data frame
		df = self.df_utilization.groupby(["LOB", "LOBDESC","BENEFITDESCRIPTION"], as_index = False).sum()
		df["AdjBenefitDescription"] = df.apply(lambda x: other_text if x["UTILIZATION"] < minUtilization else x["BENEFITDESCRIPTION"], axis = 1)
		df["SortOrder"] = df["AdjBenefitDescription"].apply(lambda x: "B" if x == "Other" else "A")
		df = df.groupby(["LOB", "LOBDESC", "SortOrder", "AdjBenefitDescription"], as_index = False).sum()
		df = df.sort_values(["LOBDESC", "SortOrder", "UTILIZATION"], ascending = [True, True, False])

		data = []
		list_lob = list(set(df["LOB"]))
		list_lob.sort()
		for lob in list_lob:
			d = {}
			d["name"] = df[df["LOB"] == lob]["LOBDESC"].values[0]
			d["records"] = df[df["LOB"] == lob][["AdjBenefitDescription", "UTILIZATION"]].to_dict("records")
			data.append(d)

		return(data)
