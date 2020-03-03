import datetime 
import sys 
import json 
import pyodbc 
import os 

class ExperienceReport(): 

	def __init__(self,  
		DSN = None, 
		conn_string = None, 
		config = None, 
		grpnum = None, 
		fromdate = None, 
		thrudate = None, 
		title = None,
		*args, **kwargs): 
	
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
		else:  
			self.conn = pyodbc.connect(conn_string)

		try: 
			html_template_name = kwargs.get("html_template", os.path.join(os.path.join(os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "static"), "experience"), "base.html"))
			
			with open(html_template_name, 'r') as f: 
				self.html_template = f.read()
		except: 
			raise Exception("Cannot read HTML template")


	def df2dict(self, df): 
		"""
			Accepts a Pandas dataframe. Returns a dictionary formatted for HTML injection
		"""
		df = df.sort_values("IncurredDate")
		df["LossRatio"] = df["IncurredClaims"] / df["EarnedPremium"]
		data = []
		list_lob = list(set(df["LOBDesc"]))
		for lob in list_lob: 
			d = {}
			d["name"] = lob
			d["Year"] = df[df["LOBDesc"] == lob]["IncurredDate"].values.tolist()
			d["Earned Premium"] = ['${:,.2f}'.format(y) for y in df[df["LOB"] == lob]["EarnedPremium"].values.tolist()]
			d["Incurred Claims"] = ['${:,.2f}'.format(y) for y in df[df["LOB"] == lob]["IncurredClaims"].values.tolist()]
			d["Loss Ratio"] = ['{:,.2%}'.format(y) for y in df[df["LOB"] == lob]["LossRatio"].values.tolist()]
			data.append(d)
		return({"data": data})


	def createQueryString(self, grpnum, fromdate = None, thrudate = None, *args, **kwargs): 
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
			
		"""
		queryTemplate = "EXEC Act_PricingTN.mcr_ClaimExperience('{0}', '{1}', '{2}')"
		
		today = datetime.date.today()
		list_grpnum = grpnum 
		dt_from = fromdate 
		dt_thru = thrudate 

		if thrudate is None: 
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
		
		if isinstance(grpnum, list) == False: 
			list_grpnum = [grpnum]
		
		
		sql = queryTemplate.format(*(','.join(list_grpnum), str(dt_from), str(dt_thru)))
		
		return(sql)
			
		
		

	def queryData(self, sql, cursor, *args, **kwargs): 
		
		"""
			Query the database
		"""
		df = pd.DataFrame()
		
		try: 
			# query data 
			df = cursor.execute(sql).fetchall()
			df = pd.DataFrame.from_records(df)
			df.columns = [x[0] for x in cursor.description]
			
		except: 
			raise Exception("Could not query data")
		
		return(df)
		


	def renderReport(self, df, html_template, runSeparate, *args, **kwargs): 
		"""
			Injects the data into the HTML template file
		""" 
		try: 
			# create consolidated report 
			experience = df.groupby(["LOBDesc", "IncurredDate"])["EarnedPremium", "IncurredClaims", "InforceCerts"].sum().reset_index()
			dict_exp = self.df2dict(experience)
			# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
			#
			#	render and export HTML here 
			#
			# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		except: 
			raise Exception("Could not create consolidated experience report")
		
		try: 
			if runSeparate: 
				grp_list = list(set(df["GRPNUM"]))
				for grp in grp_list: 
					experience = df[df["GRPNUM"] == grp].groupby(["LOBDesc", "IncurredDate"])["EarnedPremium", "IncurredClaims", "InforceCerts"].sum().reset_index()
					dict_exp = self.df2dict(experience)
					# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
					#
					#	render and export HTML here 
					#
					# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
		except: 
			raise Exception("Could not create separate experience reports") 

	
	def reportLoop(self, html_template): 
		
		cursor = self.cursor()
		
		try: 
			for data in self.request_data: 
				runSeparate = data.get("runSeparate", False)
				sql = self.createQueryString(**data)
				df = self.queryData(sql, cursor)
				self.renderReport(df, html_template, runSeparate)

		finally: 
			cursor.close()
			



if __name__ == "__main__": 

	# read configuration 
	
	with open(sys.argv[1], "r") as j: 
		config = json.load(j)
		
	experience_request = ExperienceReport(**config)
	#experience_request.reportLoop()
	print(experience_request.html_template)
		