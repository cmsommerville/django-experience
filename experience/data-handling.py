import datetime 


def df2dict(df): 
	"""
		Accepts a Pandas dataframe. Returns a dictionary formatted for HTML injection
	"""
    df = df.sort_values(["SYSTEM", "GRPNUM", "LOB", "BLUEBOOK", "IncurredDate"])
    df["LossRatio"] = df["IncurredClaims"] / df["EarnedPremium"]
    data = []
    list_lob = list(set(df["LOB"]))
    for lob in list_lob: 
        d = {}
        d["name"] = lob
        d["Year"] = df[df["LOB"] == lob]["IncurredDate"].values.tolist()
        d["Earned Premium"] = ['${:,.2f}'.format(y) for y in df[df["LOB"] == lob]["EarnedPremium"].values.tolist()]
        d["Incurred Claims"] = ['${:,.2f}'.format(y) for y in df[df["LOB"] == lob]["IncurredClaims"].values.tolist()]
        d["Loss Ratio"] = ['{:,.2%}'.format(y) for y in df[df["LOB"] == lob]["LossRatio"].values.tolist()]
        data.append(d)
    return({"data": data})


def createQueryString(
	grpnum, 
	fromdate = None, 
	thrudate = None, 
	*args, **kwargs): 
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
		
	
	
	

def queryData(data, cursor, *args, **kwargs): 
	
	"""
		Query the database
	"""
	df_dict = {}
	
	try: 

		# get query string 
		sql = createQueryString(**data)
		
		# query data 
		df = cursor.execute(sql).fetchall()
		df = pd.DataFrame.from_records(df)
		df.columns = [x[0] for x in cursor.description]
		
		df_dict = df2dict(df)
		
	except: 
		raise Exception("Could not query data")
	
	return(df_dict)
	


def generateHTMLReport(df_dict, html_template, *args, **kwargs): 
	"""
		Injects the data into the HTML template file
	""" 
	return(1)


if __name__ == "__main__": 

	
	
	
	if conn is None: 
		if conn_string is None: 
			raise Exception("No database connection or connection string was provided.")
		else: 
			conn = pyodbc.connect(conn_string)
			
	cursor = conn.cursor()
	
	
	cursor.close()
	conn.close()

	print(createQueryString(grpnum=["0000001000", "0000002000"]))
		
		