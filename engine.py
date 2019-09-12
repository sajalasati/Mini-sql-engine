import sys
import sqlparse
import re

class Database:
	def __init__(self):
		self.table_names = [] # all the table names
		self.tables = {} # dictionary of table objects, accessed by Tnames
	def add_table_name(self,Tname):
		self.table_names.append(Tname)
	def add_table(self,Tname,Tobj):
		self.tables[Tname]=Tobj

class Table:
	def __init__(self,name,cols):
		self.name = name
		self.cols = [name+"."+c for c in cols]
		self.rowsnum = 0
		self.records = []
		self.col_dict = {} # col_dict['col_name']=all entries of that column
		self.init_col_dicts()
		self.read()
	def init_col_dicts(self):
		for c in self.cols:
			self.col_dict[c]=[]
	def read(self):
		self.fname = self.name + ".csv"	
		with open(self.fname) as f:
			x = f.readline()
			while x:
				x = x.split(",")
				for i in range(len(self.cols)):
					x[i] = x[i].strip('\"\n\r')
					self.col_dict[self.cols[i]].append(int(x[i]))
				self.rowsnum += 1
				x = f.readline()

def load_tables(metadata_file,db):
	f = open(metadata_file,"r")
	line = f.readline()
	while line:
		line = line.split()[0]
		if line == '<begin_table>':
			cols = []
			line = f.readline()
			if not line or line == '<end_table>':
				return -1
			line = line.split()[0]
			Tname = line
			line = f.readline()
			if not line:
				return -1
			while line and line.split()[0] != '<end_table>':
				cols.append(line.split()[0])
				line = f.readline()
			if not line:
				return -1
			db.add_table_name(Tname)
			db.add_table(Tname,Table(Tname,cols))
		else:
			return -1
		line = f.readline()
	return 0

db = Database()
ret = load_tables("metadata.txt",db)
if ret == -1:
	print("Error in schema given in the metadata file. Please check.")
	exit(0)

data = {}
new_data = {}
len_data = [0] # plus one during insertion of entries of last table in recursive join
len_data2 = [0]
aggregates = {"sum":1,"avg":2,"max":3,"min":4}
operators = {"<":1, ">":2, "<=":3, ">=":4, "=":5}
is_distinct = [0]

def print_error():
	print("Error in query. Please check syntax.")
	exit(0)

def recursive_func(Tnames,n,old_dict):
	table = db.tables[Tnames[n]]
	for i in range(table.rowsnum):
		for col in table.cols:
			old_dict[col] = table.col_dict[col][i]
		if n==len(Tnames)-1:
			for key in list(old_dict.keys()):
				data[key].append(old_dict[key])
			len_data[0] += 1
		else:
			recursive_func(Tnames,n+1,old_dict)

def result_from(q):
	''' returns a list of tables to be joined
		and it assumes where part is removed already
	'''
	Tnames = [T.strip(' ') for T in q.split(',')]
	for T in Tnames:
		if not T in db.table_names:
			print("Table "+str(T)+" does not exist in database")
			exit(0)
		for col in db.tables[T].cols:
			data[col]=[]
	recursive_func(Tnames,0,{})
	return Tnames

def get_full_column_name(name,Tnames):
	''' return Tname.colname for non empty "name" with all the validation checks
		i.e. Exactly one table should exist for this column, Table should exist,
		and also checks for ambiguity
	'''
	if name=="":
		return ""
	full_name=name
	if "." in name:
		# means table name is present too
		T,col = name.split('.')[0],name.split('.')[1]
		if T=="" or col=="":
			print_error()
		if T in Tnames and T in db.table_names:
			pass
		elif not T in db.table_names:
			print("Table "+str(T)+" either not quried or does not exist in database. Please recheck the query")
			exit(0)
		flag=0
		for x in db.tables[T].cols:
			if name==x:
				flag=1
		if flag==0:
			print("No column named \""+str(col)+"\" found in table \""+str(T)+"\"")
			exit(0)
	else:
		T=""
		col = name.strip(' ')
		ct=0
		for tab in Tnames:
			if tab in db.table_names:
				if tab+"."+col in db.tables[tab].cols:
					T = tab
					ct+=1
			else:
				print("Table "+str(tab)+" does not exist in database")
				exit(0)
		if ct==0:
			print("Column \""+str(col)+"\" does not belong to any table specified")
			exit(0)
		elif ct>1:
			print("Column name \""+str(col)+"\" is ambiguous among the queried tables")
			exit(0)
		full_name = str(T)+"."+str(col)
	return full_name

def check_row(val1,val2,op_type):
	op_type = int(op_type)
	val1=int(val1)
	val2=int(val2)
	if op_type == 1 and val1<val2:
		return True
	if op_type == 2 and val1>val2:
		return True
	if op_type == 3 and val1<=val2:
		return True
	if op_type == 4 and val1>=val2:
		return True
	if op_type == 5 and val1==val2:
		return True
	return False

def find_join_cols_where(where_query,Tnames):
	'''returns join columns if any'''
	join_op_type, join_col1, join_col2 = 0,"",""
	join_query = ""
	for op,k in operators.items():
		join_regex = r"[A-Za-z]+[A-Za-z0-9]*\.[A-Za-z]+[A-Za-z0-9]*[ ]*"+str(op)+r"[ ]*[A-Za-z]+[A-Za-z0-9]*\.[A-Za-z]+[A-Za-z0-9]*"
		join_regex2 = r"[A-Za-z]+[A-Za-z0-9]*[ ]*"+str(op)+r"[ ]*[A-Za-z]+[A-Za-z0-9]*"
		res1 = re.findall(join_regex,where_query)
		if res1 != []:
			join_query = res1[0].strip(' ')
			join_col1 = get_full_column_name(re.split(str(op),join_query)[0].strip(' '),Tnames)
			join_col2 = get_full_column_name(re.split(str(op),join_query)[1].strip(' '),Tnames)
		else:
			res2 = re.findall(join_regex2,where_query)
			if res2!=[]:
				join_query = res2[0].strip(' ')
				join_col1 = get_full_column_name(re.split(str(op),join_query)[0].strip(' '),Tnames)
				join_col2 = get_full_column_name(re.split(str(op),join_query)[1].strip(' '),Tnames)		
		if join_col1 != "":
			join_op_type = k
			break
	return join_op_type, join_col1, join_col2, join_query

def find_norm_cols_where(where_query,Tnames):
	norm_cols,norm_vals,norm_op_type = [],[],[]
	comp_type=""

	if "AND" in where_query:
		comp_type="AND"
	if "OR" in where_query:
		comp_type="OR"
	if comp_type!="":
		left = re.split(comp_type,where_query)[0].strip(' .;,!?')
		right = re.split(comp_type,where_query)[1].strip(' .;,!?')
		for op,k in operators.items():
			res = []
			norm_regex1 = r"^[a-zA-Z]+[A-Za-z0-9]*\.[A-Za-z0-9]+[ ]*"+str(op)+r"[ ]*[-]*[0-9]+"
			norm_regex2 = r"^[A-Za-z]+[A-Za-z0-9]*[ ]*"+str(op)+r"[ ]*[-]*[0-9]+"
			for st in re.findall(norm_regex1,left):
				res.append(st)
			for st in re.findall(norm_regex1,right):
				res.append(st)
			for st in re.findall(norm_regex2,left):
				res.append(st)
			for st in re.findall(norm_regex2,right):
				res.append(st)
			for pat in res:
				norm_cols.append(get_full_column_name(pat.split(str(op))[0].strip(' '),Tnames))
				norm_vals.append(pat.split(str(op))[1].strip(' '))
				norm_op_type.append(k)		
	else:
		where_query = where_query.strip(' ,.!?')
		for op,k in operators.items():
			norm_regex1 = r"^[A-Za-z]+[A-Za-z0-9]*\.[A-Za-z0-9]+[ ]*"+str(op)+r"[ ]*[-]*[0-9]+"
			norm_regex2 = r"^[A-Za-z]+[A-Za-z0-9]*[ ]*"+str(op)+r"[ ]*[-]*[0-9]+"
			res = []
			for st in re.findall(norm_regex1,where_query):
				res.append(st)
			for st in re.findall(norm_regex2,where_query):
				res.append(st)
			for pat in res:
				norm_cols.append(get_full_column_name(pat.split(str(op))[0].strip(' '),Tnames))
				norm_vals.append(pat.split(str(op))[1].strip(' '))
				norm_op_type.append(k)
	return norm_cols,norm_vals,norm_op_type

def process_where(where_query,Tnames):
	
	# fill the norm_cols and norm_vals associated with them
	norm_cols,norm_vals,norm_op_type = find_norm_cols_where(where_query,Tnames)
	# find join columns query if any
	join_op_type, join_col1,join_col2, join_query = find_join_cols_where(where_query,Tnames)


	comp_type=""
	if "AND" in where_query:
		comp_type="AND"
	if "OR" in where_query:
		comp_type="OR"

	for key in list(data.keys()):
		new_data[key]=[]
	for i in range(len_data[0]):
		flag=True
		if comp_type=="":
			if join_query!="":
				flag = check_row(data[join_col1][i],data[join_col2][i],join_op_type)
			else:
				if len(norm_vals)==0:
					print_error()
				flag = check_row(data[norm_cols[0]][i],norm_vals[0],norm_op_type[0])
		else:
			flag1,flag2=True,True
			if join_query!="":
				flag1 = check_row(data[join_col1][i],data[join_col2][i],join_op_type)
				if len(norm_vals)<1:
					print_error()
				flag2 = check_row(data[norm_cols[0]][i],norm_vals[0],norm_op_type[0])
			else:
				if len(norm_vals)<2:
					print_error()
				flag1 = check_row(data[norm_cols[0]][i],norm_vals[0],norm_op_type[0])
				flag2 = check_row(data[norm_cols[1]][i],norm_vals[1],norm_op_type[1])
			if comp_type=="AND":
				flag = flag1 and flag2
			else:
				flag = flag1 or flag2
		if flag==True:
			len_data2[0] += 1
			for key in list(data.keys()):
				new_data[key].append(data[key][i])


def print_query_result(new_data,row_list,len_data2):
	if row_list==[]:
		print_error()
	if is_distinct[0]==1:
		distinct_data = []
		for i in range(len_data2[0]):
			lst = []
			n = len(row_list)
			flag=0
			for r in row_list:
				lst.append(new_data[r][i])
			for j in range(len(distinct_data)):
				flag2=0
				for k in range(n):
					if distinct_data[j][k]==lst[k]:
						flag2 += 1
				if flag2==n:
					flag+=1
			if flag==0:
				distinct_data.append(lst)
		print("Query OK. " + str(len(distinct_data)) + " results.")
		if len(distinct_data)>0:
			for r in row_list:
				print(r,end="\t")
			print(end="\n")
		for i in range(len(distinct_data)):
			for j in range(len(distinct_data[0])):
				print(distinct_data[i][j],end="\t\t")
			print(end="\n")
	else:
		print("Query OK. " + str(len_data2[0]) + " results.")
		if len_data2[0]>0:
			for r in row_list:
				print(r,end="\t")
			print(end="\n")
		for i in range(len_data2[0]):
			for r in row_list:
				print(new_data[r][i],end="\t\t")
			print(end="\n")

def check_if_aggregate(select_query,Tnames):
	col,agg_type="",""
	for key in list(aggregates.keys()):
		if key in select_query:
			col = select_query.replace(key,"")
			col = col.strip('()')
			agg_type = key
			break
	col = get_full_column_name(col,Tnames)
	return col,agg_type

def apply_aggregate(new_data,col,agg_type,len_data2):
	# apply, print and exit here only
	if len_data2[0]==0:
		print("Query Ok. No rows.")
	else:
		print("Query Ok. 1 Result")
		print(str(agg_type)+"("+str(col)+")")
		if agg_type == "sum":
			print(sum(new_data[col]))
		elif agg_type == "avg":
			avg = sum(new_data[col])/len(new_data[col])
		elif agg_type == "max":
			print(max(new_data[col]))
		else:
			print(min(new_data[col]))
	exit(0)
		
def process_select(data,select_query, Tnames,len_data):
	if "DISTINCT" in select_query:
		is_distinct[0]+=1
	if is_distinct[0]==1:
		temp = re.split("DISTINCT",select_query)[0].strip(' ')
		select_query = re.split("DISTINCT",select_query)[1].strip(' ')
		if select_query=="" or temp!="":
			print_error()
	all_columns = 0
	col,agg_type = check_if_aggregate(select_query,Tnames)
	if agg_type!="":
		if col=="":
			print_error()
		else:
			apply_aggregate(data,col,agg_type,len_data)
	else:
		raw_col_list = re.split(",",select_query)
		raw_col_list2 = [r.strip(" ") for r in raw_col_list]
		for  idx,r in enumerate(raw_col_list2):
			if r=="*":
				all_columns += 1
				del raw_col_list2[idx]
		cols_list = [get_full_column_name(r,Tnames) for r in raw_col_list2 if r != ""]
	if all_columns > 1:
		print_error()
	if all_columns==1:
		cols_list = list(data.keys())
	return cols_list

def basic_checking(q):
	if ";" not in q:
		print("query must end with a semicolon")
		exit(0)


def query_parsing(q):
	'''	query structure:
		SELECT ..(MAX etc, DISTINCT).. FROM (tuple of tables) WHERE ....
	'''
	q = sqlparse.format(q,keyword_case='upper')
	basic_checking(q)
	q=q.strip(";")
	if "SELECT" in q and "FROM" in q:
		pass
	else:
		print_error()
	
	# extracting different parts of the queries
	table_names=[]
	select_query = ""
	from_query = ""
	where_query = ""
	
	temp = q # "select" + "from" part of complete query
	if "WHERE" in q:
		temp = re.split('WHERE',q)[0].strip(' ')
		where_query = re.split('WHERE',q)[1].strip(' ')
	temp2 = re.split('FROM',temp)[0].strip(' ')
	select_query = re.split('SELECT',temp2)[1].strip(' ')
	from_query = re.split('FROM',temp)[1].strip(' ')

	# construct joined table of "from" statement
	Tnames = result_from(from_query)
	for T in Tnames:
		if T not in db.table_names:
			print("Table "+str(T)+" does not exit in database")
			exit(0)

	# apply "where" condition to it and reduce rows: these 2 types
		# "where" to filter rows: of joined or not joined tables coming after "from" table
		# "where" for joining: keep only those rows where that one join condition is given
	if where_query != "":
		process_where(where_query,Tnames)
		select_cols_list = process_select(new_data,select_query,Tnames,len_data2)
		print_query_result(new_data,select_cols_list,len_data2)
	else:
		select_cols_list = process_select(data,select_query,Tnames,len_data)
		print_query_result(data,select_cols_list,len_data)

q = query_parsing(sys.argv[1])