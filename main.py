import sqlparse
from sqlparse.sql import *
import csv
from math import *
import sys

def get_metadata():
	f = open('metadata.txt','r')
	tables_schema = dict()
	for line in f:
		line = line[:-1]
		if line == "<begin_table>":
			line = f.readline()[:-1]
			tables_schema[line] = []
			curr_table = line
			line = f.readline()[:-1]
			while line != "<end_table>":
				tables_schema[curr_table].append(line.lower())
				line = f.readline()[:-1]
	return tables_schema

def get_tables_from_query(parsed):
	tables = []
	id_list = sqlparse.sql.IdentifierList(parsed.tokens).get_identifiers()
	from_seen = False
	for ident in id_list:
		if isinstance(ident,IdentifierList) and from_seen:
			for rd in ident:
				if rd.value != ',' and rd.value != ' ':
					tables.append(rd.value)
			return tables
		elif from_seen:
			tables.append(ident.value)
			return tables
		if ident.value.upper() == "FROM":
			from_seen = True

def get_columns_from_query(parsed):
	cols = []
	id_list = sqlparse.sql.IdentifierList(parsed.tokens).get_identifiers()
	select_seen = False
	for ident in id_list:
		# print("danz",type(ident))
		if isinstance(ident,IdentifierList) and select_seen:
			for rd in ident:
				if rd.value != ',' and rd.value != ' ':
					cols.append(rd.value.lower())
			return cols
		elif select_seen and ident.value.upper() != "DISTINCT":
			cols.append(ident.value.lower())
			return cols
		if ident.value.upper() == "SELECT":
			select_seen = True

def get_groupby_col(parsed):
	cols = []
	id_list = sqlparse.sql.IdentifierList(parsed.tokens).get_identifiers()
	grpby_seen = False
	for ident in id_list:
		# print("danz",type(ident))
		if isinstance(ident,IdentifierList) and grpby_seen:
			for rd in ident:
				if rd.value != ',' and rd.value != ' ':
					cols.append(rd.value)
			return cols
		elif grpby_seen:
			cols.append(ident.value)
			return cols
		if ident.value.upper() == "GROUP BY":
			grpby_seen = True

def get_orderby_col(parsed):
	col = None
	asc = True
	id_list = sqlparse.sql.IdentifierList(parsed.tokens).get_identifiers()
	ordby_seen = False
	for ident in id_list:
		if ordby_seen:
			ident_split = ident.value.split()
			cols = ident_split[0]
			if len(ident_split) > 1 and ident_split[1].upper() == "DESC":
				asc = False
			return cols,asc
		if ident.value.upper() == "ORDER BY":
			ordby_seen = True
	return col,asc


def get_where_condition(parsed):
	# cond = dict()
	# cond['comp'] = []
	expr = ''
	id_list = sqlparse.sql.IdentifierList(parsed.tokens).get_identifiers()
	for ident in id_list:
		if isinstance(ident,Where):
			for rd in ident:
				# print(rd,type(rd),"kk")
				if isinstance(rd,Comparison):
					expr += rd.value.lower() + ' '
					# cond['comp'].append(rd.value)
				elif rd.value != ',' and rd.value != ' ' and rd.value.upper() != 'WHERE':
					expr += rd.value.lower() + ' '
					# cond['op'] = rd.value
			return expr

def isDistinctPresent(parsed):
	id_list = sqlparse.sql.IdentifierList(parsed.tokens).get_identifiers()
	for ident in id_list:
		if ident.value.upper() == "DISTINCT":
			return True
	return False

def get_table_data(table):
	tab = []
	with open(table+'.csv','r') as csvfile:
		csvreader = csv.reader(csvfile)
		for row in csvreader:
			temp = []
			for elem in row:
				temp.append(int(elem.replace('"','')))
			tab.append(temp)
	return tab

def printlistoflist(res):
	for elem in res[0]:
		print(elem, end = " ")
	print()
	for row in res[1:]:
		for elem in row[:-1]:
			print(elem,end=",\t")
		print(row[-1])
	print()
	print(len(res)-1,"rows fetched!")

def cartesian(tables,i):
	if i == len(tables)-1:
		return get_table_data(tables[i])
	res = []
	sub_part = cartesian(tables,i+1)
	this_t = get_table_data(tables[i])
	for r1 in this_t:
		for r2 in sub_part:
			res.append(r1+r2)
	return res

def get_index_in_cartesian(q_tables,tables_schema,col_name):
	i = 0
	for table in q_tables:
		for row in tables_schema[table]:
			if row.lower() == col_name:
				return i
			i += 1
	return -1

def remove_rows_as_condition(res,q_where,tables_schema,q_tables):
	ans = []
	# print("expr = ",q_where)
	expr = q_where.split(' ')
	other_terms = ['and','or','','>=','>','<','<=']
	for i in range(len(expr)):
		if expr[i] == '=':
			expr[i] = '=='
		elif not expr[i].isdigit() and expr[i] not in other_terms:
			ind = get_index_in_cartesian(q_tables,tables_schema,expr[i])
			if ind == -1:
				print("Column does not exists!")
				return []
			expr[i] = 'row['+str(ind)+']'
	expr = ' '.join(expr)
	# print(expr)
	for row in res:
		if eval(expr,{"row":row}):
			ans.append(row)
	return ans

def get_count(target_ind,res,grpby_col_set,grpby_ind):
	ans = dict()
	for row in res:
		if row[grpby_ind] not in ans.keys():
			ans[row[grpby_ind]] = 0
		ans[row[grpby_ind]] += 1
	ans_list = []
	for elem in grpby_col_set:
		ans_list.append(ans[elem])
	return ans_list

def get_sum(target_ind,res,grpby_col_set,grpby_ind):
	ans = dict()
	for row in res:
		if row[grpby_ind] not in ans.keys():
			ans[row[grpby_ind]] = 0
		ans[row[grpby_ind]] += row[target_ind]
	ans_list = []
	for elem in grpby_col_set:
		ans_list.append(ans[elem])
	return ans_list

def get_max(target_ind,res,grpby_col_set,grpby_ind):
	ans = dict()
	for row in res:
		if row[grpby_ind] not in ans.keys():
			ans[row[grpby_ind]] = -sys.maxsize -1
		ans[row[grpby_ind]] = max(ans[row[grpby_ind]],row[target_ind])
	ans_list = []
	for elem in grpby_col_set:
		ans_list.append(ans[elem])
	return ans_list

def get_min(target_ind,res,grpby_col_set,grpby_ind):
	ans = dict()
	for row in res:
		if row[grpby_ind] not in ans.keys():
			ans[row[grpby_ind]] = sys.maxsize
		ans[row[grpby_ind]] = min(ans[row[grpby_ind]],row[target_ind])
	ans_list = []
	for elem in grpby_col_set:
		ans_list.append(ans[elem])
	return ans_list

def get_avg(target_ind,res,grpby_col_set,grpby_ind):
	ans = dict()
	count = dict()
	for row in res:
		if row[grpby_ind] not in ans.keys():
			ans[row[grpby_ind]] = 0
			count[row[grpby_ind]] = 0
		ans[row[grpby_ind]] += row[target_ind]
		count[row[grpby_ind]] += 1

	for elem in ans.keys():
		ans[elem] /= 1.0* count[elem]
	
	ans_list = []
	for elem in grpby_col_set:
		ans_list.append(ans[elem])
	return ans_list


def after_groupby(res,groupby_col,q_columns_rem,ind,q_tables,tables_schema):
	g_col_set = set()
	for row in res:
		g_col_set.add(row[ind])
	final_columns = [] 
	for aggr_fun in q_columns_rem:
		if aggr_fun.find("count(") != -1:
			col = aggr_fun[aggr_fun.find('(')+1:aggr_fun.find(')')]
			temp_col = get_count(get_index_in_cartesian(q_tables,tables_schema,col),res,g_col_set,ind)
		elif aggr_fun.find("sum(") != -1:
			col = aggr_fun[aggr_fun.find('(')+1:aggr_fun.find(')')]
			temp_col = get_sum(get_index_in_cartesian(q_tables,tables_schema,col),res,g_col_set,ind)
		elif aggr_fun.find("max(") != -1:
			col = aggr_fun[aggr_fun.find('(')+1:aggr_fun.find(')')]
			temp_col = get_max(get_index_in_cartesian(q_tables,tables_schema,col),res,g_col_set,ind)
		elif aggr_fun.find("min(") != -1:
			col = aggr_fun[aggr_fun.find('(')+1:aggr_fun.find(')')]
			temp_col = get_min(get_index_in_cartesian(q_tables,tables_schema,col),res,g_col_set,ind)
		elif aggr_fun.find("avg(") != -1:
			col = aggr_fun[aggr_fun.find('(')+1:aggr_fun.find(')')]
			temp_col = get_avg(get_index_in_cartesian(q_tables,tables_schema,col),res,g_col_set,ind)
		elif aggr_fun.find("average(") != -1:
			col = aggr_fun[aggr_fun.find('(')+1:aggr_fun.find(')')]
			temp_col = get_avg(get_index_in_cartesian(q_tables,tables_schema,col),res,g_col_set,ind)
		else:
			print("Give Aggregate functions with 'group by'")
			return None
		final_columns.append(temp_col)
	ans = []
	for i, elem in enumerate(g_col_set):
		temp_row = []
		temp_row.append(elem)
		for col in final_columns:
			temp_row.append(col[i])
		ans.append(temp_row)
	return ans

def anyaggr_found(col):
	if col.find("sum(")!=-1 or col.find("max(")!=-1 or col.find("min(")!=-1 or col.find("avg(")!=-1 or col.find("count(")!=-1 or col.find("average(")!=-1:
		return True
	return False
def remove_aggr_from_colnames(cols):
	pure_col_names = []
	for col in cols:
		if anyaggr_found(col):
			pure_col_names.append(col[col.find('(')+1:col.find(')')])
		else:
			pure_col_names.append(col)
	return pure_col_names

def add_tablename_with_aggr(cols,q_tables,tables_schema):
	pure_col_names = remove_aggr_from_colnames(cols)
	# print("after = ",pure_col_names)
	for i, col in enumerate(cols):
		found = False
		for table in q_tables:
			for col_name in tables_schema[table]:
				if pure_col_names[i] == col_name:
					cols[i] = table+"."+cols[i]
					found = True
					break
			if found:
				break
	return cols

def get_sum_without_grpby(res,ind):
	ans = 0
	for row in res:
		ans += row[ind]
	return ans
def get_count_without_grpby(res,ind):
	ans = 0
	for row in res:
		ans += 1
	return ans
def get_avg_without_grpby(res,ind):
	return 1.0* get_sum_without_grpby(res,ind)/get_count_without_grpby(res,ind)
def get_min_without_grpby(res,ind):
	ans = sys.maxsize
	for row in res:
		ans = min(ans,row[ind])
	return ans
def get_max_without_grpby(res,ind):
	ans = -sys.maxsize -1
	for row in res:
		ans = max(ans,row[ind])
	return ans

def after_aggr_without_groupby(res,col_aggr, indices_in_res):
	ans = []
	for i,ind in enumerate(indices_in_res):
		if col_aggr[i].find("sum(") != -1:
			ans.append(get_sum_without_grpby(res,ind))
		if col_aggr[i].find("avg(") != -1:
			ans.append(get_avg_without_grpby(res,ind))
		if col_aggr[i].find("min(") != -1:
			ans.append(get_min_without_grpby(res,ind))
		if col_aggr[i].find("max(") != -1:
			ans.append(get_max_without_grpby(res,ind))
		if col_aggr[i].find("count(") != -1:
			ans.append(get_count_without_grpby(res,ind))
		if col_aggr[i].find("average(") != -1:
			ans.append(get_avg_without_grpby(res,ind))
	return [ans]

def get_index_in_res(pure_col_names,order_by_col):
	for i, col in enumerate(pure_col_names):
		if col == order_by_col:
			return i
	return None

def main():
	tables_schema = get_metadata()
	print(tables_schema)
	# print(sys.argv)
	if len(sys.argv) == 1:
		print("")
		return
	query = ' '.join(sys.argv[1:])
	print(query)
	if query[-1] != ';':
		print("Query must end with a semicolon")
		return
	query = query[:-1]
	# print(sqlparse.format(query,keyword_case='upper',identifier_case='upper', strip_comments=True, use_space_around_operators=False))
	Allparsed = sqlparse.parse(sqlparse.format(query,strip_comments=True, use_space_around_operators=False))

	for parsed in Allparsed:
		if parsed.get_type() != "SELECT":
			print("Invalid SQL query")
			return
		q_tables = get_tables_from_query(parsed)
		if q_tables is None:
			print("Enter at least one table")
			return
		# print(q_tables)
		res = []
		for table in q_tables:
			if table not in tables_schema.keys():
				print(table, " doesn't exists!")
				return
		res = cartesian(q_tables,0)
		# print(len(res))
		# printlistoflist(res)

		q_where = get_where_condition(parsed)
		if q_where is not None:
			print(q_where)
			res = remove_rows_as_condition(res,q_where,tables_schema,q_tables)

		q_columns = get_columns_from_query(parsed)
		print(q_columns)

		groupby_col = get_groupby_col(parsed)
		# print(groupby_col)
		if groupby_col is not None:
			if groupby_col[0] not in q_columns:
				print(groupby_col[0],"not in projected columns")
				return
			q_columns.remove(groupby_col[0])
			q_columns.insert(0,groupby_col[0])
			ind = get_index_in_cartesian(q_tables,tables_schema,groupby_col[0].lower())
			# print("ind = ",ind)
			res = after_groupby(res,groupby_col[0].lower(),q_columns[1:],ind,q_tables,tables_schema)
			if res is None:
				return
			q_columns = add_tablename_with_aggr(q_columns,q_tables,tables_schema)
			res.insert(0,q_columns)


		else:
			if not q_columns:
				print("Invalid query")
				return
			if q_columns[0] == '*':
				print("yo")
				res.insert(0,[])
				for table in q_tables:
					for col in tables_schema[table]:
						res[0].append(table+"."+col)
			else:
				# Aggregate functions without group by, to be implemented here.
				is_aggr_present = False
				for col in q_columns:
					if anyaggr_found(col):
						is_aggr_present = True
						break
				if is_aggr_present:
					print(q_columns)
					for col in q_columns:
						if not anyaggr_found(col):
							print("Group by should be present in the query")
							return
					pure_col_names = remove_aggr_from_colnames(q_columns)
					indices_in_res = []
					for col in pure_col_names:
						indices_in_res.append(get_index_in_cartesian(q_tables,tables_schema,col.lower()))
					res = after_aggr_without_groupby(res,q_columns, indices_in_res)
					q_columns = add_tablename_with_aggr(q_columns,q_tables, tables_schema)
					res.insert(0,q_columns)


				else:
					indices_in_res = []
					temp_res = res.copy()
					res = [[]]
					i = 0
					for table in q_tables:
						for col in tables_schema[table]:
							if col in q_columns:
								indices_in_res.append(i);
								res[0].append(table+"."+col)
							i += 1
					# print(indices_in_res)
					for row in temp_res:
						temp_row = []
						for i in range(len(row)):
							if i in indices_in_res:
								temp_row.append(row[i])
						res.append(temp_row)
		# Now res[] contains only the desired columns with attr. names.
		if isDistinctPresent(parsed):
			myset = set()
			for row in res[1:]:
				myset.add(tuple(row))
			col_names = res[0]
			res.clear()
			for row in myset:
				res.append(list(row))
			res.insert(0,col_names)

		order_by_col, ascending = get_orderby_col(parsed)	
		if order_by_col is not None:
			pure_col_names = remove_aggr_from_colnames(res[0])
			for i, col in enumerate(pure_col_names):
				pure_col_names[i] = col.split('.')[1]
			index_in_res = get_index_in_res(pure_col_names,order_by_col)
			if index_in_res is None:
				print("Order By column not found in the tables!")
				return
			col_names = res[0]
			res = sorted(res[1:], key = lambda x: x[index_in_res], reverse = not ascending)
			res.insert(0,col_names)

		printlistoflist(res)

if __name__ == '__main__':
	main()

## Optional

# check if the given col is present in given tables.
# group by col not in projected cols.