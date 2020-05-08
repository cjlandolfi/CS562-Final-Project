import postgresql
from databaseConfig import dbConfig
from prettytable import PrettyTable

selectAttributes = "cust,prod,avg_quant,max_quant"
groupingVarCount = 0
groupingAttributes = "cust,prod"
fVect = "avg_quant,max_quant,min_quant,count_quant"
predicates = ""
havingCondition = ""
MF_Struct = {}
db = postgresql.open(user = dbConfig['user'],password = dbConfig['password'],host = dbConfig['host'],port = dbConfig['port'],database = dbConfig['database'],)

query = db.prepare('SELECT * FROM sales;')


# Algorithm for basic SQL Query:
for row in query:
	key = ''
	value = {}
	for attr in groupingAttributes.split(','):
		key += f'{str(row[attr])},'
	key = key[:-1]
	if key not in MF_Struct.keys():
		for groupAttr in groupingAttributes.split(','):
			colVal = row[groupAttr]
			if colVal:
				value[groupAttr] = colVal
		for fVectAttr in fVect.split(','):
			tableCol = fVectAttr.split('_')[1]
			if (fVectAttr.split('_')[0] == 'avg'):
				value[fVectAttr] = {'sum': row[tableCol], 'count': 1, 'avg': row[tableCol]}
			elif (fVectAttr.split('_')[0] == 'count'):
				value[fVectAttr] = 1
			else:
				value[fVectAttr] = row[tableCol]
		MF_Struct[key] = value
	else:
		for fVectAttr in fVect.split(','):
			tableCol = fVectAttr.split('_')[1]
			if (fVectAttr.split('_')[0] == 'sum'):
				MF_Struct[key][fVectAttr] += int(row[tableCol])
			elif (fVectAttr.split('_')[0] == 'avg'):
				newSum = MF_Struct[key][fVectAttr]['sum'] + int(row[tableCol])
				newCount = MF_Struct[key][fVectAttr]['count'] + 1
				MF_Struct[key][fVectAttr] = {'sum': newSum, 'count': newCount, 'avg': newSum / newCount}
			elif (fVectAttr.split('_')[0] == 'count'):
				MF_Struct[key][fVectAttr] += 1
			elif (fVectAttr.split('_')[0] == 'min'):
				if row[tableCol] < MF_Struct[key][fVectAttr]:
					MF_Struct[key][fVectAttr] = int(row[tableCol])
			else:
				if row[tableCol] > MF_Struct[key][fVectAttr]:
					MF_Struct[key][fVectAttr] = int(row[tableCol])
#Generate output table(also checks the HAVING condition)
output = PrettyTable()
output.field_names = selectAttributes.split(',')
for row in MF_Struct:
	evalString = ''
	if havingCondition != '':
        #if there is a having condition, loop through each element of the having condition to fill in the correct information into the evalString
        #the eval string will be equal to the having condition, replaced with the values of the variables in question,
        # then evaluated to check if the row of the MFStruct being examined is to be included in the output table
		for string in havingCondition.split(' '):
			if string not in ['>', '<', '==', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']:
				try:
					int(string)
					evalString += string
				except:
					if len(string.split('_')) > 1 and string.split('_')[0] == 'avg':
						evalString += str(MF_Struct[row][string]['avg'])
					else:
						evalString += str(MF_Struct[row][string])
			else:
				evalString += f' {string} '
		if eval(evalString.replace('=', '==')):
			row_info = []
			for val in selectAttributes.split(','):
				if len(val.split('_')) > 1 and val.split('_')[0] == 'avg':
					row_info += [str(MF_Struct[row][val]['avg'])]
				else:
					row_info += [str(MF_Struct[row][val])]
			output.add_row(row_info)
		evalString = ''
	else:
        #there is no having condition, thus every MFStruct row will be in the output table
		row_info = []
		for val in selectAttributes.split(','):
			if len(val.split('_')) > 1 and val.split('_')[0] == 'avg':
				row_info += [str(MF_Struct[row][val]['avg'])]
			else:
				row_info += [str(MF_Struct[row][val])]
		output.add_row(row_info)
print(output)