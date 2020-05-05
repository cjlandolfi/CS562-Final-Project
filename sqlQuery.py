import postgresql
from databaseConfig import dbConfig
from prettytable import PrettyTable

db = postgresql.open(
    user = dbConfig["user"],
    password = dbConfig["password"],
    host = dbConfig["host"],
    port = dbConfig["port"],
    database = dbConfig["database"],
)

# Run sql file to initialize database
initializeFile = open("sdap.sql")
for line in initializeFile:
    db.query(line)

# Receive Input
inputType = input("Please enter the name of the file which you would like to read or enter nothing to enter the variables inline: ")
selectAttributes = ""
groupingVarCount = ""
groupingAttributes = ""
fVect = ""
predicates = ""
havingCondition = ""

if inputType != "":
    with open(inputType) as f:
        content = f.readlines()
    content = [x.rstrip() for x in content]
    i = 0
    while i < len(content):
        if(content[i] == "SELECT ATTRIBUTE(S):"):
            i += 1
            selectAttributes = content[i].replace(" ", "")
            i += 1
        elif(content[i] == "NUMBER OF GROUPING VARIABLES(n):"):
            i += 1
            groupingVarCount = content[i].replace(" ", "")
            i += 1
        elif(content[i] == "GROUPING ATTRIBUTES(V):"):
            i += 1
            groupingAttributes = content[i].replace(" ", "")
            i += 1
        elif(content[i] == "F-VECT([F]):"):
            i += 1
            fVect = content[i].replace(" ", "")
            i += 1
        elif(content[i] == "SELECT CONDITION-VECT([σ]):"):
            i += 1
            predicates = content[i].replace(" ", "")
            i += 1
        elif(content[i] == "HAVING_CONDITION(G):"): 
            i += 1     
            havingCondition = content[i]
            i += 1
        else:
            predicates += "," + content[i]
            i += 1
    selectAttributes = selectAttributes.replace(" ", "")
    groupingVarCount = groupingVarCount.replace(" ", "")
    groupingAttributes = groupingAttributes.replace(" ", "")
    fVect = fVect.replace(" ", "")
    predicates = predicates.replace(" ", "") 
    havingCondition = havingCondition
    print("selectAttributes:",selectAttributes)
    print("groupingVarCount:",groupingVarCount)
    print("groupingAttributes:",groupingAttributes)
    print("fVect:",fVect)
    print("predicates:",predicates)
    print("havingCondition:",havingCondition)
else:
    #read inline
    selectAttributes = input("Please input the select attributes seperated by a comma: ").replace(" ", "")
    groupingVarCount = input("Please input the number of grouping variables: ").replace(" ", "")
    groupingAttributes = input("Please input the grouping attribute(s). If more than one, seperate with commas: ").replace(" ", "")
    fVect = input("Please input the list of aggregate functions seperated by a comma: ").replace(" ", "")
    predicates = input("Please input the predicates that define the range of the grouping variables seperated by a comma: ").replace(" ", "")
    havingCondition = input("Please input the having condition: ")
    print("selectAttributes:",selectAttributes)
    print("groupingVarCount:",groupingVarCount)
    print("groupingAttributes:",groupingAttributes)
    print("fVect:",fVect)
    print("predicates:",predicates)
    print("havingCondition:",havingCondition)

MF_Struct = {}

db = postgresql.open(user = dbConfig['user'],password = dbConfig['password'],host = dbConfig['host'],port = dbConfig['port'],database = dbConfig['database'],)
query = db.prepare("SELECT * FROM sales;")
# Initial pass - Initialization
for row in query:
    key = ''
    value = {}
    # i = 0
    # while i < groupingVarCount:
    for attr in groupingAttributes.split(','):
        key += f'{str(row[attr])},'
    key = key[:-1]
    if key not in MF_Struct.keys():
        for groupAttr in groupingAttributes.split(','):
            colVal = row[groupAttr]
            if colVal:
                value[groupAttr] = colVal
        # Can be string?
        for fVectAttr in fVect.split(','):
            tableCol = fVectAttr.split('_')[1]
            if (fVectAttr.split('_')[0] == 'avg'):
                value[fVectAttr] = {'sum': row[tableCol], 'count': 1, 'avg': row[tableCol]}
            elif (fVectAttr.split('_')[0] == 'count'):
                value[fVectAttr] = 1
            else: #min or max
                value[fVectAttr] = row[tableCol]
        MF_Struct[key] = value
        # End initial pass
    else: # Existing group, evaluate aggregate
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
            else: #max
                if row[tableCol] > MF_Struct[key][fVectAttr]:
                    MF_Struct[key][fVectAttr] = int(row[tableCol])
print(MF_Struct) # Initialized Struct
output = PrettyTable()
output.field_names = selectAttributes.split(',')
for row in MF_Struct:
    evalString = ''
    if havingCondition != '':
        for string in havingCondition.split(' '): 
            if string not in ['>', '<', '==', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']: 
                try: #catches all ints in having clause and adds them to eval string
                    int(string)
                    evalString += string
                except: #if it is not an int, it is a variable in the MF_Struct
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
        evalString = '' #clear eval string after execution
    else:
        row_info = []
        for val in selectAttributes.split(','):
            if len(val.split('_')) > 1 and val.split('_')[0] == 'avg':
                row_info += [str(MF_Struct[row][val]['avg'])] 
            else:
                row_info += [str(MF_Struct[row][val])]
        output.add_row(row_info)      
print(output) #Pretty table corresponding to evaluation of query