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
            predicates = content[i]
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
    predicates = predicates
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
    predicates = input("Please input the predicates that define the range of the grouping variables seperated by a comma: ")
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
predicates = predicates.split(',')
print('before',predicates)
pList = []
# We are using the grouping variables as indexes, so only numbers work
for i in predicates:
    pList.append(i.split(' '))
print('pList', pList)
for i in range(int(groupingVarCount)+1):
    # Initial pass - Initialization
    if i == 0:
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
                # Can be string?
                for fVectAttr in fVect.split(','):
                    if (fVectAttr.split('_')[1] == 'avg'):
                        value[fVectAttr] = {'sum':0, 'count':0, 'avg':0}
                    elif (fVectAttr.split('_')[1] == 'min'):
                        value[fVectAttr] = 4994
                    else:
                        value[fVectAttr] = 0
                MF_Struct[key] = value
                # End initial pass
        print(MF_Struct) # Initialized Struct
    else:
        #other passes where i = grouping var number
        # Next N passes
        for aggregate in fVect.split(','):
            aggList = aggregate.split('_')
            groupVar = aggList[0]
            aggFunc = aggList[1]
            aggCol = aggList[2]
            # ['sum', 'avg', 'min', 'max', 'count']
            if i == int(groupVar):
                for row in query:
                    key = ''
                    for attr in groupingAttributes.split(','):
                        key += f'{str(row[attr])},'
                    key = key[:-1]
                    if aggFunc == 'sum':
                        #check if row meets predicate requirements
                        evalString = predicates[i-1]
                        for string in pList[i-1]:   
                            if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
                                rowVal = row[string.split('.')[1]]
                                try:
                                    int(rowVal)
                                    evalString = evalString.replace(string, str(rowVal))
                                except:
                                    evalString = evalString.replace(string, f"'{rowVal}'")
                        if eval(evalString.replace('=', '==')):
                            sum = int(row[aggCol])
                            MF_Struct[key][aggregate] += sum
                    elif aggFunc == 'avg':
                        # avg = {sum: 0, count: 0, avg: 0}
                        #check if row meets predicate requirements
                        sum = MF_Struct[key][aggregate]['sum']
                        count = MF_Struct[key][aggregate]['count']
                        evalString = predicates[i-1]
                        for string in pList[i-1]:   
                            if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
                                rowVal = row[string.split('.')[1]]
                                try:
                                    int(rowVal)
                                    evalString = evalString.replace(string, str(rowVal))
                                except:
                                    evalString = evalString.replace(string, f"'{rowVal}'")
                        if eval(evalString.replace('=', '==')):
                            sum += int(row[aggCol])
                            count += 1
                            if count != 0:
                                MF_Struct[key][aggregate] = {'sum': sum, 'count': count, 'avg': (sum/count)}
                    elif aggFunc == 'min':
                        #check if row meets predicate requirements
                        evalString = predicates[i-1]
                        for string in pList[i-1]:   
                            if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
                                rowVal = row[string.split('.')[1]]
                                try:
                                    int(rowVal)
                                    evalString = evalString.replace(string, str(rowVal))
                                except:
                                    evalString = evalString.replace(string, f"'{rowVal}'")
                        if eval(evalString.replace('=', '==')):
                            min = int(MF_Struct[key][aggregate])
                            if int(row[aggCol]) < min:
                                MF_Struct[key][aggregate] = row[aggCol]
                    elif aggFunc == 'max':
                        #check if row meets predicate requirements
                        evalString = predicates[i-1]
                        for string in pList[i-1]:   
                            if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
                                rowVal = row[string.split('.')[1]]
                                try:
                                    int(rowVal)
                                    evalString = evalString.replace(string, str(rowVal))
                                except:
                                    evalString = evalString.replace(string, f"'{rowVal}'")
                        if eval(evalString.replace('=', '==')):
                            max = int(MF_Struct[key][aggregate])
                            if int(row[aggCol]) > max:
                                MF_Struct[key][aggregate] = row[aggCol]
                    elif aggFunc == 'count':
                        #check if row meets predicate requirements
                        evalString = predicates[i-1]
                        for string in pList[i-1]:   
                            if len(string.split('.')) > 1 and string.split('.')[0] == str(i):
                                rowVal = row[string.split('.')[1]]
                                try:
                                    int(rowVal)
                                    evalString = evalString.replace(string, str(rowVal))
                                except:
                                    evalString = evalString.replace(string, f"'{rowVal}'")
                        if eval(evalString.replace('=', '==')):
                            MF_Struct[key][aggregate] += 1                  
print(MF_Struct) # Evaluated struct
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
                    if len(string.split('_')) > 1 and string.split('_')[1] == 'avg':
                        evalString += str(MF_Struct[row][string]['avg'])
                    else:
                        evalString += str(MF_Struct[row][string])
            else:
                evalString += f' {string} '
        if eval(evalString.replace('=', '==')):
            row_info = []
            for val in selectAttributes.split(','):
                if len(val.split('_')) > 1 and val.split('_')[1] == 'avg':
                    row_info += [str(MF_Struct[row][val]['avg'])]
                else:
                    row_info += [str(MF_Struct[row][val])]
            output.add_row(row_info)
        evalString = '' #clear eval string after execution
    else:
        row_info = []
        for val in selectAttributes.split(','):
            if len(val.split('_')) > 1 and val.split('_')[1] == 'avg':
                row_info += [str(MF_Struct[row][val]['avg'])]
            else:
                row_info += [str(MF_Struct[row][val])]
        output.add_row(row_info)
print(output) #Pretty table corresponding to evaluation of query