import postgresql
from databaseConfig import dbConfig

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
    havingCondition = havingCondition.replace(" ", "")
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
    havingCondition = input("Please input the having condition: ").replace(" ", "") 
    print("selectAttributes:",selectAttributes)
    print("groupingVarCount:",groupingVarCount)
    print("groupingAttributes:",groupingAttributes)
    print("fVect:",fVect)
    print("predicates:",predicates)
    print("havingCondition:",havingCondition)

MF_Struct = {'columns': {}}
for attribute in (groupingAttributes + ',' + fVect).split(','):
    if len(attribute.split('_')) > 1:
        if attribute.split('_')[1] in ['sum', 'avg', 'min', 'max', 'count']:
            MF_Struct['columns'][attribute] = 'int'
        if attribute.split('_')[0] in ['sum', 'avg', 'min', 'max', 'count']:
            MF_Struct['columns'][attribute] = 'int'
    else:
        if attribute == 'cust' or attribute == 'prod' or attribute == 'state':
            MF_Struct['columns'][attribute] = 'str'
        if attribute == 'day' or attribute == 'month' or attribute == 'year' or attribute == 'quant':
            MF_Struct['columns'][attribute] = 'int'

db = postgresql.open(user = dbConfig['user'],password = dbConfig['password'],host = dbConfig['host'],port = dbConfig['port'],database = dbConfig['database'],)
query = db.prepare("SELECT * FROM sales;")

predicates = predicates.split(',')
pList = []
# We are using the grouping variables as indexes, so only numbers work
for i in predicates:
    pList.append(i.split('.'))

for i in range(int(groupingVarCount)+1):
    # Initial pass - Initialization
    if i == 0:
        for row in query:
            # print(row)
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
                    # if MF_Struct['columns'][groupAttr] == 'int':
                    #     value[groupAttr] = 0
                    # if MF_Struct['columns'][groupAttr] == 'str':
                    #     value[groupAttr] = ''
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
        print(MF_Struct)
    else:
        #other passes where i = grouping var number
        # Next N passes
        for aggregate in fVect.split(','):
            aggList = aggregate.split('_')
            groupVar = aggList[0]
            aggFunc = aggList[1]
            aggCol = aggList[2]
            predList = [] #all predicates that apply to current groupVar
            # ['sum', 'avg', 'min', 'max', 'count']
            for row in query:
                key = ''
                for attr in groupingAttributes.split(','):
                    key += f'{str(row[attr])},'
                key = key[:-1]
                if aggFunc == 'sum':
                    #check if row meets predicate requirements
                    if pList[i-1][0] == str(i): # a string that corresponds to the grouping variable,
                        sum = 0
                        pred = pList[i-1][1]
                        if(len(pred.split('=')) != 1): # =
                            pred = pred.split('=')
                            if str(row[pred[0]]) == pred[1].replace("'", ''):
                                sum += int(row[aggCol])
                        elif(len(pred.split('>')) != 1): # >
                            pred = pred.split('>')
                            if int(row[pred[0]]) > int(pred[1].replace("'", '')):
                                sum += int(row[aggCol])
                        elif(len(pred.split('<')) != 1): # <
                            pred = pred.split('<')
                            if int(row[pred[0]]) < int(pred[1].replace("'", '')):
                                sum += int(row[aggCol])
                        elif(len(pred.split('<=')) != 1): # <=
                            pred = pred.split('<=')
                            if int(row[pred[0]]) <= int(pred[1].replace("'", '')):
                                sum += int(row[aggCol])
                        elif(len(pred.split('>=')) != 1): # >=
                            pred = pred.split('>=')
                            if int(row[pred[0]]) >= int(pred[1].replace("'", '')):
                                sum += int(row[aggCol])
                        MF_Struct[key][aggregate] += sum
                elif aggFunc == 'avg':
                    # avg = {sum: 0, count: 0, avg: 0}
                    #check if row meets predicate requirements
                    if pList[i-1][0] == str(i): # a string that corresponds to the grouping variable,
                        sum = MF_Struct[key][aggregate]['sum']
                        count = MF_Struct[key][aggregate]['count']
                        pred = pList[i-1][1]
                        if(len(pred.split('=')) != 1): # =
                            pred = pred.split('=')
                            if str(row[pred[0]]) == pred[1].replace("'", ''):
                                sum += int(row[aggCol])
                                count += 1
                        elif(len(pred.split('>')) != 1): # >
                            pred = pred.split('>')
                            if int(row[pred[0]]) > int(pred[1].replace("'", '')):
                                sum += int(row[aggCol])
                                count += 1
                        elif(len(pred.split('<')) != 1): # <
                            pred = pred.split('<')
                            if int(row[pred[0]]) < int(pred[1].replace("'", '')):
                                sum += int(row[aggCol])
                                count += 1
                        elif(len(pred.split('<=')) != 1): # <=
                            pred = pred.split('<=')
                            if int(row[pred[0]]) <= int(pred[1].replace("'", '')):
                                sum += int(row[aggCol])
                                count += 1
                        elif(len(pred.split('>=')) != 1): # >=
                            pred = pred.split('>=')
                            if int(row[pred[0]]) >= int(pred[1].replace("'", '')):
                                sum += int(row[aggCol])
                                count += 1
                        if(count != 0):
                            MF_Struct[key][aggregate] = {'sum': sum, 'count': count, 'avg': (sum/count)}
                elif aggFunc == 'min':
                    #check if row meets predicate requirements AND is less than min
                    if pList[i-1][0] == str(i): # a string that corresponds to the grouping variable,
                        pred = pList[i-1][1]
                        min = MF_Struct[key][aggregate]
                        if(len(pred.split('=')) != 1): # =
                            pred = pred.split('=')
                            if str(row[pred[0]]) == pred[1].replace("'", '') and (int(row[aggCol]) < min):
                                min = int(row[aggCol])
                        elif(len(pred.split('>')) != 1): # >
                            pred = pred.split('>')
                            if int(row[pred[0]]) > int(pred[1].replace("'", '')) and (int(row[aggCol]) < min):
                                min = int(row[aggCol])
                        elif(len(pred.split('<')) != 1): # <
                            pred = pred.split('<')
                            if int(row[pred[0]]) < int(pred[1].replace("'", '')) and (int(row[aggCol]) < min):
                                min = int(row[aggCol])
                        elif(len(pred.split('<=')) != 1): # <=
                            pred = pred.split('<=')
                            if int(row[pred[0]]) <= int(pred[1].replace("'", '')) and (int(row[aggCol]) < min):
                                min = int(row[aggCol])
                        elif(len(pred.split('>=')) != 1): # >=
                            pred = pred.split('>=')
                            if int(row[pred[0]]) >= int(pred[1].replace("'", '')) and (int(row[aggCol]) < min):
                                min = int(row[aggCol])
                        MF_Struct[key][aggregate] = min
                elif aggFunc == 'max':
                    #check if row meets predicate requirements AND is greater than max
                    if pList[i-1][0] == str(i): # a string that corresponds to the grouping variable,
                        pred = pList[i-1][1]
                        max = MF_Struct[key][aggregate]
                        if(len(pred.split('=')) != 1): # =
                            pred = pred.split('=')
                            if str(row[pred[0]]) == pred[1].replace("'", '') and (int(row[aggCol]) > max):
                                max = int(row[aggCol])
                        elif(len(pred.split('>')) != 1): # >
                            pred = pred.split('>')
                            if int(row[pred[0]]) > int(pred[1].replace("'", '')) and (int(row[aggCol]) > max):
                                max = int(row[aggCol])
                        elif(len(pred.split('<')) != 1): # <
                            pred = pred.split('<')
                            if int(row[pred[0]]) < int(pred[1].replace("'", '')) and (int(row[aggCol]) > max):
                                max = int(row[aggCol])
                        elif(len(pred.split('<=')) != 1): # <=
                            pred = pred.split('<=')
                            if int(row[pred[0]]) <= int(pred[1].replace("'", '')) and (int(row[aggCol]) > max):
                                max = int(row[aggCol])
                        elif(len(pred.split('>=')) != 1): # >=
                            pred = pred.split('>=')
                            if int(row[pred[0]]) >= int(pred[1].replace("'", '')) and (int(row[aggCol]) > max):
                                max = int(row[aggCol])
                        MF_Struct[key][aggregate] = max
                elif aggFunc == 'count':
                    #check if row meets predicate requirements
                    if pList[i-1][0] == str(i): # a string that corresponds to the grouping variable,
                        pred = pList[i-1][1]
                        count = MF_Struct[key][aggregate]
                        if(len(pred.split('=')) != 1): # =
                            pred = pred.split('=')
                            if str(row[pred[0]]) == pred[1].replace("'", ''):
                                count += 1
                        elif(len(pred.split('>')) != 1): # >
                            pred = pred.split('>')
                            if int(row[pred[0]]) > int(pred[1].replace("'", '')):
                                count += 1
                        elif(len(pred.split('<')) != 1): # <
                            pred = pred.split('<')
                            if int(row[pred[0]]) < int(pred[1].replace("'", '')):
                                count += 1
                        elif(len(pred.split('<=')) != 1): # <=
                            pred = pred.split('<=')
                            if int(row[pred[0]]) <= int(pred[1].replace("'", '')):
                                count += 1
                        elif(len(pred.split('>=')) != 1): # >=
                            pred = pred.split('>=')
                            if int(row[pred[0]]) >= int(pred[1].replace("'", '')):
                                count += 1
                        MF_Struct[key][aggregate] = count
                    
print(MF_Struct)

#Does not do the following:
#   take into account EMF query x.state = state etc.
#   Work on simple SQL queries, no grouping attributes(grouping attribute count = 0)
#   Print results to table(taking into account the having statment)
#TODO
#   Presentation
#   Write 5 test queries to run for presentation
#   Make this all write to a file