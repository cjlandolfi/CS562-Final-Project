import postgresql
from databaseConfig import dbConfig

# Create connection to database
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

algorithmFile = open("algorithm.py", "w")
algorithmFile.write("import postgresql\nfrom databaseConfig import dbConfig\n")
algorithmFile.write("MF_Struct = {'columns': {}}\n")
algorithmFile.write(f"for attribute in '{(groupingAttributes + fVect)}'.split(','):\n")
algorithmFile.write("   if len(attribute.split('_')) > 1:\n")
algorithmFile.write("       if attribute.split('_')[1] in ['sum', 'avg', 'min', 'max', 'count']:\n")
algorithmFile.write("           MF_Struct['columns'][attribute] = 'int'\n")
algorithmFile.write("   if attribute.split('_')[0] in ['sum', 'avg', 'min', 'max', 'count']:\n")
algorithmFile.write("       MF_Struct['columns'][attribute] = 'int'\n")
algorithmFile.write("   else:\n")
algorithmFile.write("       if attribute == 'cust' or attribute == 'prod' or attribute == 'state':\n")
algorithmFile.write("           MF_Struct['columns'][attribute] = 'str'\n")
algorithmFile.write("       if attribute == 'day' or attribute == 'month' or attribute == 'year' or attribute == 'quant':\n")
algorithmFile.write("           MF_Struct['columns'][attribute] = 'int'\n")
algorithmFile.write("print(MF_Struct)\n")
algorithmFile.write("db = postgresql.open(user = dbConfig['user'],password = dbConfig['password'],host = dbConfig['host'],port = dbConfig['port'],database = dbConfig['database'],)\n")
algorithmFile.write("""print(db.query("SELECT column_name, data_type FROM information_schema.COLUMNS WHERE TABLE_NAME = 'sales';"))\n""")
algorithmFile.write("db.close()")

# Row iterator
# query = db.prepare("SELECT * FROM sales;")
# for row in query:
#     print(row['cust'], row)

# algorithmFile.write(f"MF_Struct: {MF_Struct}")
# db.query("SELECT column_name, data_type FROM information_schema.COLUMNS WHERE TABLE_NAME = 'sales';")
db.close()