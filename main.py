import postgresql

# Create connection to database
db = postgresql.open(
    user = 'postgres',
    password = 'Pielover1339',
    host = '127.0.0.1',
    port = '5432',
    database = 'postgres'
)

# Run sql file to initialize database
initializeFile = open("sdap.sql")
for line in initializeFile:
    db.query(line)

# Getting data types for struct
test = db.query("SELECT column_name, data_type FROM information_schema.COLUMNS WHERE TABLE_NAME = 'sales';")
print(test)

inputType = input("Please enter the name of the file which you would like to read or enter nothing to enter the variables inline: ")
selectAttributes = ""
groupingVarCount = ""
groupingAttributes = ""
fVect = ""
predicates = ""
havingCondition = ""
MF_Struct = {
    'columns': {}
}
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
        elif(content[i] == "SELECT CONDITION-VECT([Ïƒ]):"):
            i += 1
            predicates = content[i].replace(" ", "")
            i += 1
        elif(content[i] == "HAVING_CONDITION(G):"): 
            i += 1     
            havingCondition = content[i]
            i += 1
        else:
            predicates += ", " + content[i]
            i += 1
    predicates = predicates.replace(" ", "")
    print(selectAttributes)
    print(groupingVarCount)
    print(groupingAttributes)
    print(fVect)
    print(predicates)
    print(havingCondition)

else:
    #read inline
    selectAttributes = input("Please input the select attributes seperated by a comma: ").replace(" ", "")
    groupingVarCount = input("Please input the number of grouping variables: ").replace(" ", "")
    groupingAttributes = input("Please input the grouping attribute(s). If more than one, seperate with commas: ").replace(" ", "")
    fVect = input("Please input the list of aggregate functions seperated by a comma: ").replace(" ", "")
    predicates = input("Please input the predicates that define the range of the grouping variables seperated by a comma: ").replace(" ", "")
    havingCondition = input("Please input the having condition: ").replace(" ", "") 
    print(selectAttributes)
    print(groupingVarCount)
    print(groupingAttributes)
    print(fVect)
    print(predicates)
    print(havingCondition)
    
aggregateList = ['sum', 'avg', 'min', 'max', 'count']
for attribute in selectAttributes.split(','):
    if len(attribute.split('_')) > 1:
        if attribute.split('_')[1] in aggregateList:
            MF_Struct['columns'][attribute] = 'int'
    else:
        MF_Struct['columns'][attribute] = '' #insert attribute type from call to stats table

db.close()