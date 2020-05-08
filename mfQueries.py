def mfQuery():
    with open("algorithm.py", 'a') as algorithmFile:
        #splits predicates by each predicate statment and creates list to store the parts of each predicate in a single 2D array
        algorithmFile.write("""predicates = predicates.split(',')\npList = []\n""")
        algorithmFile.write("""for i in predicates:\n\tpList.append(i.split(' '))\n""")
        algorithmFile.write("""for i in range(int(groupingVarCount)+1):\n\t\n""")
        # 0th pass of the algorithm, where each row of the MF Struct is initalized for every unique group based on the grouping variables.
        # Each row in the MF struct also has its columns initalized appropriately based on the aggregates in the F-Vect
        algorithmFile.write("""\tif i == 0:\n\t\tfor row in query:\n\t\t\tkey = ''\n\t\t\tvalue = {}\n""")
        algorithmFile.write("""\t\t\tfor attr in groupingAttributes.split(','):\n\t\t\t\tkey += f'{str(row[attr])},'\n""")
        algorithmFile.write("""\t\t\tkey = key[:-1]\n\t\t\tif key not in MF_Struct.keys():\n""")
        algorithmFile.write("""\t\t\t\tfor groupAttr in groupingAttributes.split(','):\n\t\t\t\t\tcolVal = row[groupAttr]\n""")
        algorithmFile.write("""\t\t\t\t\tif colVal:\n\t\t\t\t\t\tvalue[groupAttr] = colVal\n""")
        algorithmFile.write("""\t\t\t\tfor fVectAttr in fVect.split(','):\n""")
        # Average is saved as an object with the sum, count, and overall average
        algorithmFile.write("""\t\t\t\t\tif (fVectAttr.split('_')[1] == 'avg'):\n\t\t\t\t\t\tvalue[fVectAttr] = {'sum':0, 'count':0, 'avg':0}\n""")
        # Min is initialized as 4994, which is the largest value of 'quant' in the sales table. This allows the first value that the algorithm comes across will be saved as the min (except the row with quant=4994)
        algorithmFile.write("""\t\t\t\t\telif (fVectAttr.split('_')[1] == 'min'):\n\t\t\t\t\t\tvalue[fVectAttr] = 4994 # Max quant in sales\n""")
        algorithmFile.write("""\t\t\t\t\telse:\n\t\t\t\t\t\tvalue[fVectAttr] = 0\n""")
        algorithmFile.write("""\t\t\t\tMF_Struct[key] = value\n""")
        algorithmFile.write("""\telse:\n""")
        # Begin n passes for each of the n grouping variables
        algorithmFile.write("""\t\t\tfor aggregate in fVect.split(','):\n\t\t\t\taggList = aggregate.split('_')\n\t\t\t\tgroupVar = aggList[0]\n\t\t\t\taggFunc = aggList[1]\n\t\t\t\taggCol = aggList[2]\n""")
        # Check to make sure the aggregate function is being called on the grouping variable you are currently on (i)
        algorithmFile.write("""\t\t\t\tif i == int(groupVar):\n\t\t\t\t\tfor row in query:\n\t\t\t\t\t\tkey = ''\n""")
        algorithmFile.write("""\t\t\t\t\t\tfor attr in groupingAttributes.split(','):\n\t\t\t\t\t\t\tkey += f'{str(row[attr])},'\n""")
        algorithmFile.write("""\t\t\t\t\t\tkey = key[:-1]\n\t\t\t\t\t\tif aggFunc == 'sum':\n""")
        # Creates a string to be run with the eval() method by replacing grouping variables with their actual values
        algorithmFile.write("""\t\t\t\t\t\t\tevalString = predicates[i-1]\n\t\t\t\t\t\t\tfor string in pList[i-1]:\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true, update the sum
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tsum = int(row[aggCol])\n\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] += sum\n""")
        algorithmFile.write("""\t\t\t\t\t\telif aggFunc == 'avg':\n""")
        algorithmFile.write("""\t\t\t\t\t\t\tsum = MF_Struct[key][aggregate]['sum']\n\t\t\t\t\t\t\tcount = MF_Struct[key][aggregate]['count']\n\t\t\t\t\t\t\tevalString = predicates[i-1]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\tfor string in pList[i-1]:\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true and the count isn't 0, update the avg
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tsum += int(row[aggCol])\n\t\t\t\t\t\t\t\tcount += 1\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\tif count != 0:\n\t\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] = {'sum': sum, 'count': count, 'avg': (sum/count)}\n""")
        algorithmFile.write("""\t\t\t\t\t\telif aggFunc == 'min':\n\t\t\t\t\t\t\t # check if row meets predicate requirements\n\t\t\t\t\t\t\tevalString = predicates[i-1]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\tfor string in pList[i-1]:\n\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true, update the min
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tmin = int(MF_Struct[key][aggregate])\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\tif int(row[aggCol]) < min:\n\t\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] = row[aggCol]\n""")
        algorithmFile.write("""\t\t\t\t\t\telif aggFunc == 'max':\n\t\t\t\t\t\t\t # check if row meets predicate requirements\n\t\t\t\t\t\t\tevalString = predicates[i-1]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\tfor string in pList[i-1]:\n\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true, update the max
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tmax = int(MF_Struct[key][aggregate])\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\tif int(row[aggCol]) > max:\n\t\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] = row[aggCol]\n""")
        algorithmFile.write("""\t\t\t\t\t\telif aggFunc == 'count':\n\t\t\t\t\t\t\t # check if row meets predicate requirements\n\t\t\t\t\t\t\tevalString = predicates[i-1]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\tfor string in pList[i-1]:\n\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true, increment the count
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] += 1\n""")


        #Generate output table(also checks the HAVING condition)
        algorithmFile.write("""output = PrettyTable()\noutput.field_names = selectAttributes.split(',')\nfor row in MF_Struct:\n""")
        #create an evalString to be used to check each having condition
        algorithmFile.write("""\tevalString = ''\n\tif havingCondition != '':\n""")
        #if there is a having condition, loop through each element of the having condition to fill in the correct information into the evalString
        #the eval string will be equal to the having condition, replaced with the values of the variables in question, 
        #then evaluated to check if the row of the MFStruct being examined is to be included in the output table
        algorithmFile.write("""\t\tfor string in havingCondition.split(' '):\n\t\t\tif string not in ['>', '<', '==', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']:\n""")
        algorithmFile.write("""\t\t\t\ttry: #catches all ints in having clause and adds them to eval string\n\t\t\t\t\tfloat(string)\n\t\t\t\t\tevalString += string\n""")
        algorithmFile.write("""\t\t\t\texcept: #if it is not an int, it is a variable in the MF_Struct\n""")
        algorithmFile.write("""\t\t\t\t\tif len(string.split('_')) > 1 and string.split('_')[1] == 'avg':\n\t\t\t\t\t\tevalString += str(MF_Struct[row][string]['avg'])\n""")
        algorithmFile.write("""\t\t\t\t\telse:\n\t\t\t\t\t\tevalString += str(MF_Struct[row][string])\n""")
        algorithmFile.write("""\t\t\telse:\n\t\t\t\tevalString += f' {string} '\n""")
        algorithmFile.write("""\t\tif eval(evalString.replace('=', '==')):\n\t\t\trow_info = []\n\t\t\tfor val in selectAttributes.split(','):\n""")
        algorithmFile.write("""\t\t\t\tif len(val.split('_')) > 1 and val.split('_')[1] == 'avg':\n\t\t\t\t\trow_info += [str(MF_Struct[row][val]['avg'])]\n""")
        algorithmFile.write("""\t\t\t\telse:\n\t\t\t\t\trow_info += [str(MF_Struct[row][val])]\n""")
        algorithmFile.write("""\t\t\toutput.add_row(row_info)\n\t\tevalString = '' #clear eval string after execution\n""")

        #there is no having condition, thus every MFStruct row will added to the output table
        algorithmFile.write("""\telse:\n\t\trow_info = []\n\t\tfor val in selectAttributes.split(','):\n""")
        algorithmFile.write("""\t\t\tif len(val.split('_')) > 1 and val.split('_')[1] == 'avg':\n\t\t\t\trow_info += [str(MF_Struct[row][val]['avg'])]\n""")
        algorithmFile.write("""\t\t\telse:\n\t\t\t\trow_info += [str(MF_Struct[row][val])]\n""")
        algorithmFile.write("""\t\toutput.add_row(row_info)\n""")    
        algorithmFile.write("""print(output) #Pretty table corresponding to evaluation of query\n""")
        algorithmFile.close()