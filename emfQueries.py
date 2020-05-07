def emfQuery():
    with open("algorithm.py", 'a') as algorithmFile:
        algorithmFile.write("""predicates = predicates.split(',')\npList = []\nfor i in predicates:\n\tpList.append(i.split(' '))\n""") #splits predicates by each predicate statment and creates list to store the parts of each predicate in a single 2D array
        algorithmFile.write("""for i in range(int(groupingVarCount)+1):\n""")
        # Initialization occurs in the 0th pass of the algorithm
        algorithmFile.write("""\tif i == 0:\n\t\tfor row in query:\n\t\t\tkey = ''\n\t\t\tvalue = {}\n""")
        algorithmFile.write("""\t\t\tfor attr in groupingAttributes.split(','):\n\t\t\t\tkey += f'{str(row[attr])},'\n""")
        algorithmFile.write("""\t\t\tkey = key[:-1]\n\t\t\tif key not in MF_Struct.keys():\n""")
        algorithmFile.write("""\t\t\t\tfor groupAttr in groupingAttributes.split(','):\n\t\t\t\t\tcolVal = row[groupAttr]\n""")
        algorithmFile.write("""\t\t\t\t\tif colVal:\n\t\t\t\t\t\tvalue[groupAttr] = colVal\n""")
        algorithmFile.write("""\t\t\t\tfor fVectAttr in fVect.split(','):\n""")
        # Average is saved as an object with the sum, count, and overall average
        algorithmFile.write("""\t\t\t\t\tif (fVectAttr.split('_')[1] == 'avg'):\n\t\t\t\t\t\tvalue[fVectAttr] = {'sum':0, 'count':0, 'avg':0}\n""")
        # Min is initialized as 4994, which is the largest value of 'quant' in the sales table. This allows the first value that the algorithm comes across will be saved as the min (except the row with quant=4994)
        algorithmFile.write("""\t\t\t\t\telif (fVectAttr.split('_')[1] == 'min'):\n\t\t\t\t\t\tvalue[fVectAttr] = 4994\n""")
        algorithmFile.write("""\t\t\t\t\telse:\n\t\t\t\t\t\tvalue[fVectAttr] = 0\n""")
        algorithmFile.write("""\t\t\t\tMF_Struct[key] = value\n""")
        # Begin n passes for each of the n grouping variables
        algorithmFile.write("""\telse:\n\t\tfor aggregate in fVect.split(','):\n\t\t\taggList = aggregate.split('_')\n\t\t\tgroupVar = aggList[0]\n\t\t\taggFunc = aggList[1]\n\t\t\taggCol = aggList[2]\n""")
        # Check to make sure the aggregate function is being called on the grouping variable you are currently on (i)
        # Also loop through every key in the MF_Struct to see if statements like '1.state = state' applies to it
        algorithmFile.write("""\t\t\tif i == int(groupVar):\n\t\t\t\tfor row in query:\n\t\t\t\t\tfor key in MF_Struct.keys():\n""")
        algorithmFile.write("""\t\t\t\t\t\tif aggFunc == 'sum':\n\t\t\t\t\t\t\tevalString = predicates[i-1]\n""")
        # Creates a string to be run with the eval() method by replacing grouping variables with their actual values
        # Since it's an EMF query, it must also check if the string is a grouping variable and replace that with the actual value as well
        algorithmFile.write("""\t\t\t\t\t\t\tfor string in pList[i-1]:\n\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\telif string in groupingAttributes.split(','):\n\t\t\t\t\t\t\t\t\trowVal = MF_Struct[key][string]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true, update the sum
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tsum = int(row[aggCol])\n\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] += sum\n""")
        algorithmFile.write("""\t\t\t\t\t\telif aggFunc == 'avg':\n\t\t\t\t\t\t\tsum = MF_Struct[key][aggregate]['sum']\n\t\t\t\t\t\t\tcount = MF_Struct[key][aggregate]['count']\n\t\t\t\t\t\t\tevalString = predicates[i-1]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\tfor string in pList[i-1]:\n\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\telif string in groupingAttributes.split(','):\n\t\t\t\t\t\t\t\t\trowVal = MF_Struct[key][string]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true, update the avg
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tsum += int(row[aggCol])\n\t\t\t\t\t\t\t\tcount += 1\n\t\t\t\t\t\t\t\tif count != 0:\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] = {'sum': sum, 'count': count, 'avg': (sum/count)}\n""")
        algorithmFile.write("""\t\t\t\t\t\telif aggFunc == 'min':\n\t\t\t\t\t\t\tevalString = predicates[i-1]\n\t\t\t\t\t\t\tfor string in pList[i-1]:\n\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n""")                
        algorithmFile.write("""\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\telif string in groupingAttributes.split(','):\n\t\t\t\t\t\t\t\t\trowVal = MF_Struct[key][string]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true, update the min
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tmin = int(MF_Struct[key][aggregate])\n\t\t\t\t\t\t\t\tif int(row[aggCol]) < min:\n\t\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] = row[aggCol]\n""")
        algorithmFile.write("""\t\t\t\t\t\telif aggFunc == 'max':\n\t\t\t\t\t\t\tevalString = predicates[i-1]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\tfor string in pList[i-1]:\n\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tvalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\telif string in groupingAttributes.split(','):\n\t\t\t\t\t\t\t\t\trowVal = MF_Struct[key][string]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tvalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true, update the max
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tmax = int(MF_Struct[key][aggregate])\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\tif int(row[aggCol]) > max:\n\t\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] = row[aggCol]\n""")
        algorithmFile.write("""\t\t\t\t\t\telif aggFunc == 'count':\n\t\t\t\t\t\t\tevalString = predicates[i-1]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\tfor string in pList[i-1]:\n\t\t\t\t\t\t\t\tif len(string.split('.')) > 1 and string.split('.')[0] == str(i):\n\t\t\t\t\t\t\t\t\trowVal = row[string.split('.')[1]]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\telif string in groupingAttributes.split(','):\n\t\t\t\t\t\t\t\t\trowVal = MF_Struct[key][string]\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\ttry:\n\t\t\t\t\t\t\t\t\t\tint(rowVal)\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, str(rowVal))\n""")
        algorithmFile.write("""\t\t\t\t\t\t\t\t\texcept:\n\t\t\t\t\t\t\t\t\t\tevalString = evalString.replace(string, f"'{rowVal}'")\n""")
        # If evalString is true, increment the count
        algorithmFile.write("""\t\t\t\t\t\t\tif eval(evalString.replace('=', '==')):\n\t\t\t\t\t\t\t\tMF_Struct[key][aggregate] += 1\n""")
        #output/having clause
        algorithmFile.write("""output = PrettyTable()\noutput.field_names = selectAttributes.split(',')\nfor row in MF_Struct:\n""")
        algorithmFile.write("""\tevalString = ''\n\tif havingCondition != '':\n""") #create an evalString to be used to check each having condition
        
        #if there is a having condition, loop through each element of the having condition to fill in the correct information into the evalString
        #the eval string will be equal to the having condition, replaced with the values of the variables in question, 
        #then evaluated to check if the row of the MFStruct being examined is to be included in the output table
        algorithmFile.write("""\t\tfor string in havingCondition.split(' '):\n\t\t\tif string not in ['>', '<', '==', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']:\n""")
        algorithmFile.write("""\t\t\t\ttry:\n\t\t\t\t\tfloat(string)\n\t\t\t\t\tevalString += string\n""")
        algorithmFile.write("""\t\t\t\texcept:\n""")
        algorithmFile.write("""\t\t\t\t\tif len(string.split('_')) > 1 and string.split('_')[1] == 'avg':\n\t\t\t\t\t\tevalString += str(MF_Struct[row][string]['avg'])\n""")
        algorithmFile.write("""\t\t\t\t\telse:\n\t\t\t\t\t\tevalString += str(MF_Struct[row][string])\n""")
        algorithmFile.write("""\t\t\telse:\n\t\t\t\tevalString += f' {string} '\n""")
        algorithmFile.write("""\t\tif eval(evalString.replace('=', '==')):\n\t\t\trow_info = []\n\t\t\tfor val in selectAttributes.split(','):\n""")
        algorithmFile.write("""\t\t\t\tif len(val.split('_')) > 1 and val.split('_')[1] == 'avg':\n\t\t\t\t\trow_info += [str(MF_Struct[row][val]['avg'])]\n""")
        algorithmFile.write("""\t\t\t\telse:\n\t\t\t\t\trow_info += [str(MF_Struct[row][val])]\n""")
        algorithmFile.write("""\t\t\toutput.add_row(row_info)\n\t\tevalString = ''\n""")

        #there is no having condition, thus every MFStruct row will be in the output table
        algorithmFile.write("""\telse:\n\t\trow_info = []\n\t\tfor val in selectAttributes.split(','):\n""")
        algorithmFile.write("""\t\t\tif len(val.split('_')) > 1 and val.split('_')[1] == 'avg':\n\t\t\t\trow_info += [str(MF_Struct[row][val]['avg'])]\n""")
        algorithmFile.write("""\t\t\telse:\n\t\t\t\trow_info += [str(MF_Struct[row][val])]\n""")
        algorithmFile.write("""\t\toutput.add_row(row_info)\n""")    
        algorithmFile.write("""print(output)\n""")
        algorithmFile.close()