def sqlQuery():
    with open("algorithm.py", 'a') as algorithmFile:
        algorithmFile.write("""for row in query:\n""") #scan the sales table row by row
        #create the key that will be used in the MF Struct dictionary, this key is a combination of the grouping attributes seperated by a comma
        algorithmFile.write("""\tkey = ''\n\tvalue = {}\n""")
        algorithmFile.write("""\tfor attr in groupingAttributes.split(','):\n\t\tkey += f'{str(row[attr])},'\n""")
        algorithmFile.write("""\tkey = key[:-1]\n""") #removes trailing comma
        #check if the current row corresponds to an existing row in the MF Struct, if not then create a new row for that group and update the new row accordingly

        #if the row is not in the MFStruct, create a new value dictionary to store the values of the columns of the MF Struct
        algorithmFile.write("""\tif key not in MF_Struct.keys():\n\t\tfor groupAttr in groupingAttributes.split(','):\n\t\t\tcolVal = row[groupAttr]\n""") 
        algorithmFile.write("""\t\t\tif colVal:\n\t\t\t\tvalue[groupAttr] = colVal\n""") 
        #loop through the fVects and initalize the values for each aggreagte function being calculated
        #initalize count to 1, sum to the current row's quant value, min and max to the current row's quant value, and average to a dictionary with 3 componenets
        algorithmFile.write("""\t\tfor fVectAttr in fVect.split(','):\n\t\t\ttableCol = fVectAttr.split('_')[1]\n""")
        algorithmFile.write("""\t\t\tif (fVectAttr.split('_')[0] == 'avg'):\n\t\t\t\tvalue[fVectAttr] = {'sum': row[tableCol], 'count': 1, 'avg': row[tableCol]}\n""") #average is stored as a dictionary tracking, sum, count, and average. Each is calculated and stored when the row is updated
        algorithmFile.write("""\t\t\telif (fVectAttr.split('_')[0] == 'count'):\n\t\t\t\tvalue[fVectAttr] = 1\n""")
        algorithmFile.write("""\t\t\telse:\n\t\t\t\tvalue[fVectAttr] = row[tableCol]\n""")
        algorithmFile.write("""\t\tMF_Struct[key] = value\n""") #insert new row into the MFStruct

        #if the row already exists in the MFStruct, update that row based on the values found in the current row in the table
        algorithmFile.write("""\telse:\n\t\tfor fVectAttr in fVect.split(','):\n\t\t\ttableCol = fVectAttr.split('_')[1]\n""") #loop through fVect to check what aggreagates are being calculated
        algorithmFile.write("""\t\t\tif (fVectAttr.split('_')[0] == 'sum'):\n\t\t\t\tMF_Struct[key][fVectAttr] += int(row[tableCol])\n""") #Add the quant to the sum for the corresponding row in the MF Struct
        algorithmFile.write("""\t\t\telif (fVectAttr.split('_')[0] == 'avg'):\n""") #update the 3 values of the average value in the MF Struct
        algorithmFile.write("""\t\t\t\tnewSum = MF_Struct[key][fVectAttr]['sum'] + int(row[tableCol])\n\t\t\t\tnewCount = MF_Struct[key][fVectAttr]['count'] + 1\n\t\t\t\tMF_Struct[key][fVectAttr] = {'sum': newSum, 'count': newCount, 'avg': newSum / newCount}\n""")
        algorithmFile.write("""\t\t\telif (fVectAttr.split('_')[0] == 'count'):\n\t\t\t\tMF_Struct[key][fVectAttr] += 1\n""") #increment count by 1
        algorithmFile.write("""\t\t\telif (fVectAttr.split('_')[0] == 'min'):\n\t\t\t\tif row[tableCol] < MF_Struct[key][fVectAttr]:\n\t\t\t\t\tMF_Struct[key][fVectAttr] = int(row[tableCol])\n""") #check if the row's quant is a new min or max compared to the corresponding row of the MFStruct
        algorithmFile.write("""\t\t\telse:\n\t\t\t\tif row[tableCol] > MF_Struct[key][fVectAttr]:\n\t\t\t\t\tMF_Struct[key][fVectAttr] = int(row[tableCol])\n""")


        #Generate output table(also checks the HAVING condition)
        algorithmFile.write("""output = PrettyTable()\noutput.field_names = selectAttributes.split(',')\nfor row in MF_Struct:\n""")
        algorithmFile.write("""\tevalString = ''\n\tif havingCondition != '':\n""") #create an evalString to be used to check each having condition

        #if there is a having condition, loop through each element of the having condition to fill in the correct information into the evalString
        #the eval string will be equal to the having condition, replaced with the values of the variables in question,
        # then evaluated to check if the row of the MFStruct being examined is to be included in the output table
        algorithmFile.write("""\t\tfor string in havingCondition.split(' '):\n\t\t\tif string not in ['>', '<', '==', '<=', '>=', 'and', 'or', 'not', '*', '/', '+', '-']:\n""")
        algorithmFile.write("""\t\t\t\ttry:\n\t\t\t\t\tint(string)\n\t\t\t\t\tevalString += string\n""")
        algorithmFile.write("""\t\t\t\texcept:\n""")
        algorithmFile.write("""\t\t\t\t\tif len(string.split('_')) > 1 and string.split('_')[0] == 'avg':\n\t\t\t\t\t\tevalString += str(MF_Struct[row][string]['avg'])\n""")
        algorithmFile.write("""\t\t\t\t\telse:\n\t\t\t\t\t\tevalString += str(MF_Struct[row][string])\n""")
        algorithmFile.write("""\t\t\telse:\n\t\t\t\tevalString += f' {string} '\n""")
        algorithmFile.write("""\t\tif eval(evalString.replace('=', '==')):\n\t\t\trow_info = []\n\t\t\tfor val in selectAttributes.split(','):\n""")
        algorithmFile.write("""\t\t\t\tif len(val.split('_')) > 1 and val.split('_')[0] == 'avg':\n\t\t\t\t\trow_info += [str(MF_Struct[row][val]['avg'])]\n""")
        algorithmFile.write("""\t\t\t\telse:\n\t\t\t\t\trow_info += [str(MF_Struct[row][val])]\n""")
        algorithmFile.write("""\t\t\toutput.add_row(row_info)\n\t\tevalString = ''\n""")

        #there is no having condition, thus every MFStruct row will be in the output table
        algorithmFile.write("""\telse:\n\t\trow_info = []\n\t\tfor val in selectAttributes.split(','):\n""")
        algorithmFile.write("""\t\t\tif len(val.split('_')) > 1 and val.split('_')[0] == 'avg':\n\t\t\t\trow_info += [str(MF_Struct[row][val]['avg'])]\n""")
        algorithmFile.write("""\t\t\telse:\n\t\t\t\trow_info += [str(MF_Struct[row][val])]\n""")
        algorithmFile.write("""\t\toutput.add_row(row_info)\n""")    
        algorithmFile.write("""print(output)\n""")
        algorithmFile.close()