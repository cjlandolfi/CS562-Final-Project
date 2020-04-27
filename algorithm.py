import postgresql
from databaseConfig import dbConfig
MF_Struct = {'columns': {}}
for attribute in 'cust,1_sum_quant,2_sum_quant,3_sum_quant'.split(','):
   if len(attribute.split('_')) > 1:
       if attribute.split('_')[1] in ['sum', 'avg', 'min', 'max', 'count']:
           MF_Struct['columns'][attribute] = 'int'
   else:
       if attribute == 'cust' or attribute == 'prod' or attribute == 'state':
           MF_Struct['columns'][attribute] = 'str'
       if attribute == 'day' or attribute == 'month' or attribute == 'year' or attribute == 'quant':
           MF_Struct['columns'][attribute] = 'int'
print(MF_Struct)
db = postgresql.open(user = dbConfig['user'],password = dbConfig['password'],host = dbConfig['host'],port = dbConfig['port'],database = dbConfig['database'],)

query = db.prepare("SELECT * FROM sales;")
for row in query:
    key = ''
    for attr in groupingAttributes.split(','):
        key += row[attr]
    if key not in MF_Struct.keys():
        MF_Struct[key] = row
    print(row['cust'], row)