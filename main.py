from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import mysql.connector
import pandas as pd
import re
import os, time
from datetime import datetime
import warnings
warnings.filterwarnings("ignore")
import etl
import charts as ch
import credintails

os.system('cls')
etl.load()
os.system('cls')
print('Preparing to work'.center(130)+'\n')

#2 Application Front-End
# Load from DB, creating all dataframes for tables, same tempview 
# and one corrected with date type for TIMED field from Credit table
spark = SparkSession.builder.appName('Cards').getOrCreate()
db_session = mysql.connector.connect(user=credintails.login()[0], password=credintails.login()[1])
db_pointer = db_session.cursor()
db_pointer.execute("USE creditcard_capstone;")

def from_DB (t_name):
    t_name_full = f"creditcard_capstone.{t_name}"
    df = spark.read.format("jdbc") \
    .options(driver="com.mysql.cj.jdbc.Driver",\
    user=credintails.login()[0],\
    password=credintails.login()[1],\
    url="jdbc:mysql://localhost:3306/creditcard_capstone",\
    dbtable=t_name_full).load()
    return df

df_branch=from_DB("CDW_SAPP_BRANCH")
df_credit=from_DB("CDW_SAPP_CREDIT_CARD")
df_cust=from_DB("CDW_SAPP_CUSTOMER")
df_loan=from_DB("`CDW-SAPP_loan_application`")
df_credit.createOrReplaceTempView("credit_table")
df_cust.createOrReplaceTempView("cust_table")
df_credit.withColumn('TIMED', F.to_date(df_credit.TIMED, 'yyyyMMdd')).createOrReplaceTempView("credit_table_date")
# Load and correct dictinary for the chart 3.2
state_dict = pd.read_csv(str(os.path.sys.path[0])+'\Assets\states.csv',index_col=0).to_dict('dict')['ALL']
state_dict = {v: k for k, v in state_dict.items()}

#3.1 Creating pandas dataframe for the chart from Spark DF using PySpark commands
df_gr1=df_credit.select('TRANSACTION_TYPE').groupby('TRANSACTION_TYPE').count().orderBy(F.col('count').desc())
pd_gr1 = df_gr1.withColumnRenamed("TRANSACTION_TYPE","Type of transaction"). \
withColumnRenamed("count","Transaction rate").toPandas()
#3.2 Creating pandas dataframe for the chart from Spark DF using PySpark commands, change names for states
pd_gr2=df_cust.select('CUST_STATE').groupby('CUST_STATE').count().orderBy(F.col('count').desc()). \
withColumnRenamed("CUST_STATE","Customer state").withColumnRenamed("count","Number of customers"). \
replace(state_dict, 1).toPandas()
#3.3 Creating pandas dataframe for the chart from Spark DF using PySpark commands,
# create field with full name and new line literal for futher chart using
df_gr3=df_credit.select('CUST_SSN','TRANSACTION_VALUE').groupby('CUST_SSN').sum()
df_ssn_join=df_gr3.join(df_cust,df_gr3.CUST_SSN == df_cust.SSN)
pd_gr3 = df_ssn_join.withColumn('CUST_SSN', F.concat(df_ssn_join['FIRST_NAME'],F.lit('\n'),df_ssn_join['MIDDLE_NAME'],F.lit('\n'), \
df_ssn_join['LAST_NAME'])).orderBy(F.col('sum(TRANSACTION_VALUE)').desc()).limit(10). \
select(F.col("CUST_SSN").alias("Customer name"), F.col('sum(TRANSACTION_VALUE)').alias("Sum of transactions")).toPandas()
pd_gr3.sort_values(by=['Sum of transactions'],ascending=True,inplace=True)
pd_gr3.set_index('Customer name',inplace=True)
# 5.1 Creating pandas dataframe for the chart from dictinory using PySpark commands
gr4_se_total=df_loan.filter((F.col('Self_Employed')=='Yes')).count()
dict_gr4={'Approved':100*df_loan.filter((F.col('Self_Employed')=='Yes') & \
(F.col('Application_Status')=='Y')).count()/gr4_se_total, \
 'Not approved': 100*df_loan.filter((F.col('Self_Employed')=='Yes') & \
(F.col('Application_Status')=='N')).count()/gr4_se_total}
pd_gr4=pd.DataFrame(list(dict_gr4.items()),columns=['Category','Value'])
#5.2 Creating pandas dataframe for the chart from dictinory with dictinory value transformation using PySpark commands
total_applic=df_loan.select('Application_Status').count()
dict_gr5={'Male married applicants rejection rate':['Male','Yes','N',total_applic], \
'Male married applicants approving rate':['Male','Yes','Y', total_applic], \
'Female married applicants rejection rate':['Female','Yes','N', total_applic], \
'Female married applicants approving rate':['Female','Yes','Y', total_applic]}
for k,v in dict_gr5.items():
    dict_gr5[k]=100*(df_loan.filter((F.col('Gender')==v[0]) & (F.col('Married')==v[1]) & \
    (F.col('Application_Status')==v[2])).count())/v[3]
pd_gr5=pd.DataFrame(list(dict_gr5.items()),columns=['Category','Value'])
#5.3 Creating pandas dataframe for the chart with SQL commands and month change
pd_gr6=spark.sql("SELECT month(credit_table_date.TIMED) AS Month, SUM(credit_table_date.TRANSACTION_VALUE) AS Transactions \
FROM credit_table_date GROUP BY Month ORDER BY Transactions DESC LIMIT 3").toPandas()
month_dict={1:'January',2:'February',3:'March',4:'April',5:'May',6:'June',7:'July',8:'August',9:'September', \
10:'October',11:'November',12:'December'}
for k,v in month_dict.items(): pd_gr6.loc[:,'Month'].replace(k,v, inplace=True)
pd_gr6['Transactions'].astype('float').round(2)
#5.4 Creating pandas dataframe for the chart with SQL commands
pd_gr7=spark.sql("SELECT credit_table_date.BRANCH_CODE, SUM(credit_table_date.TRANSACTION_VALUE) AS Healthcare \
FROM credit_table_date WHERE TRANSACTION_TYPE='Healthcare' GROUP BY BRANCH_CODE ORDER BY Healthcare DESC"). \
toPandas().set_axis(['Branch', 'Healthcare transactions'], axis='columns')

os.system('cls')
# Functions for the module 2.1 and 2.2
def numcheck(numb_2_chek):
    x=re.findall(r"^[(][0-9]{3}[)][0-9]{3}[-][0-9]{4}$",numb_2_chek)
    if x != []: return True
    else: return False
def mailcheck(mail_2_chek):
    x=re.findall(r"([A-Za-z0-9]+[.-_])*[A-Za-z0-9]+@[A-Za-z0-9-]+(\.[A-Z|a-z]{2,})+",mail_2_chek)
    if x != []: return True
    else: return False

#2.1.1 Input filtered by type, length, as well as by zip, year+month (if not in the database request not executing). Time used in string format.
# For Month input adding 0, if needed. Request used PySpark commands.
def request1():
    w=True
    while w:
        os.system('cls')
        print('\nDisplay the transactions made by customers living in a given zip code for a given month and year')
        print('For exit to the main menu enter 0 for all inputs (Zip, Year, Month)')
        z=input("Enter ZIP: ")
        y=input("Enter Year: ")
        m=input("Enter Month: ")
        if z.isdigit() and y.isdigit() and m.isdigit():
            if int(z)==int(y)==int(m)==0:
                os.system('cls')
                break
            elif len(z)<6 and len(y)==4 and len(m)<3:
                df_ssn_join=df_credit.join(df_cust,df_credit.CUST_SSN == df_cust.SSN)
                zip=df_ssn_join.select('CUST_ZIP').rdd.map(lambda x: x[0]).collect()
                year_month=df_ssn_join.select('TIMED').rdd.map(lambda x: x[0][:6]).collect()
                if len(m) !=2: m='0'+m
                z=int(z)
                if (y+m in year_month) and (z in zip):
                    df_ssn_join.select('TIMED','CUST_ZIP','TRANSACTION_TYPE','TRANSACTION_ID', 'TRANSACTION_VALUE').\
                    filter((df_ssn_join.CUST_ZIP==z) & (df_ssn_join.TIMED.like(y+m+'%'))). \
                    orderBy(F.col('TIMED').desc()).show(100)
                else: print('\nEntered data does not mutch database records') 
            else: print('\nEntered data is incorrect')                 
        else: print('\nIncorrect input. Try again.')
        i_1=input('Do you want to make same another request? (enter 1 for Yes): ')
        if i_1=='1': continue
        else: 
            w = False
            os.system('cls')
#2.1.2 Input filtered by type, correct values from the list of choices. Request used PySpark commands.
def request2():
    w=True
    while w:
        os.system('cls')
        print('\nDisplay the number and total values of transactions for a given type')
        print('For exit to the main menu enter 0\n')
        df_trans_types=df_credit.select('TRANSACTION_TYPE').distinct().rdd.flatMap(lambda x: x).collect()
        for i in range(len(df_trans_types)): print(f'{i+1} for {df_trans_types[i]}')
        z=input("Enter number from the list of transaction types: ")
        if z.isdigit():
            if int(z)==0:
                os.system('cls')
                break
            if int(z) in list(range(1, len(df_trans_types)+1)):
                t_type=df_credit.select('TRANSACTION_TYPE').groupby('TRANSACTION_TYPE').count(). \
                where(F.col('TRANSACTION_TYPE').contains(df_trans_types[int(z)-1]))
                t_value=df_credit.select('TRANSACTION_VALUE', 'TRANSACTION_TYPE').groupby('TRANSACTION_TYPE').sum(). \
                where(F.col('TRANSACTION_TYPE').contains(df_trans_types[int(z)-1]))
                t_type=t_type.join(t_value,['TRANSACTION_TYPE'])
                t_type.select('TRANSACTION_TYPE', F.col("count").alias("Total number for a given type"), \
                F.col("sum(TRANSACTION_VALUE)").alias("Total values of transactions for a given type")).show()
            else: print('\nIncorrect input, try again') 
        else: print('\nOnly numbers allowed, try again')
        i_1=input('Do you want to make same another request? (enter 1 for Yes, any input for Main Menu): ')
        if i_1=='1': continue
        else: 
            w=False
            os.system('cls')
#2.1.3 Input filtered by type, correct values from the list of choices. Request used PySpark commands.
def request3():
    w=True
    while w:
        os.system('cls')
        print('\nDisplay the total number and total values of transactions for branches in a given state')
        print('For exit to the main menu enter 0\n')
        df_branch_join=df_credit.join(df_branch,df_credit.BRANCH_CODE == df_branch.BRANCH_CODE)
        df_branch_state=df_branch_join.select('BRANCH_STATE').distinct().rdd.flatMap(lambda x: x).collect()
        for i in range(len(df_branch_state)): print(f'{i+1} for {state_dict[df_branch_state[i]]}')
        z=input("Enter number from the list of states: ")
        if z.isdigit():
            if int(z)==0:
                os.system('cls')
                break
            if int(z) in list(range(1, len(df_branch_state)+1)):
                t_type=df_branch_join.select('BRANCH_STATE','TRANSACTION_VALUE').groupby('BRANCH_STATE').count(). \
                where(F.col('BRANCH_STATE').contains(df_branch_state[int(z)-1]))
                t_value=df_branch_join.select('BRANCH_STATE','TRANSACTION_VALUE').groupby('BRANCH_STATE').sum(). \
                where(F.col('BRANCH_STATE').contains(df_branch_state[int(z)-1]))
                t_type=t_type.join(t_value,['BRANCH_STATE'])
                t_type.select(F.col("count").alias(f"Total number for {state_dict[df_branch_state[int(z)-1]]}"), \
                F.col("sum(TRANSACTION_VALUE)").alias(f"Total values of transactions for {df_branch_state[int(z)-1]}")).show()
            else: print('\nIncorrect input, try again')                
        else: print('\nOnly numbers allowed, try again')
        i_1=input('Do you want to make same another request? (enter 1 for Yes, any input for Main Menu): ')
        if i_1=='1': continue
        else: 
            w=False
            os.system('cls')
#2.2.1 Request used SQL commands with controling for empty result to find result with SSN or full name data input with low/capitalise solving
def request4():
    w = True
    while w:
        os.system('cls')
        print('\nCheck the existing account details of a customer')
        print('Customer could be identificated by SSN or personal data (First Name, Middle name, Last name)')
        pd_inp=input('For exit to the main menu enter 0, for identification by personal data enter 1 or any other number for SSN identification: ')
        if pd_inp.isdigit():
            if int(pd_inp)==0:
                os.system('cls')
                break
            if int(pd_inp)!=1:
                ssn_inp=input("Enter SSN number : ")
                res=spark.sql(f"SELECT cust_table.SSN, cust_table.FIRST_NAME, cust_table.MIDDLE_NAME, \
                cust_table.LAST_NAME, credit_table.BRANCH_CODE, credit_table.CUST_CC_NO \
                FROM cust_table JOIN credit_table ON  credit_table.CUST_SSN=cust_table.SSN \
                WHERE cust_table.SSN = {int(ssn_inp)}")
                if res.rdd.isEmpty():
                    print('\nEntered data does not mutch database records')
                else: 
                    res.show(30)
            else:  
                cust_f=input("Enter customer First Name : ")
                cust_m=input("Enter customer Middle Name: ")
                cust_l=input("Enter customer Last Name: ")
                res=spark.sql(f"SELECT cust_table.SSN, cust_table.FIRST_NAME, cust_table.MIDDLE_NAME, \
                cust_table.LAST_NAME, credit_table.BRANCH_CODE, credit_table.CUST_CC_NO \
                FROM cust_table JOIN credit_table ON  credit_table.CUST_SSN=cust_table.SSN \
                WHERE cust_table.FIRST_NAME = '{cust_f.capitalize()}' \
                AND cust_table.MIDDLE_NAME = '{cust_m.lower()}' AND cust_table.LAST_NAME = '{cust_l.capitalize()}'")
                if res.rdd.isEmpty():
                    print('\nEntered data does not mutch database records')
                else: 
                    res.show(30)
        else: 
            print('\nOnly numbers allowed, try again')
        i_1=input('Do you want to make same another request? (enter 1 for Yes, any input for Main Menu): ')
        if i_1=='1': continue
        else: 
            w=False
            os.system('cls')
#2.2.2 Input SSN cheked in database. It is using like a key to modify all other fields. Full name data input with low/capitalise solving,
# phone number and Email inputs controlled by templates, zip for digit and length. Current timestamp saved with new data to the DB.
# Request used SQL commands. 
def request5():
    w=True
    while w:
        os.system('cls')
        print('\nModify the existing account details of a customer. Phone must be in format (XXX)XXX-XXXX')
        print('For exit to the main menu enter 0\n')
        ssn_inp=input("Enter SSN number : ")
        if ssn_inp.isdigit():
            if int(ssn_inp)==0:
                os.system('cls')
                break
            elif (int(ssn_inp) in df_cust.select('SSN').rdd.map(lambda x: x[0]).collect()):
                count=1
                menu_choises={}
                for i in df_cust.columns[1:-1]:
                    print(f'{count} for ',i)
                    menu_choises.update({count:i}) 
                    count+=1  
                z=input("Enter number for the field to modify: ")
                if z.isdigit():
                    z=int(z)
                    if z<count and z in menu_choises.keys():
                        f_modif=input(f"Enter data for the field {menu_choises[z]} to modify: ")
                        f_modif_ins="'"+f'{f_modif}'+"'"
                        if z==1 or z==3: f_modif_ins="'"+f'{f_modif.strip().capitalize()}'+"'"
                        elif z==2: f_modif_ins="'"+f'{f_modif.strip().lower()}'+"'"
                        elif z==9 and f_modif.isdigit() and len(f_modif)<6: f_modif_ins=int(f_modif)
                        elif z==10 and numcheck(f_modif): pass
                        elif z==11 and mailcheck(f_modif): pass
                        elif z==4 or z==5 or z==6 or z==7 or z==8: pass
                        else:
                            print('\nIncorrect input. Try again.')
                            time.sleep(3)
                            continue
                        try:
                            for _ in range(3):
                                db_pointer.execute("alter table CDW_SAPP_CUSTOMER drop primary key;")
                                db_pointer.execute("ALTER TABLE CDW_SAPP_CUSTOMER ADD PRIMARY KEY (SSN);")
                                db_pointer.execute(f"UPDATE CDW_SAPP_CUSTOMER SET {menu_choises[z]}={f_modif_ins} WHERE SSN={int(ssn_inp)};")
                                db_pointer.execute(f"UPDATE CDW_SAPP_CUSTOMER SET LAST_UPDATED=current_timestamp WHERE SSN={int(ssn_inp)};")
                        except: print('Too long for the database field')
                    else:
                        print('\nEntered data is incorrect')
            else: 
                print('\nEntered SSN does not mutch database records')               
        else: 
            print('\nIncorrect input. Try again.')
        i_1=input('\nDo you want to make same another request? (enter 1 for Yes, any other input for Main menu): ')
        if i_1=='1': continue
        else: 
            w = False
            os.system('cls')
#2.2.3  Input filtered by type and length. Request used SQL commands, controling for empty result. Time used in string format.
def request6():
    w=True
    while w:
        os.system('cls')
        print('\nGenerate a monthly bill for a credit card number for a given month and year')
        print('For exit to the main menu enter 0 for credit card number')
        z=input("Enter credit card number: ")
        y=input("Enter Year: ")
        m=input("Enter Month: ")
        if z.isdigit() and y.isdigit() and m.isdigit():
            if int(z)==int(y)==int(m)==0:
                os.system('cls')
                break
            elif len(z)<17 and len(y)==4 and len(m)<3:
                if len(m) !=2: m='0'+m
                z=int(z)
                res=spark.sql(f"SELECT CUST_CC_NO, SUM(TRANSACTION_VALUE) AS MONTLY_BILL FROM credit_table \
                WHERE CUST_CC_NO = {z} AND TIMED LIKE '{y+m}__' GROUP BY CUST_CC_NO")
                if res.rdd.isEmpty():
                    print('\nEntered data does not mutch database records')
                else: 
                    res.show(50)
            else: print('\nIncorrect input. Try again.') 
        else: print('\nOnly digits allowed')                 
        i_1=input('Do you want to make same another request? (enter 1 for Yes): ')
        if i_1=='1': continue
        else: 
            w = False
            os.system('cls')
#2.2.4 Input control correct format for dates. Request used SQL commands, controling for empty result.
# For request could be used SSN or full name data input with low/capitalise solving. Time used in datetime format.
def request7():
    w = True
    while w:
        os.system('cls')
        print('\nDisplay the transactions made by a customer between two dates')
        print('For exit to the main menu enter 0 for both from the next input\n')
        date1=input('Input start date in number format (4 digits for Year, 1 or 2 for Month and Day: ')
        date2=input('Input end date in format (4 digits for Year, 1 or 2 for Month and Day: ')
        if date1.isdigit() and date2.isdigit():
            if int(date1)==int(date2)==0:
                os.system('cls')
                break
            else:
                try:
                    date1=datetime.strptime(date1, '%Y%m%d')
                    date2=datetime.strptime(date2, '%Y%m%d')
                except:
                    print('\nIncorrect date.')
                    time.sleep(3)
                    continue
            print('Customer could be identificated by SSN or personal data (First Name, Middle name, Last name)')
            pd_inp=input('For identification by personal data enter 1 or any other input for SSN identification: ')
            if pd_inp!='1':
                ssn_inp=input("Enter SSN number : ")
                res=spark.sql(f"SELECT credit_table_date.TIMED, credit_table_date.CUST_SSN, cust_table.FIRST_NAME, cust_table.MIDDLE_NAME, \
                cust_table.LAST_NAME, credit_table_date.TRANSACTION_TYPE, credit_table_date.TRANSACTION_VALUE, \
                credit_table_date.TRANSACTION_ID \
                FROM credit_table_date JOIN cust_table ON credit_table_date.CUST_SSN=cust_table.SSN \
                WHERE cust_table.SSN = {int(ssn_inp)} AND (credit_table_date.TIMED BETWEEN '{date1}' AND '{date2}') \
                ORDER BY year(credit_table_date.TIMED) DESC, month(credit_table_date.TIMED) DESC, \
                day(credit_table_date.TIMED) DESC")
                if res.rdd.isEmpty():
                    print('\nEntered data does not mutch database records')
                else:
                    res.withColumn("TIMED", F.date_format(F.col("TIMED"), "yyyyMMdd")).show(30)
            else:
                cust_f=input("Enter customer First Name : ")
                cust_m=input("Enter customer Middle Name: ")
                cust_l=input("Enter customer Last Name: ")
                res=spark.sql(f"SELECT credit_table_date.TIMED, credit_table_date.CUST_SSN, cust_table.FIRST_NAME, cust_table.MIDDLE_NAME, \
                cust_table.LAST_NAME, credit_table_date.TRANSACTION_TYPE, credit_table_date.TRANSACTION_VALUE, \
                credit_table_date.TRANSACTION_ID \
                FROM credit_table_date JOIN cust_table ON credit_table_date.CUST_SSN=cust_table.SSN \
                WHERE (cust_table.FIRST_NAME = '{cust_f.capitalize()}' AND cust_table.MIDDLE_NAME = '{cust_m.lower()}' \
                AND cust_table.LAST_NAME = '{cust_l.capitalize()}') \
                AND (credit_table_date.TIMED BETWEEN '{date1}' AND '{date2}') \
                ORDER BY year(credit_table_date.TIMED) DESC, month(credit_table_date.TIMED) DESC, \
                day(credit_table_date.TIMED) DESC")
                if res.rdd.isEmpty():
                    print('\nEntered data does not mutch database records')
                else:
                    res.withColumn("TIMED", F.date_format(F.col("TIMED"), "yyyyMMdd")).show(100)            
        else: 
            print('\nOnly numbers allowed, try again')
        i_1=input('Do you want to make same another request? (enter 1 for Yes, any input for Main Menu): ')
        if i_1=='1': continue
        else: 
            w=False
            os.system('cls')

# Main terminal
while True:
    print("\t**********************************************".center(120))
    print("\t***              Main Menu                 ***".center(120))
    print("\t**********************************************".center(120))
    print('\n'+'<<< Please choose request by number. For Exit enter 0 >>>'.center(125)+'\n')
    print('\t'*4+'1. Display the transactions made by customers living in a given zip code for a given month and year')
    print('\t'*4+'2. Display the number and total values of transactions for a given type')
    print('\t'*4+'3. Display the total number and total values of transactions for branches in a given state')
    print('\t'*4+'4. Check the existing account details of a customer')
    print('\t'*4+'5. Modify the existing account details of a customer')
    print('\t'*4+'6. Generate a monthly bill for a credit card number for a given month and year')
    print('\t'*4+'7. Display the transactions made by a customer between two dates')
    print('\t'*4+'8. Visualization for Credit Card data')
    print('\t'*4+'9. Visualization for Loan Application data')
    z=input("\nMake your choice: ")
    if z not in ['0','1','2','3','4','5','6','7','8','9']:
        print('\nIncorrect input, try again\n')
        time.sleep(3)
        os.system('cls')
    else:
        if z=='0': quit()
        if z=='1': request1()     
        if z=='2': request2() 
        if z=='3': request3()
        if z=='4': request4()
        if z=='5': request5()
        if z=='6': request6()
        if z=='7': request7()
        if z=='8': ch.gr_group1(pd_gr1, pd_gr3, pd_gr2)
        if z=='9': ch.gr_group2(pd_gr4, pd_gr5, pd_gr6, pd_gr7)