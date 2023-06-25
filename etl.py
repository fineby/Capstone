from pyspark.sql.types import StructType,StructField,StringType,IntegerType,TimestampType
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import mysql.connector
import requests
import pandas as pd
import re
import os, time

# All paths to files linked to the directory with main 3 Python files.
def load(usr, passw):
    os.system('cls')
    print('Loading and mapping data'.center(130))
    # Reading customer table with near to requested order of fields, with integer type for key (SSN) and string for all other columns.
    # Dropping rows without key, change style for First, Middle, Last names.    
    spark = SparkSession.builder.appName('Cards').getOrCreate()
    schema_cust = StructType([StructField('SSN',IntegerType(),True),
    StructField('FIRST_NAME',StringType(),True),
    StructField('MIDDLE_NAME',StringType(),True),  
    StructField('LAST_NAME',StringType(),True), 
    StructField('CREDIT_CARD_NO',StringType(),True),
    StructField('STREET_NAME',StringType(),True), 
    StructField('APT_NO',StringType(),True), 
    StructField('CUST_CITY',StringType(),True),
    StructField('CUST_STATE',StringType(),True),
    StructField('CUST_COUNTRY',StringType(),True),
    StructField('CUST_ZIP', StringType(),True), 
    StructField('CUST_PHONE',StringType(),True),
    StructField('CUST_EMAIL',StringType(),True),
    StructField('LAST_UPDATED',StringType(),True)])
    df_cust = spark.read.schema(schema_cust).json(str(os.path.sys.path[0])+'\Assets\cdw_sapp_custmer.json').na.drop(subset=['SSN']). \
    withColumnRenamed("CREDIT_CARD_NO","Credit_card_no").withColumnRenamed("CUST_PHONE","CUST_PHONE1")
    df_cust = df_cust.withColumn('FIRST_NAME', F.initcap(df_cust.FIRST_NAME)). \
    withColumn('MIDDLE_NAME', F.lower(df_cust.MIDDLE_NAME)). \
    withColumn('LAST_NAME', F.initcap(df_cust.LAST_NAME)) 
    # Creating new column for address, as requested.
    addr_cust=df_cust.select(df_cust.SSN, F.concat_ws(',',df_cust.APT_NO, df_cust.STREET_NAME).alias('FULL_STREET_ADDRESS1'))
    df_cust=df_cust.join(addr_cust, ['SSN'])
    pd_cust=df_cust.drop('STREET_NAME','APT_NO').toPandas()
    pd_cust.insert(loc = 5, column = 'FULL_STREET_ADDRESS', value = pd_cust['FULL_STREET_ADDRESS1'])
    pd_cust.drop('FULL_STREET_ADDRESS1',axis=1, inplace=True)
    df_cust=spark.createDataFrame(pd_cust)
    # Adding state codes, changing format for phone number.
    codes_dict = pd.read_csv(str(os.path.sys.path[0])+'\Assets\\area-codes.csv',index_col=0).to_dict('dict')['Region']
    codes_dict = {v: k for k, v in codes_dict.items()}
    rdd_cust_phone=df_cust.select('SSN', 'CUST_PHONE1', 'CUST_STATE').rdd
    rdd_cust_phone=rdd_cust_phone.map(lambda x: str(codes_dict[x[2]])+x[1]) 
    rdd_cust_phone=rdd_cust_phone.map(lambda x:'(%s)%s-%s' % tuple(re.findall(r'\d{4}$|\d{3}', x))) 
    rdd_cust_code=df_cust.select('SSN').rdd.flatMap(lambda x: x)
    df_cust_phone = spark.createDataFrame(data = rdd_cust_code.zip(rdd_cust_phone), \
    schema ="`SSN` INT, `CUST_PHONE2` STRING")
    pd_cust=df_cust.join(df_cust_phone, ['SSN']).toPandas()
    pd_cust.insert(loc = 11, column = 'CUST_PHONE', value = pd_cust['CUST_PHONE2'])
    pd_cust.drop(['CUST_PHONE1','CUST_PHONE2'],axis=1, inplace=True)
    df_cust=spark.createDataFrame(pd_cust)
    # Correcting types
    df_cust=df_cust.withColumn('SSN',df_cust['SSN'].cast(IntegerType())). \
    withColumn('CUST_ZIP',df_cust['CUST_ZIP'].cast(IntegerType())). \
    withColumn('LAST_UPDATED',df_cust['LAST_UPDATED'].cast(TimestampType()))

    # Reading branch table with requested order of fields, with integer type for key (BRANCH_CODE) and string for all other columns.
    # Dropping rows without key, change ZIP for null to 99999.
    schema_branch="`BRANCH_CODE` INT, `BRANCH_NAME` STRING, `BRANCH_STREET` STRING, `BRANCH_CITY` STRING, `BRANCH_STATE` STRING, `BRANCH_ZIP` STRING, `BRANCH_PHONE` STRING, `LAST_UPDATED` STRING"
    df_branch = spark.read.format('json').load(str(os.path.sys.path[0])+'\Assets\CDW_SAPP_BRANCH.json', schema=schema_branch). \
    na.drop(subset=['BRANCH_CODE']).na.fill(value='99999',subset=['BRANCH_ZIP']).withColumnRenamed("BRANCH_PHONE","BRANCH_PHONE1")
    # Change format for phone number
    rdd_branch_phone=df_branch.select('BRANCH_CODE', 'BRANCH_PHONE1'). \
    rdd.map(lambda x:'(%s)%s-%s' % tuple(re.findall(r'\d{4}$|\d{3}', x[1])))
    rdd_branch_code=df_branch.select('BRANCH_CODE').rdd.flatMap(lambda x: x)
    df_branch_phone = spark.createDataFrame(data = rdd_branch_code.zip(rdd_branch_phone), \
    schema ="`BRANCH_CODE` INT, `BRANCH_PHONE2` STRING")
    pd_branch=df_branch.join(df_branch_phone, ['BRANCH_CODE']).toPandas()
    pd_branch.insert(loc = 7, column = 'BRANCH_PHONE', value = pd_branch['BRANCH_PHONE2'])
    pd_branch.drop(['BRANCH_PHONE1','BRANCH_PHONE2'],axis=1, inplace=True)
    df_branch=spark.createDataFrame(pd_branch)
    # Correcting types
    df_branch=df_branch.withColumn('BRANCH_CODE',df_branch['BRANCH_CODE'].cast(IntegerType())). \
    withColumn('BRANCH_ZIP',df_branch['BRANCH_ZIP'].cast(IntegerType())). \
    withColumn('LAST_UPDATED',df_branch['LAST_UPDATED'].cast(TimestampType()))

    # Reading credit card table with automatic schema
    df_credit = spark.read.load(str(os.path.sys.path[0])+'\Assets\cdw_sapp_credit.json', format="json", header = True, inferSchema = True). \
    na.drop(subset=['TRANSACTION_ID']). \
    withColumnRenamed("CREDIT_CARD_NO","CUST_CC_NO")  
    df_credit = df_credit.withColumn('TRANSACTION_ID',df_credit['TRANSACTION_ID'].cast(IntegerType()))
    # Creating TIMED column, change names and order for columns.
    time_credit=df_credit.select(df_credit.TRANSACTION_ID, F.concat_ws(' ',df_credit.YEAR, df_credit.MONTH, df_credit.DAY).alias('TIMED1'))
    df_credit=df_credit.join(time_credit, ['TRANSACTION_ID'])
    pd_credit=df_credit.drop('YEAR','MONTH','DAY').toPandas()
    pd_credit.rename(columns={"CUST_CC_NO": "CUST_CC_NO1", "CUST_SSN": "CUST_SSN1", "BRANCH_CODE": "BRANCH_CODE1", \
    "TRANSACTION_TYPE": "TRANSACTION_TYPE1", "TRANSACTION_VALUE": "TRANSACTION_VALUE1"}, inplace=True)
    pd_credit.insert(loc = 0, column = 'CUST_CC_NO', value = pd_credit['CUST_CC_NO1'])
    pd_credit.insert(loc = 1, column = 'TIMED', value = pd.to_datetime(pd_credit['TIMED1'], format='%Y %m %d'))
    pd_credit.insert(loc = 2, column = 'CUST_SSN', value = pd_credit['CUST_SSN1'])
    pd_credit.insert(loc = 3, column = 'BRANCH_CODE', value = pd_credit['BRANCH_CODE1'])
    pd_credit.insert(loc = 4, column = 'TRANSACTION_TYPE', value = pd_credit['TRANSACTION_TYPE1'])
    pd_credit.insert(loc = 5, column = 'TRANSACTION_VALUE', value = pd_credit['TRANSACTION_VALUE1'])
    pd_credit.drop(['CUST_CC_NO1','TIMED1','CUST_SSN1','BRANCH_CODE1','TRANSACTION_TYPE1','TRANSACTION_VALUE1'],axis=1, inplace=True)
    df_credit=spark.createDataFrame(pd_credit)
    # Correcting types
    df_credit = df_credit.withColumn("TIMED", F.date_format(F.col("TIMED"), "yyyyMMdd")). \
    withColumn('CUST_SSN',df_credit['CUST_SSN'].cast(IntegerType())). \
    withColumn('BRANCH_CODE',df_credit['BRANCH_CODE'].cast(IntegerType())). \
    withColumn('TRANSACTION_ID',df_credit['TRANSACTION_ID'].cast(IntegerType()))

    #4
    # 4.1 Get data from End point
    url='https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json'
    response=requests.get(url)
    os.system('cls')
    print("API endpoint for the loan application dataset", url)
    # 4.2 Find the status code of the above API endpoint
    print("Status code of the above API endpoint" , response.status_code)
    time.sleep(10)
    os.system('cls')
    loan_dict=response.json()
    pd_df_loan = pd.DataFrame(loan_dict)
    df_loan = spark.createDataFrame(pd_df_loan)
    # Correcting types and simple cleaning of data (deleting + for Dependens)
    df_loan=df_loan.withColumn('Dependents', F.regexp_replace('Dependents', '^([0-9]*)\+', '$1'))
    df_loan=df_loan.withColumn('Dependents',df_loan['Dependents'].cast(IntegerType())) \
    .withColumn('Credit_History',df_loan['Credit_History'].cast(IntegerType()))

    #1.2 Create and save to DB
    print('Loading to the Database'.center(130))
    db_session = mysql.connector.connect(user=usr, password=passw)
    db_pointer = db_session.cursor()
    db_pointer.execute("DROP DATABASE IF EXISTS creditcard_capstone;")
    db_pointer.execute("CREATE DATABASE IF NOT EXISTS creditcard_capstone;")
    db_pointer.execute("USE creditcard_capstone;")

    def to_DB (t_name,fname):
        t_name_full = "creditcard_capstone.{0}".format(t_name)
        fname.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable", t_name_full) \
        .option("user", usr) \
        .option("password", passw) \
        .save()
        # Corrected types in DB from string to varchar with maximum length equal to maximum length for the field
        df_str_name=[f.name for f in fname.schema.fields if f.dataType==StringType()]
        df_str_len=[(fname.select(F.max(F.length(f.name))).toPandas().iloc[0,0]) for f in fname.schema.fields if f.dataType==StringType()]
        for i in range(len(df_str_name)): db_pointer.execute(f"ALTER TABLE {t_name} MODIFY {df_str_name[i]} VARCHAR({df_str_len[i]});")

    to_DB("CDW_SAPP_BRANCH", df_branch)
    to_DB("CDW_SAPP_CREDIT_CARD", df_credit)
    to_DB("CDW_SAPP_CUSTOMER", df_cust)
    try: db_pointer.execute("ALTER TABLE CDW_SAPP_CUSTOMER ADD PRIMARY KEY (SSN);")
    except: pass
    #4.3
    to_DB("`CDW-SAPP_loan_application`", df_loan)
    os.system('cls')
    print('All data have been transformed and loaded to the Database'.center(130))
    time.sleep(5)
    os.system('cls')