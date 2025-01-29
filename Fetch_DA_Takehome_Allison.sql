-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Fetch - Data Analyst Take Home
-- MAGIC Allison Liu 01/28/2025
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Table of Content
-- MAGIC 1. Data Cleaning & Data Aggregation
-- MAGIC - User Table
-- MAGIC - Transactions Table
-- MAGIC - Products Table
-- MAGIC 2. Three Key Questions
-- MAGIC - 2.1 Explore the Data  
-- MAGIC Are there any data quality issues present?  
-- MAGIC Are there any fields that are challenging to understand?
-- MAGIC - 2.2 Provide SQL Queries  
-- MAGIC What are the top 5 brands by receipts scanned among users 21 and over?  
-- MAGIC What are the top 5 brands by sales among users that have had their account for at least six months?  
-- MAGIC Who are Fetch’s power users?
-- MAGIC - 2.3 Communicate with Stakeholders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 1. Data Cleaning & Data Aggregation
-- MAGIC Understand 3 key tables and perform data preprocessing.  
-- MAGIC **Workflow**:   
-- MAGIC 1. Verify column types and identify non-numeric values
-- MAGIC 2. Detect outliers
-- MAGIC 3. Handle missing values
-- MAGIC 4. Column aggregation
-- MAGIC 5. Remove duplicate rows and drop unnecessary columns

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC User = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/liu02637@umn.edu/USER_TAKEHOME.csv")
-- MAGIC Transactions = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/liu02637@umn.edu/TRANSACTION_TAKEHOME.csv")
-- MAGIC Products = spark.read.format("csv").option("header", "true").load("dbfs:/FileStore/shared_uploads/liu02637@umn.edu/PRODUCTS_TAKEHOME.csv")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### User Table
-- MAGIC **Workflow**:
-- MAGIC 1. Verify column types and convert to correct column types
-- MAGIC 4. Categorize `GENDER` column
-- MAGIC 5. Calculate age and accounts holding time
-- MAGIC 7. Handle missing values
-- MAGIC 8. Visualize the users by age, state, and accounts duration groups
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Verify column types and convert to correct column types  
-- MAGIC Understand column types and extract date from `CREATED_DATE` and `BIRTH_DATE`

-- COMMAND ----------

-- MAGIC %python
-- MAGIC User.toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check column types
-- MAGIC User.dtypes

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date, col
-- MAGIC
-- MAGIC # Convert the string columns of CREATED_DATE and BIRTH_DATE to date type using 'to_date'
-- MAGIC User = User.withColumn("CREATED_DATE", to_date(col("CREATED_DATE"), "yyyy-MM-dd"))
-- MAGIC User = User.withColumn("BIRTH_DATE", to_date(col("BIRTH_DATE"), "yyyy-MM-dd"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
-- MAGIC User.limit(10).toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check the unique values in the LANGUAGE column
-- MAGIC User.groupby('LANGUAGE').count().toPandas()
-- MAGIC
-- MAGIC # After verifying the language code 'es-419', I found that it represents Spanish (Latin America).

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Categorize `GENDER` groups  
-- MAGIC Replace self-reported answers with predefined categories.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check gender categories
-- MAGIC User.groupby('GENDER').count().toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Replace gender into categories
-- MAGIC User = User.toPandas()
-- MAGIC User['GENDER'] = User['GENDER'].replace({"My gender isn't listed": 'not_listed', 
-- MAGIC                                          'Prefer not to say':'prefer_not_to_say'})
-- MAGIC User

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Calculate `AGE` and `ACCOUNT_MONTHS`  
-- MAGIC Use **today**'s date to calculate the user's age and the duration of account ownership in months

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Calculate age from today
-- MAGIC from datetime import date
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC today = pd.Timestamp(date.today())
-- MAGIC User['AGE'] = ((today - pd.to_datetime(User['BIRTH_DATE'])).dt.days / 365).round(1)
-- MAGIC
-- MAGIC User

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Based on Fetch's Terms of Service, which state that the services are not directed at children **under 13 years old** and that Fetch does not knowingly collect or maintain personal information from children under 13, I believe it is reasonable to exclude this age group from further analysis.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Remove users under age of 13
-- MAGIC User = User[~(User['AGE'] < 13)]
-- MAGIC User.shape

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Calculate accounts holding months
-- MAGIC User['ACCOUNT_MONTHS'] = ((today - pd.to_datetime(User['CREATED_DATE'])).dt.days / 30).round(1)
-- MAGIC User

-- COMMAND ----------

-- MAGIC %md
-- MAGIC I checked the Fetch App release year and found that actual users should not have created accounts before **2017**. Therefore, I assumed that these were only testing accounts before the app launched, so I removed them.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Remove created year before 2017
-- MAGIC User = User[User['CREATED_DATE'] >= pd.to_datetime('2017-01-01').date()]
-- MAGIC User

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Drop duplicates
-- MAGIC User = User.drop_duplicates()
-- MAGIC User.shape

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check distrubution of user data
-- MAGIC User.describe(include='all')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Handle missing values  
-- MAGIC By writing a function to generate a summary table for missing values

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Write a function to show the missing values table
-- MAGIC def missing_values_summary(df):
-- MAGIC     missing_summary = pd.DataFrame({
-- MAGIC         'Column': df.columns,
-- MAGIC         'Non-Null Count': df.notnull().sum(),
-- MAGIC         '% of Missing Values': (df.isnull().sum() / len(df)) * 100
-- MAGIC     }).reset_index(drop=True)
-- MAGIC     
-- MAGIC     return missing_summary
-- MAGIC
-- MAGIC # Call the function on User DataFrame
-- MAGIC missing_values_summary(User)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC It’s challenging to decide whether to remove missing values or keep them, as each user row is meaningful. Therefore, I would like to request guidance on the appropriate actions.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Convert to tempview for SQL using
-- MAGIC User = spark.createDataFrame(User)
-- MAGIC User.createOrReplaceTempView("User")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Visualize User Table
-- MAGIC - Top 20 States
-- MAGIC - Age group distribution
-- MAGIC - Account holding year distribution

-- COMMAND ----------

-- DBTITLE 1,Top 20 Number of Users by States
select State, count(*) as STATE_CNT
from User
where State is not null
group by State
order by 2 desc
limit 20

-- COMMAND ----------

-- DBTITLE 1,Number of Users by Age Group
SELECT
    CASE
        WHEN AGE BETWEEN 11 AND 20 THEN '11-20'
        WHEN AGE BETWEEN 20 AND 30 THEN '21-30'
        WHEN AGE BETWEEN 30 AND 40 THEN '31-40'
        WHEN AGE BETWEEN 40 AND 50 THEN '41-50'
        WHEN AGE BETWEEN 50 AND 60 THEN '51-60'
        WHEN AGE BETWEEN 60 AND 70 THEN '61-70'
        WHEN AGE BETWEEN 70 AND 80 THEN '71-80'
        WHEN AGE BETWEEN 80 AND 90 THEN '81-90'
        ELSE '91+'
    END AS AGE_GROUP,
    COUNT(*) AS COUNT
FROM User
WHERE AGE is not null
GROUP BY AGE_GROUP
ORDER BY AGE_GROUP;

-- COMMAND ----------

-- DBTITLE 1,Number of Users by Accounts Holding Years
SELECT
    CASE
        WHEN ACCOUNT_MONTHS <= 12 THEN 'less than 1 year'
        WHEN ACCOUNT_MONTHS BETWEEN 12 AND 24 THEN '1-2 years'
        WHEN ACCOUNT_MONTHS BETWEEN 24 AND 36 THEN '2-3 years'
        WHEN ACCOUNT_MONTHS BETWEEN 36 AND 48 THEN '3-4 years'
        WHEN ACCOUNT_MONTHS BETWEEN 48 AND 60 THEN '4-5 years'
        WHEN ACCOUNT_MONTHS BETWEEN 60 AND 72 THEN '5-6 years'
        WHEN ACCOUNT_MONTHS BETWEEN 72 AND 84 THEN '6-7 years'
        WHEN ACCOUNT_MONTHS > 84 THEN 'more then 7 years'
    END AS ACCOUNT_MONTHS_GROUP,
    COUNT(*) AS Count
FROM User
WHERE ACCOUNT_MONTHS is not null
GROUP BY ACCOUNT_MONTHS_GROUP
ORDER BY ACCOUNT_MONTHS_GROUP;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Based on the graphs above, we can see that most of our users are from TX, FL, and CA, and are between 21-50 years old. Additionally, most users created their accounts 2-5 years ago.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Transactions Table
-- MAGIC 1. Verify column types, convert to correct types, and identify non-numeric values
-- MAGIC 2. Identify and handle outliers in `FINAL_QUANTITY`
-- MAGIC 3. Aggregate data by `RECEIPT_ID`, `USER_ID`, and `BARCODE`
-- MAGIC 4. Filter out `FINAL_SALE` = 0 and `PURCHASE_DATE` later than `SCAN_DATE`
-- MAGIC 5. Visualize transaction trends by `FINAL_QUANTITY` and `FINAL_SALE`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Verify column types, convert non-numeric `FINAL_QUANTITY` into numeric, and understand the Transactions table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC Transactions = Transactions.toPandas()
-- MAGIC Transactions.sort_values(by='RECEIPT_ID')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC I found that the Transactions table is inconsistent in `FINAL_QUANTITY` and `FINAL_SALE` when there are duplicate and same `RECEIPT_ID`, `USER_ID`, and `BARCODE` values. Therefore, it is important to resolve this issue using an aggregation function.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC Transactions = spark.createDataFrame(Transactions)
-- MAGIC # Convert the string columns to date type using 'to_date'
-- MAGIC Transactions = Transactions.withColumn("SCAN_DATE", to_date(col("SCAN_DATE"), "yyyy-MM-dd"))
-- MAGIC Transactions = Transactions.withColumn("PURCHASE_DATE", to_date(col("PURCHASE_DATE"), "yyyy-MM-dd"))
-- MAGIC
-- MAGIC Transactions.limit(5).toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Attempt to cast the column 'FINAL_QUANTITY' to a numeric type
-- MAGIC Transactions = Transactions.withColumn("FINAL_QUANTITY_NEW", 
-- MAGIC                                       col("FINAL_QUANTITY").cast("double"))
-- MAGIC
-- MAGIC # Filter rows where 'FINAL_QUANTITY_NEW' is null and count occurrences of 'FINAL_QUANTITY'
-- MAGIC Transactions.filter(col("FINAL_QUANTITY_NEW").isNull()) \
-- MAGIC            .groupBy("FINAL_QUANTITY") \
-- MAGIC            .count() \
-- MAGIC            .toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import col, when
-- MAGIC
-- MAGIC # Transfer 'zero' to 0
-- MAGIC Transactions = Transactions.withColumn(
-- MAGIC     "FINAL_QUANTITY",
-- MAGIC     when(col("FINAL_QUANTITY") == 'zero', "0").otherwise(col("FINAL_QUANTITY"))
-- MAGIC )
-- MAGIC Transactions = Transactions.drop('FINAL_QUANTITY_NEW')
-- MAGIC Transactions.limit(5).toPandas()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Convert quantity and sale columns into double
-- MAGIC Transactions = Transactions.withColumn("FINAL_QUANTITY", col("FINAL_QUANTITY").cast("double"))
-- MAGIC Transactions = Transactions.withColumn("FINAL_SALE", col("FINAL_SALE").cast("double"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Filter outliers in `FINAL_QUANTITY`  
-- MAGIC When grouping by `FINAL_QUANTITY`, I identified that **quantities with decimal values represented outliers and were not meaningful for quantity analysis**. Therefore, I removed rows with non-integer `FINAL_QUANTITY`, which accounted for **0.22%** of the dataset.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check distribution of FINAL_QUANTITY and identify the large gap between integer values and decimal values
-- MAGIC Transactions = Transactions.toPandas()
-- MAGIC Transactions.groupby('FINAL_QUANTITY')['RECEIPT_ID'].count().reset_index(name='CNT').head(25)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Calculate percentage of outliers
-- MAGIC Transactions[Transactions['FINAL_QUANTITY'] % 1 != 0].shape[0] / Transactions.shape[0] * 100
-- MAGIC # 110 / 50000

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Remove non-integer final quantity
-- MAGIC Transactions = Transactions[Transactions['FINAL_QUANTITY'] % 1 == 0]
-- MAGIC Transactions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Drop duplicated rows
-- MAGIC Transactions = Transactions.drop_duplicates()
-- MAGIC Transactions.shape

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Aggregate data by RECEIPT_ID, USER_ID, and BARCODE  
-- MAGIC I believe that each `RECEIPT_ID` with the same `PURCHASE_DATE`, `SCAN_DATE`, `STORE_NAME`, `USER_ID`, and `BARCODE` should **only have one `FINAL_QUANTITY` and `FINAL_SALE`**. However, I found that there's multiple entries with the same `RECEIPT_ID`, `USER_ID`, and `BARCODE`, and the reason for this could be _either users attempting to earn more rewards from the same receipt or technical issues_. Therefore, I used an aggregate function to get the **maximum** of `FINAL_QUANTITY` and `FINAL_SALE` to retain as much information as possible.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check the details of multiple entries from same receipt
-- MAGIC Transactions[Transactions['RECEIPT_ID'] == '431fe612-ed55-470e-939c-043ad31f33f3']

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Fill null value of final sale with 0
-- MAGIC Transactions['FINAL_SALE'] = Transactions['FINAL_SALE'].fillna(0).astype(float)
-- MAGIC # Filling null barcode with a placeholder like 'UNKNOWN' to grouping rows together
-- MAGIC Transactions['BARCODE'] = Transactions['BARCODE'].fillna("UNKNOWN")
-- MAGIC
-- MAGIC # Aggregate Transactions table using the maximum values of final_sale and final_quantity
-- MAGIC Transactions = Transactions.groupby(
-- MAGIC     ['RECEIPT_ID', 'PURCHASE_DATE', 'SCAN_DATE', 'STORE_NAME', 'USER_ID', 'BARCODE']
-- MAGIC ).agg({
-- MAGIC     'FINAL_QUANTITY': 'max',  
-- MAGIC     'FINAL_SALE': 'max'       
-- MAGIC }).reset_index()
-- MAGIC
-- MAGIC Transactions.sort_values(by='RECEIPT_ID')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Filter out `FINAL_SALE` = 0 and `PURCHASE_DATE` is later than `SCAN_DATE`  
-- MAGIC I assumed that a quantity of 0 might occur because users could forget to fill in the information or face difficulties scanning. However, it's impossible for a purchase date to be later than the scan date or for the sale value to be zero, I removed these cases.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Remove final_sale = 0
-- MAGIC Transactions = Transactions[Transactions['FINAL_SALE'] != 0]
-- MAGIC Transactions.shape

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Remove purchase date later than scan date
-- MAGIC Transactions = Transactions[Transactions['PURCHASE_DATE'] <= Transactions['SCAN_DATE']]
-- MAGIC Transactions.shape

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC missing_values_summary(Transactions)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC By using the aggregation function, I ensure that there are no duplicate `RECEIPT_ID` and `BARCODE` entries scanned by the same person and each receipt all having sales values.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC Transactions.describe(include='all')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Transactions = spark.createDataFrame(Transactions)
-- MAGIC Transactions.createOrReplaceTempView("Transactions")

-- COMMAND ----------

-- Identify the date range for purchasing and scanning 
select min(purchase_date) as min_purchase, 
      max(purchase_date) as max_purchase,
      min(scan_date) as min_scan, 
      max(scan_date) as max_scan
from Transactions

-- COMMAND ----------

-- Visualize distribution of quantity 
SELECT 
    CASE 
        WHEN FINAL_QUANTITY = 0 THEN '0'
        WHEN FINAL_QUANTITY BETWEEN 1 AND 5 THEN '01-05'
        WHEN FINAL_QUANTITY BETWEEN 6 AND 10 THEN '06-10'
        WHEN FINAL_QUANTITY BETWEEN 11 AND 20 THEN '11-20'
        ELSE '21+'
    END AS quantity_range, 
    COUNT(*) AS count
FROM Transactions
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- Visualize distribution of quantity 
SELECT 
    CASE 
        WHEN final_sale BETWEEN 1 AND 25 THEN '01-25'
        WHEN final_sale BETWEEN 25 AND 50 THEN '25-50'
        WHEN final_sale BETWEEN 50 AND 75 THEN '50-75'
        WHEN final_sale BETWEEN 75 AND 100 THEN '75-100'
        ELSE '100+'
    END AS sale_range, 
    COUNT(*) AS count
FROM Transactions
GROUP BY 1
ORDER BY 1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC According to the charts above, there is a large number of transactions with quantities ranging from 1 to 5 and sales values between $1 and $25.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Products Table
-- MAGIC 1. Verify column types and identify non-numeric values
-- MAGIC 2. Identify hierarchy of product categories
-- MAGIC 3. Drop duplicated rows

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Convert to pandas
-- MAGIC Products = Products.toPandas()
-- MAGIC Products

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check column types
-- MAGIC Products.dtypes

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Filter out null barcode
-- MAGIC Products = Products[~Products['BARCODE'].isnull()]
-- MAGIC Products

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Identify hierarchy of product categories using groupby
-- MAGIC Product_category = Products.groupby(['CATEGORY_1', 'CATEGORY_2', 'CATEGORY_3', 'CATEGORY_4'])['BARCODE'].count().reset_index()
-- MAGIC
-- MAGIC Product_category

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Drop duplicates
-- MAGIC Products = Products.drop_duplicates()
-- MAGIC Products.shape

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC
-- MAGIC # Check percentage of missing values 
-- MAGIC missing_values_summary(Products)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC Products = spark.createDataFrame(Products)
-- MAGIC Products.createOrReplaceTempView("Products")

-- COMMAND ----------

-- Display products table
select *
from Products
limit 10

-- COMMAND ----------

-- Calculate number of distinct barcode
select count(distinct BARCODE) as Num_Barcode
from Products

-- COMMAND ----------

-- Calculate number of distinct brand
select count(distinct brand) as Num_Brand
from Products

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 2. Three Key Questions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.1 Explore the Data
-- MAGIC Review the unstructured csv files and answer the following questions with code that supports your conclusions:  
-- MAGIC **1. Are there any data quality issues present?**
-- MAGIC   - **Data Accuracy**: For all three datasets, I ensured the data accurately reflects real-world values. For example, I removed unreasonable ages—such as users under 13 years old—to comply with Fetch's Terms of Service. Additionally, I verified the Fetch App release year to ensure that users registered their accounts after the release date.
-- MAGIC   - **Data Completeness**: I checked column types and retained any non-numeric values if they were meaningful. However, I found a high percentage of missing values across the three tables. For example, 26.8% of brands are missing in the *Products* table, 3.7% of age are missing in the *User* table.
-- MAGIC   - **Data Consistency**: I ensured data consistency within and across different datasets to support accurate analysis. For example, while reviewing the *Transactions* table, I identified an issue where the same receipt, purchase date, and barcode appeared in multiple rows but with different quantities and sales. To address this, I used aggregate functions to clean the data effectively.  
-- MAGIC
-- MAGIC **2. Are there any fields that are challenging to understand?**
-- MAGIC - **Category** in *Products* table: In the category column, I found that it's challenging to directly understand the hierarchy of each products, it have to use grouping function to see the subcategory of each product category. 
-- MAGIC - **Barcode** column: Based on the ERM, it specifies that the barcode should be an integer data type, but I believe this is unreasonable because each barcode represents a unique key value for each product. Therefore, I kept the data type of the barcode as a string.
-- MAGIC - *Transactions* Table: A significant issue in the *Transactions* table is the presence of multiple rows with the same `receipt_id`, `purchase_date`, `scan_date`, `user_id`, and `barcode`, even when `final_quantity` or `final_sale` is missing. I would like to determine whether this issue stems from a technical error or customer behavior. Additionally, it is important to identify solutions to prevent such problems in the future to ensure data analysis is conducted effectively and efficiently.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2.2 Provide SQL Queries
-- MAGIC What are the top 5 brands by receipts scanned among users 21 and over?  
-- MAGIC What are the top 5 brands by sales among users that have had their account for at least six months?  
-- MAGIC Who are Fetch’s power users?  

-- COMMAND ----------

-- MAGIC %md
-- MAGIC First, I performed an inner join between the _Products_ and _Transactions_ tables, finding that they matched with over 9,000 entries. However, when I inner joined with the _User_ table, I found only 130 rows. This indicates that **Fetch's users may have registered but never used the Fetch App (churned customers), or they might have scanned receipts while using guest mode**. This discrepancy is important to investigate, as it provides insight into the App's long-term customer engagement and lifetime value.

-- COMMAND ----------

-- Check for matching records between the Transactions and Products tables
select *
from Transactions T inner join Products P using(barcode)

-- COMMAND ----------

-- Check matching rows between transactions and user data
select *
from Transactions T inner join User U on T.user_id = U.id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Question 1. What are the top 5 brands by receipts scanned among users 21 and over?
-- MAGIC
-- MAGIC Using the main table, _Transactions_, I performed an left join with both the _User_ and _Products_ tables to count receipts by brand.

-- COMMAND ----------

-- DBTITLE 1,Top 5 brands by receipts scanned
-- Group by brand and count scanned receipts
select brand, count(receipt_id) as cnt
from Transactions T left join User U on U.id = T.user_id
left join Products P using(barcode)
where age >= 21 and brand is not null
group by brand
order by cnt desc
limit 5

-- COMMAND ----------

-- DBTITLE 1,Details of products among the top 5 brand
-- Look at the details of what products they bought
select *
from Transactions T inner join User U on U.id = T.user_id
join Products P using(barcode)
where brand = 'DOVE' or brand = 'NERDS CANDY' or brand = 'SOUR PATCH KIDS' or brand = 'TRIDENT' or brand = 'COCA-COLA'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Based on the table above, among the five brands, I believe SOUR PATCH KIDS and NERDS CANDY are the two main competitors in the confectionery candy market, while TRIDENT focuses on gum. Additionally, DOVE appears to be a popular choice for bar soap and body wash products.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Question 2.2 What are the top 5 brands by sales among users that have had their account for at least six months?  
-- MAGIC
-- MAGIC Using the main table, _Transactions_, I performed an left join with both the _User_ and _Products_ tables can calculate total sales by brand.

-- COMMAND ----------

-- DBTITLE 1,Top 5 brands by sales
-- Group by brand and sum up final sales 
select brand, round(sum(final_sale), 2) as total_sale
from Transactions T left join User U on U.id = T.user_id
left join Products P using(barcode)
where ACCOUNT_MONTHS >= 6 and brand is not null
group by brand
order by total_sale desc
limit 5

-- COMMAND ----------

-- DBTITLE 1,Details of unit price by brand
select brand, round(avg(final_sale), 2) as avg_sale, round(avg(final_quantity), 2) as avg_quantity
from Transactions T inner join User U on U.id = T.user_id
inner join Products P using(barcode)
where brand is not null
group by brand
order by avg_sale desc, avg_quantity desc
limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Based on the results above, I can summarize that CVS, DOVE, and TRIDENT had the highest sales among users with transactions and accounts that have been active for at least six months. When examining the details of average sales and quantity by brand, I observed that these brands tend to sell products with a higher unit price compared to other brands, indicating that customers are spending more money on these items.
-- MAGIC
-- MAGIC Therefore, I recommend that **Fetch explore potential partnerships with high-unit-price brands, as this could provide customers with more opportunities to earn rewards and, in turn, enhance app loyalty**. This strategy would not only boost customer engagement but also drive higher-value transactions, reinforcing brand partnerships and long-term customer retention.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Question 2.3 Who are Fetch’s power users?
-- MAGIC
-- MAGIC Define Power Users:
-- MAGIC - Power users are those with more than 2 receipts scanned in the past year and significant spending.

-- COMMAND ----------

-- DBTITLE 1,Fetch's power users
-- Group by user id and calculate number of scanned receipts and total sales
select ID, count(receipt_id) as num_receipts, round(sum(final_sale), 2) as total_sale
from Transactions T left join User U on U.id = T.user_id
where ID is not null
group by ID
having num_receipts > 1
order by total_sale desc, num_receipts desc
limit 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Based on the result, the top user scanned 2 receipts and spent $75.99, while others scanned 2-3 receipts and had total sales ranging from $14.27 to $26.14.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2.3 Communicate with stakeholders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Subject: Data Quality Issues and Insights from Recent Investigation**
-- MAGIC
-- MAGIC Hi stakeholders,  
-- MAGIC I wanted to share a quick summary of my investigation and some key points on the data:
-- MAGIC
-- MAGIC **Data Quality Issues:**
-- MAGIC - Missing Data: I've found that 26.8% of brands in the Products table and 3.7% of ages in the User table are missing.
-- MAGIC - Inconsistencies: In the Transactions table, some receipts are duplicated with different quantities and sales. I’ve cleaned this up but would like to understand whether this is a technical issue or user behavior.  
-- MAGIC
-- MAGIC **Insights:**
-- MAGIC - User Engagement: There’s a large mismatch between users and transactions—only 130 users matched when joining with the User table, suggesting that many users may have churned or scanned receipts in guest mode.
-- MAGIC - Top Brands: CVS, DOVE, and TRIDENT have the highest sales among users active for at least six months. These brands have higher unit prices, which could be a great opportunity for partnership to boost customer loyalty.
-- MAGIC - Power Users: I’ve identified top users who scan receipts and spend significantly. Targeting these users with rewards could increase engagement.  
-- MAGIC
-- MAGIC **Actions Request:**
-- MAGIC Can you help explore solutions for the missing data and assist with understanding the user-transaction mismatch to improve customer engagement insights?
-- MAGIC
-- MAGIC Let me know if you’d like to discuss further.
-- MAGIC
-- MAGIC Warm regards,  
-- MAGIC Allison
