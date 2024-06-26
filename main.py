from google.cloud import bigquery
from google.oauth2 import service_account
import pandas


credentials = service_account.Credentials.from_service_account_file('C:/Users/diouc/OneDrive/Bureau/Business/Datasets/Google Cloud Key/python-roject-a6566e288ed9.json')

#Creatin a client object
client = bigquery.Client(credentials=credentials)


# Construct a reference to the "openad" dataset that contains air quality data
dataset_ref = client.dataset("openaq", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

#datasets are list of tables, we need to list all tables from the hacker news dataset
tables = list(client.list_tables(dataset))
# Lets check the tables in the dataset
for table in tables:
    print(table.table_id)
    print("-------------------")

#let's knows fetch de table named "crime"
table_ref = dataset_ref.table("global_air_quality")
table = client.get_table(table_ref)

print(table.schema)
print("-------------------")

# Preview the first five lines of the "full" table
print(client.list_rows(table, max_results=5).to_dataframe())
print("-------------------")

#Query_1 will list countries with reported pollution levels in units of "ppm"?
query_1 = """
              SELECT DISTINCT country, unit
              FROM `bigquery-public-data.openaq.global_air_quality`
              WHERE unit = "ppm"
              """
query_job = client.query(query_1)
query_1_results = query_job.to_dataframe()

#Check the query result:
print(f"Query 1 Results :\n {query_1_results.head()} \n")
print("-------------END--------------")

#Query_2 will select all columns of the rows where the value column is 0
query_2 = """
              SELECT *
              FROM `bigquery-public-data.openaq.global_air_quality`
              WHERE value = 0
              """
query_job = client.query(query_2)
query_2_results = query_job.to_dataframe()

#Check the query result:
print(f"Query 2 Results :\n {query_2_results.head()} \n")
print("-------------END--------------")

#Query_3 how many time units appears
query_3 = """
              SELECT unit, count(unit) AS Iteration
              FROM `bigquery-public-data.openaq.global_air_quality`
              GROUP BY unit 
              HAVING count(unit) > 1
              """
query_job = client.query(query_3)
query_3_results = query_job.to_dataframe()

#Check the query result:
print(f"Query 3 Results :\n {query_3_results.head()} \n")
print("-------------END--------------")

#Query_4 average ppm pollution score value per country
query_4 = """
              SELECT country, AVG(value) AS Avg_value
              FROM `bigquery-public-data.openaq.global_air_quality`
              WHERE unit = "ppm"
              GROUP BY country 
              ORDER BY Avg_value
              """
query_job = client.query(query_4)
query_4_results = query_job.to_dataframe()

#Check the query result:
print(f"Query 4 Results :\n {query_4_results.head()} \n")
print("-------------END--------------")

#Query_5 Counting number of rows for each year
query_5 = """
              SELECT EXTRACT(YEAR FROM timestamp) AS Year, count(1) AS Nb_of_rows
              FROM `bigquery-public-data.openaq.global_air_quality`
              GROUP BY Year
              ORDER BY Year DESC
              """
query_job = client.query(query_5)
query_5_results = query_job.to_dataframe()

#Check the query result:
print(f"Query 5 Results :\n {query_5_results.head()} \n")
print("-------------END--------------")

#-----------------------------------DATASET number 2--------------

# Construct a reference to the dataset that holds data from StackOverflow posts
dataset_ref_2 = client.dataset("stackoverflow", project="bigquery-public-data")

dataset_2 = client.get_dataset(dataset_ref_2)

table_ref_2 = dataset_ref_2.table("posts_questions")
stackoverflow_post_table = client.get_table(table_ref_2)

print(stackoverflow_post_table.schema)
print("-------------------")

table_ref_3 = dataset_ref_2.table("posts_answers")
stackoverflow_answer_table = client.get_table(table_ref_3)

print(stackoverflow_answer_table.schema)
print("-------------------")

query_6 = """
              SELECT q.id AS q_id,
                  MIN(TIMESTAMP_DIFF(a.creation_date, q.creation_date, SECOND)) as time_to_answer
              FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
                  LEFT JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
              ON q.id = a.parent_id
              WHERE q.creation_date >= '2018-01-01' and q.creation_date < '2018-02-01'
              GROUP BY q_id
              ORDER BY time_to_answer
              """

query_6_results = client.query(query_6).result().to_dataframe()
print(" Query 6 : Percentage of answered questions: %s%%" % \
      (sum(query_6_results["time_to_answer"].notnull()) / len(query_6_results) * 100))
print("Number of questions:", len(query_6_results))
query_6_results.head()
print("-------------END--------------")


#Query_7 Users who posted but not answered and users who answered but did not post in juanury 2019
query_7 = """
          SELECT q.owner_user_id AS owner_user_id,
              MIN(q.creation_date) AS q_creation_date,
              MIN(a.creation_date) AS a_creation_date
          FROM `bigquery-public-data.stackoverflow.posts_questions` AS q
              FULL JOIN `bigquery-public-data.stackoverflow.posts_answers` AS a
          ON q.owner_user_id = a.owner_user_id 
          WHERE q.creation_date >= '2019-01-01' AND q.creation_date < '2019-02-01' 
              AND a.creation_date >= '2019-01-01' AND a.creation_date < '2019-02-01'
          GROUP BY owner_user_id
            """
query_job = client.query(query_7)
query_7_results = query_job.to_dataframe()

#Check the query result:
print(f"Query 7 Results :\n {query_7_results.head()} \n")
print("-------------END--------------")
