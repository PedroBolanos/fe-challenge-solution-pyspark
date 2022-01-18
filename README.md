# fe-challenge-solution-pyspark
Spark job that ingests data from 2 groups of .csv files into PostgreSQL relational database.

## Deployment
Go to the top of *data-loading.py* file and find the **GLOBAL VARIABLES** and **POSTGRESS CONNECTION** section.
1.  Set **zip_file_path** to the local path where the zip file to be ingested is. **Make sure the zip file is in that path.**
2.  Set **extract_at_path** to the local path where the zip file is going to be extracted to.
3.  Set **pg_database** to the name of a database. **Make sure the database exists before submitting the job**.
4.  Set **pg_schema** to the name of a schema from given database. **Make sure the schema exists under given database before submitting the job**.
5.  Set **pg_host** properties to the wanted values.
6.  Set **pg_port** properties to the wanted values.
7.  Set **pgOptions** properties to the wanted values.
8.  Make sure all the imported libraries are installed in the environment.
9.  Make sure **postgresql-42.2.24.jar** file is in SPARK_HOME/.../jars directory.
10.  If running locally use command **python3 pandas-data-loading.py** to start its execution.

## Analytics
All the queries to answer the questions about the data are comment (respective question) separated in *analytics.sql* file.