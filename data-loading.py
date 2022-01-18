import psycopg2
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as pys_functions
from zipfile import ZipFile

## GLOBAL VARIABLES
input_files_location = "../input-files"
zip_file_path = "../input-files.zip"

## POSTGRESS CONNECTION
pg_database = "challenge"
pg_schema = "public"
pg_host = "127.0.0.1"
pg_port = "5432"
pg_url = "jdbc:postgresql://{0}:{1}/{2}".format(pg_host, pg_port, pg_database)
pgOptions = {
    "user" : "postgres",
    "password" : "**********",
    "driver": "org.postgresql.Driver"
}

## USED TO EXTRACT VALID EMAIL ADDRESSES
rfc_822_regexp = r'(?:(?:\r\n)?[ \t])*(?:(?:(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*\<(?:(?:\r\n)?[ \t])*(?:@(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*(?:,@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*)*:(?:(?:\r\n)?[ \t])*)?(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*\>(?:(?:\r\n)?[ \t])*)|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*:(?:(?:\r\n)?[ \t])*(?:(?:(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*\<(?:(?:\r\n)?[ \t])*(?:@(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*(?:,@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*)*:(?:(?:\r\n)?[ \t])*)?(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*\>(?:(?:\r\n)?[ \t])*)(?:,\s*(?:(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*|(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)*\<(?:(?:\r\n)?[ \t])*(?:@(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*(?:,@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*)*:(?:(?:\r\n)?[ \t])*)?(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|"(?:[^\"\r\\]|\\.|(?:(?:\r\n)?[ \t]))*"(?:(?:\r\n)?[ \t])*))*@(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*)(?:\.(?:(?:\r\n)?[ \t])*(?:[^()<>@,;:\\".\[\] \000-\031]+(?:(?:(?:\r\n)?[ \t])+|\Z|(?=[\["()<>@,;:\\".\[\]]))|\[([^\[\]\r\\]|\\.)*\](?:(?:\r\n)?[ \t])*))*\>(?:(?:\r\n)?[ \t])*))*)?;\s*)'
    
## Spark Initialization
sc = SparkContext.getOrCreate()

URI           = sc._gateway.jvm.java.net.URI
Path          = sc._gateway.jvm.org.apache.hadoop.fs.Path
FileSystem    = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
Configuration = sc._gateway.jvm.org.apache.hadoop.conf.Configuration

spark = SparkSession \
    .builder \
    .appName("data-loading") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

#######     FUNCTIONS   ###########################################################################################

## READ CSV FILES INTO DATAFRAMES
def read_input_files(name):
    return_df = []

    try:
        return_df = spark.read.option("header","true").csv("{0}/{1}_*.csv".format(input_files_location, name))
    except:
        with ZipFile(zip_file_path, 'r') as zipObj:
        # Extract all the contents of zip file in current directory
            zipObj.extractall(input_files_location)
        return_df = spark.read.option("header","true").csv("{0}/{1}_*.csv".format(input_files_location, name))
    
    return return_df

## FORMAT DATE TIMES
## Returns a Dataframe with standardized "date" column
def format_date_time(unformatted_df):
    return unformatted_df.withColumn('date', pys_functions.when(pys_functions.to_timestamp(unformatted_df.date).isNull(), pys_functions.to_timestamp(unformatted_df.date, 'EEE LLL d HH:mm:ss yyyy'))\
                                                                   .otherwise(pys_functions.to_timestamp(unformatted_df.date)))

## CHECK DUPLICATES
## Returns a dataframe resulting from adding row_num column to differenciate records with the same 'id' having 0 in the most recent one
def check_duplicates(df):
    df.createOrReplaceTempView("dataset")
    query = """
    select *, row_number() over (partition by id order by date desc) - 1 as row_num
    from dataset"""
    return spark.sql(query)

## CREATE POSTGRESS TABLE
def create_postgress_table(table_name, columns, datatypes, constraints):
    print(table_name)
    conn = psycopg2.connect(host=pg_host, dbname=pg_database, user=pgOptions["user"], password=pgOptions["password"])
    cursor = conn.cursor()
    create_table = "create table if not exists {0} ( {1} )".format(table_name, ",".join([" ".join([columns[i], datatypes[i], constraints[i]]) for i in range(len(columns))]))
    print(create_table)

    cursor.execute(create_table)
    conn.commit()
    conn.close()

## WRITE DATAFRAME INTO POSTGRESS DATABASE
def write_df_to_ps_table(df, table_name):
    df.write.jdbc(url=pg_url,table=table_name,mode='overwrite', properties=pgOptions)

## OPEN EVENTS PROCESSING
def load_open_events_into_pg():
    name = "open_events"
    ps_table = "{0}.{1}".format(pg_schema, name)

    ## Read open_events from files
    open_events_df = read_input_files(name)

    ## Standardize "date" column
    open_events_df = format_date_time(open_events_df)

    print(open_events_df.schema)

    ## Check for Duplicates
    duplicates_checked_df = check_duplicates(open_events_df)
    ## Dedupes by getting only records where row_num = 0 and droping 'row_num' column
    open_events_df = duplicates_checked_df.where(duplicates_checked_df.row_num == 0).drop('row_num')
    ## The rest of the records go to a historical table that remains with the row_num column
    hist_open_events_df = duplicates_checked_df.where(duplicates_checked_df.row_num > 0)

    ## Create postgres table
    create_postgress_table(ps_table, open_events_df.columns, ["varchar(64)", "timestamp", "varchar(64)", "json"], ["not null", "not null", "not null", ""])

    ## Write into postgress
    write_df_to_ps_table(open_events_df, ps_table)

    ## Create historical table and write into it if there were duplicates
    if(hist_open_events_df.count() > 0):
        ps_table = "engagement.open_events_historic"
        create_postgress_table(ps_table, hist_open_events_df.columns, ["varchar(64)", "timestamp", "varchar(64)", "json", "int"], ["not null", "not null", "not null", "", "not null"])
        write_df_to_ps_table(hist_open_events_df, ps_table)

## RECEIPT EVENTS PROCESSING
def load_receipt_events_into_pg():
    name = "receipt_events"
    ps_table = "{0}.{1}".format(pg_schema, name)

    ## Read receipt_events from files
    receipt_events_df = read_input_files(name)

    ## Standardize "date" column
    receipt_events_df = format_date_time(receipt_events_df)

    ## Standardize "email_address" and "trans_amt" column
    receipt_events_df = receipt_events_df.withColumn('email_address', pys_functions.regexp_extract(receipt_events_df.email_address,rfc_822_regexp,0))\
                                            .withColumn('trans_amt', pys_functions.regexp_replace(receipt_events_df.trans_amt, "[^0-9.]", "").cast("decimal(10,2)"))

    # Uncomment to check schema
    # receipt_events_df.printSchema()

    ## Check for Duplicates
    duplicates_checked_df = check_duplicates(receipt_events_df)
    receipt_events_df = duplicates_checked_df.where(duplicates_checked_df.row_num == 0).drop('row_num')
    hist_receipt_events_df = duplicates_checked_df.where(duplicates_checked_df.row_num > 0)

    ## Create postgres table
    create_postgress_table(ps_table, receipt_events_df.columns, ["varchar(64)", "timestamp", "varchar(64)", "decimal(10,2)", "varchar(64)"], ["not null", "not null", "not null", "not null", ""])

    ## Write into postgress
    write_df_to_ps_table(receipt_events_df, ps_table)

    if(hist_receipt_events_df.count() > 0):
        ps_table = "engagement.receipt_events_historic"
        create_postgress_table(ps_table, hist_receipt_events_df.columns, ["varchar(64)", "timestamp", "varchar(64)", "decimal(10,2)", "varchar(64)", "int"], ["not null", "not null", "not null", "not null", "not null", "not null"])
        write_df_to_ps_table(hist_receipt_events_df, ps_table)

if '__main__':
    load_open_events_into_pg()
    load_receipt_events_into_pg()