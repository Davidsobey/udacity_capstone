from pyspark.sql import SparkSession, DataFrame
from pyspark import SparkConf
from pyspark.sql.functions import input_file_name, substring_index, to_timestamp, year, month, dayofweek, weekofyear, date_format, lit
from pyspark.sql.types import IntegerType, DoubleType
from yaml import FullLoader, load


def create_table(df: DataFrame, table_name: str):
    user = "postgres"
    password = ""
    url = "jdbc:postgresql://localhost:5432/capstone"
    properties = {
        "user": user,
        "password": password
    }
    df.write.jdbc(url=url, table=table_name,
                  mode="overwrite", properties=properties)


def read_table(table_name: str, spark: SparkSession):
    user = "postgres"
    password = ""
    url = "jdbc:postgresql://localhost:5432/capstone"
    properties = {
        "user": user,
        "password": password
    }
    df = spark.read.jdbc(url=url, table=table_name, properties=properties)
    return df


def build_stocks_dataframe(df: DataFrame):
    df = df.withColumn("act_symbol", substring_index(substring_index(df.filename, "/", -1), ".", 1)) \
        .withColumn('date_formatted', to_timestamp(df.Date, 'yyyy-MM-dd')) \
        .withColumn('full_date', date_format(df.Date, "yyyyMMdd").cast(IntegerType()))
    df = df.drop("filename", "OpenInt", "Date")
    return df


def main():
    """
    Main entry point for the application
    :return: None
    """
    # Create Spark Session
    spark = SparkSession.builder.config(
        "spark.jars", "./drivers/postgresql-42.2.18.jar").getOrCreate()

    # Load raw tables from source
    exhange_df = spark.read.json('./data/exchange_list.json')
    country_df = spark.read.json('./data/country_list.json')
    df = spark.read.json(
        './data/nyse-listed_json.json').withColumn('Exchange', lit('N'))
    df2 = spark.read.json('./data/other-listed_json.json')
    df3 = spark.read.csv('./data/ETFs/*.txt', sep=',',
                         header=True).withColumn("filename", input_file_name())

    # Create stocks dataframe for NYSE
    df3 = build_stocks_dataframe(df3)

    # Create stocks dataframe for Others
    df4 = spark.read.csv('./data/Stocks/*.txt', sep=',',
                         header=True).withColumn("filename", input_file_name())
    df4 = build_stocks_dataframe(df4)

    # Union Stock Tables and rename columns
    stock_daily_df = df3.unionByName(df4).dropDuplicates()
    stock_daily_df = stock_daily_df \
        .withColumnRenamed("Open", "open") \
        .withColumnRenamed("High", "high") \
        .withColumnRenamed("Low", "low") \
        .withColumnRenamed("Close", "close") \
        .withColumnRenamed("Volume", "volume")

    # Create date table from execution date
    datedf = stock_daily_df.select("date_formatted", "full_date").distinct() \
        .withColumn("day_of_week", dayofweek("date_formatted")) \
        .withColumn("week_of_year", weekofyear("date_formatted")) \
        .withColumn("month", month("date_formatted")) \
        .withColumn("year", year("date_formatted")) \
        .drop("date_formatted")

    # Create time PSQL Table
    create_table(datedf, "trade_day")

    # Clean the stock table
    stock_daily_df = stock_daily_df.drop("date_formatted")
    stock_daily_df = stock_daily_df \
        .withColumn("open", stock_daily_df.open.cast(DoubleType())) \
        .withColumn("high", stock_daily_df.high.cast(DoubleType())) \
        .withColumn("low", stock_daily_df.low.cast(DoubleType())) \
        .withColumn("close", stock_daily_df.close.cast(DoubleType())) \
        .withColumn("volume", stock_daily_df.volume.cast(IntegerType()))

    # Create the PSQL Table
    create_table(stock_daily_df, "daily_stock")

    # Union the companies from the two different stock tables
    company_df = df.union(df2.select("ACT Symbol", "Company Name", "Exchange"))

    # Clean the company table
    company_df = company_df.dropDuplicates() \
        .withColumnRenamed("ACT Symbol", "act_symbol") \
        .withColumnRenamed("Company Name", "company_name") \
        .withColumnRenamed("Exchange", "exchange_code")

    # Create last three tables
    create_table(company_df, "company_details")
    create_table(country_df, "country_details")
    create_table(exhange_df, "exchange_details")

    # Checks
    table_list = ["company_details", "country_details",
                  "daily_stock", "exchange_details", "trade_day"]
    
    table_column_count = [3, 2, 7, 3, 5]
    
    for i, table in enumerate(table_list):
        df: DataFrame = read_table(table, spark)
        if df.count() > 0:
            print(f"Pass: Total count for {table} is: {df.count()}")
        else:
            print("No data loaded")
            raise Exception(LookupError)

        if len(df.columns) == table_column_count[i]:
            print(f"Table lenght is correct for {table} at: {len(df.columns)}")
        else:
            print("Incorrect Number of Columns")
            raise Exception(LookupError)

if __name__ == "__main__":
    main()
