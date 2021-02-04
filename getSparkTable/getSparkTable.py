#imports
import pandas as pd
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark import SparkContext, SparkConf
import psycopg2
import os


class Vars:
    """Function to get Variables from a stored class"""
    def __init__(self):
        self.url = '../data/credit_unions.csv'
        self.hostName = 'noahcook_db_1'
        self.eins = ''
        self.pandasBool = False
        self.filterBool = True

    def __str__(self):
        """Function to get Variables from a stored class as a string"""
        return 'No string for you!'

    def setHostName(self, name):
        assert isinstance(name, str)
        self.hostName = name
        
    def setURL(self, url):
        assert isinstance(url, str)
        self.url = url
        
    def setPandasBool(self, pandasBool):
        assert isinstance(pandasBool, bool)
        self.pandasBool = pandasBool

    def setFilterBool(self, filterBool):
        assert isinstance(filterBool, bool)
        self.filterBool = filterBool


variables = Vars()


def getSparkTable(tableName, hostName='', eins='', pandasBool=False, filterBool=True):
    """Pulls given table from irs990 database."""
    if hostName == '':
        hostName = variables.hostName
    if not pandasBool:
        pandasBool = variables.pandasBool
    if filterBool:
        filterBool = variables.filterBool

    connection = makeConnection(hostName)

    spark = makeSession()

    table = pullTable(spark, tableName, hostName)

    filtered_table = filterTable(table, eins)

    filtered_pandas = filtered_table.toPandas()

    if pandasBool:
        return filtered_pandas
    elif filterBool:
        return filtered_table
    else:
        return table


def makeConnection(hostName=''):
    """Makes a connections to the irs 990 table.  Requires name of db docker container name."""
    if hostName == '':
        hostName = variables.hostName
    # Actually making the connection
    connection = psycopg2.connect(
        host = hostName, # Name of database docker container
        database = 'irs990', # Name of database
        user = 'postgres', # Username for our database
        password = 'postgres1234', # Password for our database
        port='5432' # Port to be used for our connection
    )
    
    return connection
    

def makeSession():
    """Makes a spark session to connect to our database."""
    # Configuration settings for the session
    conf = (SparkConf()
    .set("spark.ui.port", "4041") # port of which our spark session is running on
    .set('spark.executor.memory', '4G') # RAM available to our spark session
    .set('spark.driver.memory', '45G') # Simulated HDD for our spark session
    .set('spark.driver.maxResultSize', '10G') # Size of largest results back from requests
    .set('spark.jars', '/home/jovyan/scratch/postgresql-42.2.18.jar')) # Not sure what this does????
    
    # Create the session
    spark = SparkSession.builder \
    .appName('test') \
    .config(conf=conf) \
    .getOrCreate()
    
    return spark
    

def pullTable(spark, tableName, hostName=''):
    """Retrieves table from irs990 database.  Needs table name and db docker container name."""
    if hostName == '':
        hostName = variables.hostName
    # Creating the properties for the table that we are going to pull from our database
    table_properties = {
    'driver': 'org.postgresql.Driver',  # should match the type of sql that our db uses I think
    'url': 'jdbc:postgresql://' + hostName + ':5432/irs990', # Location of where our database is
    'user': 'postgres', # Username for our database
    'password': 'postgres1234', # Password for our database
    'dbtable': tableName, # Name of table in our database that we want to pull
    }
    
    table = spark.read \
    .format('jdbc') \
    .option('driver', table_properties['driver']) \
    .option('url', table_properties['url']) \
    .option('dbtable', table_properties['dbtable']) \
    .option('user', table_properties['user']) \
    .load()
    
    return table
    
    
def filterTable(table, eins=''):
    """Pulls only given type from Spark table.  Eins are specified list of class of interest beforehand"""
    if eins == '':
        eins = getEINS()
    return table.where(f.col("ein").isin(eins))


def getEINS(url=''):
    if url == '':
        url = variables.url
    # read in credit union data
    dat = pd.read_csv(url)
    # create list of CUs eins
    return dat.ein.to_list()
