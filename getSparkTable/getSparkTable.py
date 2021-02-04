#imports
import pandas as pd
import pyspark
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark import SparkConf
import psycopg2

class Vars:
    """Function to get Variables from a stored class"""
    def __init__(self):
        self.url = '../data/credit_unions.csv'
        self.hostName = 'noahcook_db_1'
        self.databaseName = 'irs990'
        self.username = 'postgres'
        self.password = 'postgres1234'
        self.port = '5432'
        self.eins = ''
        self.pandasBool = False
        self.filterBool = True

    def __str__(self):
        """Function to get Variables from a stored class as a string"""
        return 'No string for you!'

    def setHostName(self, hostName):
        assert isinstance(hostName, str)
        self.hostName = hostName
        
    def setDatabaseName(self, dbName):
        assert isinstance(dbName, str)
        self.databaseName = dbName
        
    def setUsername(self, username):
        assert isinstance(username, str)
        self.username = username
        
    def setPassword(self, password):
        assert isinstance(password, str)
        self.password = password
        
    def setURL(self, url):
        assert isinstance(url, str)
        self.url = url
        
    def setPandasBool(self, pandasBool):
        assert isinstance(pandasBool, bool)
        self.pandasBool = pandasBool

    def setFilterBool(self, filterBool):
        assert isinstance(filterBool, bool)
        self.filterBool = filterBool
        
    
    # Add spark port if necessary for later.
        


variables = Vars()


def getSparkTable(tableName, hostName='', databaseName='', username='', password='', port='', eins='', pandasBool=None, filterBool=None):
    """Pulls given table from givem database."""
    hostName, databaseName, username, password, port, pandasBool, filterBool = \
        getDefaults(hostName, databaseName, username, password, port, pandasBool, filterBool)

    
    connection = makeConnection(hostName, databaseName, username, password, port)

    spark = makeSession()

    table = pullTable(spark, tableName, hostName, databaseName, username, password, port)

    filtered_table = filterTable(table, eins)

    if filterBool:
        if pandasBool:
            return filtered_table.toPandas()
        return filtered_table
            
    elif pandasBool:
        return table.toPandas()
    return table


def makeConnection(hostName='', databaseName='', username='', password='', port=''):
    """Makes a connections to the given database.  Requires name of db docker container name."""
    hostName, databaseName, username, password, port = \
        getDefaults(hostName, databaseName, username, password, port)[0:5]
    
    # Actually making the connection
    connection = psycopg2.connect(
        host = hostName, # Name of database docker container
        database = databaseName, # Name of database
        user = username, # Username for our database
        password = password, # Password for our database  (Currently this function works without this line, unsure if this is because password is saved)
        port = port # Port to be used for our connection
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
    .set('spark.jars', '/home/jovyan/scratch/postgresql-42.2.18.jar')) # Similar to a dll, its a type of additional java stuff
    
    # Create the session
    spark = SparkSession.builder \
    .appName('test') \
    .config(conf=conf) \
    .getOrCreate()
    
    return spark
    

def pullTable(spark, tableName, hostName='', databaseName='', username='', password='', port=''):
    """Retrieves table from irs990 database.  Needs table name and db docker container name."""
    hostName, databaseName, username, password, port = \
        getDefaults(hostName, databaseName, username, password, port)[0:5]
    
    # Creating the properties for the table that we are going to pull from our database
    table_properties = {
    'driver': 'org.postgresql.Driver',  # should match the type of sql that our db uses I think
    'url': 'jdbc:postgresql://' + hostName + ':' + port + '/' + databaseName, # Location of where our database is
    'user': username, # Username for our database
    'password': password, # Password for our database  (Currently this function works without this line, unsure if this is because password is saved)
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

def getDefaults(hostName, databaseName, username, password, port, pandasBool=None, filterBool=None):
    if hostName == '':
        hostName = variables.hostName
    if databaseName == '':
        databaseName = variables.databaseName
    if username == '':
        username = variables.username
    if password == '':
        password = variables.password
    if type(port) == int:
        port = str(port)
    elif port == '':
        port = variables.port
    if pandasBool is None:
        pandasBool = variables.pandasBool
    if filterBool is None:
        filterBool = variables.filterBool
        
    return hostName, databaseName, username, password, port, pandasBool, filterBool
    