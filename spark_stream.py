from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField
import logging

def create_keyspace(session):

    session.execute('''
    CREATE TABLE IF NOT EXISTS spark_streams
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    ''')

    print('Keyspace created successfully')


def create_table(session):

    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created users(
        id uuid PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        dob TEXT,
        address TEXT,
        postcode TEXT,
        email TEXT,
        photo TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT);
    """)


    print('Table created successfully')


def insert_data(session, **kwargs):
    print('Inserting data...')

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    username = kwargs.get('username')
    email = kwargs.get('email')
    address = kwargs.get('address')
    postcode = kwargs.get('postcode')
    dob = kwargs.get('dob')
    gender = kwargs.get('gender')
    phone = kwargs.get('phone')
    photo = kwargs.get('photo')
    registered_date = kwargs.get('registered_date')

    try:
        session.execute("""
        INSERT INFO spark_streams.created_users(id, first_name, last_name, username, gender, dob, email, address,
            postcode, phone, registered_date, photo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, first_name, last_name, username, gender, dob, email, address, postcode,
                  phone, registered_date, photo))
        
        logging.info(f'Data inserted for {first_name} {last_name}!')
    
    except Exception as e:
        logging.error(f'Data could not be inserted due to: {e}')


def create_spark_connection():
    s_conn = None

    try:
        s_conn = SparkSession.builder \
                .appName('SparkDataStreaming') \
                .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
                .config('spark.cassandra.connection.host', 'localhost') \
                .getOrCreate()
        
        s_conn.SparkContext.setLogLevel('ERROR')
        logging.info('Spark connection established successfully!')
    
    except Exception as e:
        logging.error(f"Couldn't establish spark connection due to: {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'localhost:9092') \
                .option('subsribe', 'users_created') \
                .option('startingOffsets', 'earliest') \
                .load()
        
        logging.info('Kafka dataframe created successfully')
    
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created due to: {e}")

    return spark_df


def create_selection_df_from_kafka(spark_df):
   
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("dob", StringType(), False),
        StructField("address", StringType(), False),
        StructField("postcode", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("photo", StringType(), False)
    ])
    sel = spark_df.selectExpr("CAST(value as STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()

        return cas_session
    
    except Exception as e:
        logging.error(f'Could not connect to cassandra session due to: {e}')
        return None


if __name__ == '__main__':
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session:
            create_keyspace(session)
            create_table(session)
            logging.info('streaming is starting up...')

            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra") \
                               .option('checkpointLocation', '/tmp/checkpoint') \
                               .option('keyspace', 'spark_streams') \
                               .option('table', 'created_users') \
                               .start())
            streaming_query.awaitTermination()