import sys,os,time
import psycopg2
from psycopg2 import sql
from pyspark.sql import SparkSession
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from utility.utility import setup_logging,format_time

def create_spark_session():
    """Initialize Spark session with PostgreSQL JDBC driver."""
    return SparkSession.builder \
        .appName("ZomatoDataLoad") \
        .getOrCreate()

def create_postgres_tables(database,host_name,port_no,pg_un, pg_pw,):
    """Create PostgreSQL tables if they don't exist using psycopg2."""
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=database,
            user=pg_un,
            password=pg_pw,
            host=host_name,
            port=port_no
        )
        cursor = conn.cursor()

        create_table_queries = [
        """
        CREATE TABLE IF NOT EXISTS complete_master_table (
            order_date DATE,
            sales_qty INTEGER,
            sales_amount INTEGER,
            currency VARCHAR(10),
            user_id INTEGER,
            user_name TEXT,
            email TEXT,
            age INTEGER,
            gender VARCHAR(20),
            marital_status TEXT,
            occupation TEXT,
            monthly_income INTEGER,
            r_id INTEGER,
            restaurant_name TEXT,
            city TEXT,
            rating FLOAT,
            rating_count INTEGER,
            restaurant_avg_cost INTEGER,
            restaurant_cuisine TEXT,
            lic_no TEXT,
            link TEXT,
            menu_id VARCHAR(50),
            f_id VARCHAR(50),
            menu_cuisine TEXT,
            price INTEGER,
            item TEXT,
            veg_or_non_veg VARCHAR(20)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS menu_restaurant_master (
            menu_id VARCHAR(50),
            r_id INTEGER,
            f_id VARCHAR(50),
            menu_cuisine TEXT,
            price INTEGER,
            item TEXT,
            veg_or_non_veg VARCHAR(20),
            restaurant_name TEXT,
            city TEXT,
            rating FLOAT,
            rating_count INTEGER,
            restaurant_avg_cost INTEGER,
            restaurant_cuisine TEXT,
            lic_no TEXT,
            link TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS orders_users_master (
            order_date DATE,
            sales_qty INTEGER,
            sales_amount INTEGER,
            currency VARCHAR(10),
            user_id INTEGER,
            r_id INTEGER,
            user_name TEXT,
            email TEXT,
            age INTEGER,
            gender VARCHAR(20),
            marital_status TEXT,
            occupation TEXT,
            monthly_income INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS food_metadata (
            f_id VARCHAR(50) PRIMARY KEY,
            item TEXT,
            veg_or_non_veg VARCHAR(20)
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS restaurant_metadata (
            id INTEGER PRIMARY KEY,
            name TEXT,
            city TEXT,
            rating FLOAT,
            rating_count INTEGER,
            cost INTEGER,
            cuisine TEXT,
            lic_no TEXT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS restaurant_menu_summary (
            r_id INTEGER PRIMARY KEY,
            menu_ids TEXT[],
            avg_menu_price FLOAT,
            total_menu_items BIGINT
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS city_restaurant_summary (
            city TEXT PRIMARY KEY,
            total_restaurants BIGINT,
            avg_city_rating FLOAT,
            avg_city_cost FLOAT,
            available_cuisines TEXT[]
        );
        """
        ]
        
        for query in create_table_queries:
            cursor.execute(query)
            conn.commit()
        
        logger.info("PostgreSQL tables created successfully")

    except Exception as e:
        logger.warning(f"Error creating tables: {e}")
    finally:
        logger.debug("Closing Connection and cursor to postgres db")
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def load_to_postgres(spark, input_dir,pg_un,pg_pw):
    """Load Parquet files to PostgreSQL."""
    jdbc_url = "jdbc:postgresql://localhost:5432/postgres"
    connection_properties = {
        "user": pg_un,
        "password": pg_pw,
        "driver": "org.postgresql.Driver"
    }

    tables = [
        # Stage 2 Master Tables
        ("stage2/complete_master_table", "complete_master_table"),
        ("stage2/menu_restaurant_master", "menu_restaurant_master"),
        ("stage2/orders_users_master", "orders_users_master"),
        
        # Stage 3 Query-Optimized Tables
        ("stage3/food_metadata", "food_metadata"),
        ("stage3/restaurant_metadata", "restaurant_metadata"),
        ("stage3/restaurant_menu_summary", "restaurant_menu_summary"),
        ("stage3/user_behavior_summary", "user_behavior_summary"),
        ("stage3/restaurant_performance", "restaurant_performance"),
        ("stage3/city_restaurant_summary", "city_restaurant_summary"),
        ("stage3/menu_price_analysis", "menu_price_analysis")
    ]

    for parquet_path, table_name in tables:
        try:
            df = spark.read.parquet(os.path.join(input_dir, parquet_path))
            
            # Use append mode for master tables to allow incremental loads
            mode = "append" if 'master' in table_name else "overwrite"
            
            df.write \
              .mode(mode) \
              .jdbc(url=jdbc_url, table=table_name, properties=connection_properties)
            
            logger.info(f"Loaded {table_name} to PostgreSQL ({df.count()} rows)")
            
        except Exception as e:
            logger.warning(f"Error loading {table_name}: {e}")


if __name__ == "__main__":

    logger=setup_logging("load.log")
    start=time.time()

    
    if len(sys.argv) != 7:
        logger.error("Usage: python load/execute.py <input_dir> <database> <host> <port> <pg_un> <pg_pw>")
        sys.exit(1)

    input_dir = sys.argv[1]
    db_name=sys.argv[2]
    host_name=sys.argv[3]
    port=sys.argv[4]
    pg_un=sys.argv[5]
    pg_pw=sys.argv[6]

    if not os.path.exists(input_dir):
        logger.error(f"Error: Input directory {input_dir} does not exist")
        sys.exit(1)

    spark = create_spark_session()
    create_postgres_tables(db_name,host_name,port,pg_un,pg_pw)
    load_to_postgres(spark, input_dir,pg_un,pg_pw)

    end=time.time()
    logger.info("Zomato data load stage completed")
    logger.info(f"Total time taken: {format_time(end-start)}")