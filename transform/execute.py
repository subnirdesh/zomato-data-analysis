import sys,os,time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))
from utility.utility import setup_logging,format_time



def create_spark_session():
    return (SparkSession.builder
            .appName("ZomatoDataTransform")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "4g")
            .getOrCreate()
    )

def load_and_clean(spark, input_dir, output_dir):
    """Stage 1: Load and Clean Data for Zomato Dataset"""
    
    # Defining schemas
    food_schema = T.StructType([
        T.StructField("serial_no", T.IntegerType(), False),
        T.StructField("f_id", T.StringType(), False),
        T.StructField("item", T.StringType(), True), 
        T.StructField("veg_or_non_veg", T.StringType(), True),
    ])

    restaurant_schema = T.StructType([
        T.StructField("serial_no", T.IntegerType(), False),
        T.StructField("id", T.IntegerType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
        T.StructField("rating", T.StringType(), True),
        T.StructField("rating_count", T.StringType(), True),
        T.StructField("cost", T.StringType(), True),
        T.StructField("cuisine", T.StringType(), True),
        T.StructField("lic_no", T.StringType(), True),   
        T.StructField("link", T.StringType(), True), 
        T.StructField("address", T.StringType(), True), 
        T.StructField("menu", T.StringType(), True),  
    ])

    menu_schema = T.StructType([
         T.StructField("serial_no", T.IntegerType(), False),
        T.StructField("menu_id", T.StringType(), False),
        T.StructField("r_id", T.IntegerType(), False),
        T.StructField("f_id", T.StringType(), False),
        T.StructField("cuisine", T.StringType(), True),
        T.StructField("price", T.DoubleType(), True),    
    ])

    users_schema = T.StructType([
        T.StructField("serial_no", T.IntegerType(), False),
        T.StructField("user_id", T.IntegerType(), False),
        T.StructField("name", T.StringType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("password", T.StringType(), True),
        T.StructField("Age", T.IntegerType(), True),
        T.StructField("Gender", T.StringType(), True),
        T.StructField("Marital Status", T.StringType(), True),
        T.StructField("Occupation", T.StringType(), True),   
        T.StructField("Monthly Income", T.StringType(), True), 
        T.StructField("Educational Qualifications", T.StringType(), True), 
        T.StructField("Family Size", T.IntegerType(), True),            
    ])

    orders_schema = T.StructType([
        T.StructField("serial_no", T.IntegerType(), False),
        T.StructField("order_date", T.StringType(), False),
        T.StructField("sales_qty", T.IntegerType(), False),
        T.StructField("sales_amount", T.IntegerType(), False),
        T.StructField("currency", T.StringType(), True),
        T.StructField("user_id", T.IntegerType(), False),
        T.StructField("r_id", T.IntegerType(), False),    
    ])
    
    # Loading data from CSV files
    food_df = spark.read.schema(food_schema).csv(
        os.path.join(input_dir, "food.csv"), header=True)
    food_df = food_df.drop(food_df.columns[0])

    
    restaurant_df = spark.read.schema(restaurant_schema).csv(
        os.path.join(input_dir, "restaurant.csv"), header=True)
    restaurant_df = restaurant_df.drop(restaurant_df.columns[0])
    
    menu_df = spark.read.schema(menu_schema).csv(
        os.path.join(input_dir, "menu.csv"), header=True)
    menu_df = menu_df.drop(menu_df.columns[0])
    
    users_df = spark.read.schema(users_schema).csv(
        os.path.join(input_dir, "users.csv"), header=True)
    users_df = users_df.drop(users_df.columns[0])

    
    orders_df = spark.read.schema(orders_schema).csv(
        os.path.join(input_dir, "orders.csv"), header=True)
    orders_df = orders_df.drop(orders_df.columns[0])
    
    # Cleaning food data
    food_df = food_df.dropDuplicates(["f_id"]).filter(F.col("f_id").isNotNull())
    food_df = food_df.withColumn(
        "veg_or_non_veg", 
        F.upper(F.trim(F.col("veg_or_non_veg")))
    ).withColumn(
        "item", 
        F.trim(F.col("item"))
    )
    
    # Cleaning restaurant data
    restaurant_df = restaurant_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
    # Cleaning numeric columns stored as strings
    restaurant_df = restaurant_df.withColumn(
        "rating",
        F.regexp_replace("rating", "[^0-9.]", "").cast(T.FloatType())
    ).withColumn(
        "rating_count",
        F.regexp_replace("rating_count", "[^0-9]", "").cast(T.IntegerType())
    ).withColumn(
        "cost",
        F.regexp_replace("cost", "[^0-9]", "").cast(T.IntegerType())
    ).withColumn(
        "name",
        F.trim(F.col("name"))
    ).withColumn(
        "city",
        F.trim(F.col("city"))
    ).withColumn(
        "cuisine",
        F.trim(F.col("cuisine"))
    )
    
    # Cleaning menu data
    menu_df = menu_df.dropDuplicates(["menu_id"]).filter(
        F.col("menu_id").isNotNull() & 
        F.col("r_id").isNotNull() & 
        F.col("f_id").isNotNull()
    )
    menu_df = menu_df.withColumn(
    "price",
    F.when(F.col("price").isNull() | (F.col("price") == ""), 0.0)
     .otherwise(
         F.regexp_replace(F.col("price"), r"[^\d.]", "").cast("double")
     )
    ).withColumn(
    "cuisine",
    F.trim(F.col("cuisine"))
    )
    
    # Cleaning users data
    users_df = users_df.dropDuplicates(["user_id"]).filter(F.col("user_id").isNotNull())
    # Cleaning Monthly Income column (remove currency symbols, commas)
    users_df = users_df.withColumn(
        "Monthly Income",
        F.regexp_replace("Monthly Income", "[^0-9]", "").cast(T.IntegerType())
    ).withColumn(
        "name",
        F.trim(F.col("name"))
    ).withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    ).withColumn(
        "Gender",
        F.upper(F.trim(F.col("Gender")))
    ).withColumn(
        "Marital Status",
        F.trim(F.col("Marital Status"))
    ).withColumn(
        "Occupation",
        F.trim(F.col("Occupation"))
    )
    
    orders_df = orders_df.withColumn(
        "sales_qty", 
        F.when(F.col("sales_qty").isNull() | (F.col("sales_qty") < 0), 1)
         .otherwise(F.col("sales_qty").cast(T.IntegerType()))
    ).withColumn(
        "sales_amount", 
        F.when(F.col("sales_amount").isNull() | (F.col("sales_amount") < 0), 0)
         .otherwise(F.col("sales_amount").cast(T.IntegerType()))
    ).withColumn(
        "order_date", 
        F.when(
            F.col("order_date").isNull() | (F.trim(F.col("order_date")) == ""),
            F.current_date()
        ).otherwise(
            F.to_date(F.col("order_date"), "yyyy-MM-dd")
        )
    ).withColumn(
        "currency",
        F.when(
            F.col("currency").isNull() | (F.trim(F.col("currency")) == ""),
            "INR"
        ).otherwise(F.upper(F.trim(F.col("currency"))))
    )

    # Writing cleaned data to parquet files
    food_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage1", "food")
    )
    
    restaurant_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage1", "restaurant")
    )
    
    menu_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage1", "menu")
    )
    
    users_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage1", "users")
    )
    
    orders_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage1", "orders")
    )
    
    logger.info("Stage 1: All Zomato data loaded and cleaned successfully.")
    return food_df, restaurant_df, menu_df, users_df, orders_df




from pyspark.sql import functions as F
import os

def create_master_table(food_df, restaurant_df, menu_df, users_df, orders_df, output_dir):
    """Stage 2: Create Master Tables by joining all Zomato data"""

    logger.info("=== DEBUGGING JOINS ===")
    logger.info(f"Orders count before join: {orders_df.count()}")
    logger.info(f"Users count before join: {users_df.count()}")
    logger.info(f"Menu count before join: {menu_df.count()}")
    logger.info(f"Restaurant count before join: {restaurant_df.count()}")

    # Check distinct keys
    logger.info(f"Distinct user_ids in orders: {orders_df.select('user_id').distinct().count()}")
    logger.info(f"Distinct user_ids in users: {users_df.select('user_id').distinct().count()}")
    logger.info(f"Distinct r_ids in orders: {orders_df.select('r_id').distinct().count()}")
    logger.info(f"Distinct r_ids in restaurants: {restaurant_df.select('id').distinct().count()}")

    # 1. Build Menu-Food-Restaurant master for reference/analytics
    menu_food_df = menu_df.join(
        food_df,
        menu_df.f_id == food_df.f_id,
        "inner"
    ).select(
        menu_df.menu_id,
        menu_df.r_id,
        menu_df.f_id,
        menu_df.cuisine.alias("menu_cuisine"),
        menu_df.price,
        food_df.item,
        food_df.veg_or_non_veg
    )

    menu_food_restaurant_df = menu_food_df.join(
        restaurant_df,
        menu_food_df.r_id == restaurant_df.id,
        "inner"
    ).select(
        menu_food_df.menu_id,
        menu_food_df.r_id,
        menu_food_df.f_id,
        menu_food_df.menu_cuisine,
        menu_food_df.price,
        menu_food_df.item,
        menu_food_df.veg_or_non_veg,
        restaurant_df.name.alias("restaurant_name"),
        restaurant_df.city,
        restaurant_df.rating,
        restaurant_df.rating_count,
        restaurant_df.cost.alias("restaurant_avg_cost"),
        restaurant_df.cuisine.alias("restaurant_cuisine"),
        restaurant_df.lic_no,
        restaurant_df.link
    )

    logger.info(f"Menu-Food-Restaurant joined rows: {menu_food_restaurant_df.count()}")

    # 2. Orders ↔ Users
    orders_users_df = orders_df.join(
        users_df,
        orders_df.user_id == users_df.user_id,
        "left"
    ).select(
        orders_df.order_date,
        orders_df.sales_qty,
        orders_df.sales_amount,
        orders_df.currency,
        orders_df.user_id,
        orders_df.r_id,
        F.coalesce(users_df.name, F.lit("Unknown User")).alias("user_name"),
        F.coalesce(users_df.email, F.lit("unknown@example.com")).alias("email"),
        F.coalesce(users_df.Age, F.lit(0)).alias("Age"),
        F.coalesce(users_df.Gender, F.lit("UNKNOWN")).alias("Gender"),
        F.coalesce(F.col("Marital Status"), F.lit("Unknown")).alias("marital_status"),
        F.coalesce(users_df.Occupation, F.lit("Unknown")).alias("Occupation"),
        F.coalesce(F.col("Monthly Income"), F.lit(0)).alias("monthly_income")
    )

    logger.info(f"Orders-Users joined rows: {orders_users_df.count()}")

    # 3. Orders ↔ Restaurants (since menu_id is missing in orders)
    final_master_df = orders_users_df.join(
        restaurant_df,
        orders_users_df.r_id == restaurant_df.id,
        "left"
    ).select(
        # Order info
        orders_users_df.order_date,
        orders_users_df.sales_qty,
        orders_users_df.sales_amount,
        orders_users_df.currency,
        # User info
        orders_users_df.user_id,
        orders_users_df.user_name,
        orders_users_df.email,
        orders_users_df.Age,
        orders_users_df.Gender,
        orders_users_df.marital_status,
        orders_users_df.Occupation,
        orders_users_df.monthly_income,
        # Restaurant info
        orders_users_df.r_id,
        F.coalesce(restaurant_df.name, F.lit("Unknown Restaurant")).alias("restaurant_name"),
        F.coalesce(restaurant_df.city, F.lit("Unknown City")).alias("city"),
        F.coalesce(restaurant_df.rating, F.lit(0.0)).alias("rating"),
        F.coalesce(restaurant_df.rating_count, F.lit(0)).alias("rating_count"),
        F.coalesce(restaurant_df.cost, F.lit(0)).alias("restaurant_avg_cost"),
        F.coalesce(restaurant_df.cuisine, F.lit("Unknown")).alias("restaurant_cuisine"),
        restaurant_df.lic_no,
        restaurant_df.link
    )

    logger.info(f"Final master table rows: {final_master_df.count()}")

    # Save all master tables
    menu_food_restaurant_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage2", "menu_restaurant_master")
    )

    orders_users_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage2", "orders_users_master")
    )

    final_master_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage2", "complete_master_table")
    )

    logger.info("Stage 2: All master tables created and saved")
    return final_master_df
    
def create_query_table(output_dir, food_df, restaurant_df, menu_df, users_df, orders_df):
    # 1. Restaurant-Menu mapping for quick lookups
    restaurant_menu_df = menu_df.select("r_id", "menu_id", "f_id", "price") \
                                .groupBy("r_id") \
                                .agg(F.collect_list("menu_id").alias("menu_ids"),
                                     F.avg("price").alias("avg_menu_price"),
                                     F.count("menu_id").alias("total_menu_items"))
    
    restaurant_menu_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage3", "restaurant_menu_summary")
    )
    
    # 2. Food metadata for quick searches
    food_metadata_df = food_df.select("f_id", "item", "veg_or_non_veg")
    food_metadata_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage3", "food_metadata")
    )
    
    # 3. Restaurant metadata with aggregated info
    restaurant_metadata_df = restaurant_df.select(
        "id", "name", "city", "rating", "rating_count", 
        "cost", "cuisine", "lic_no"
    )
    restaurant_metadata_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage3", "restaurant_metadata")
    )
    
    # 4. User purchase behavior summary (with LEFT join)
    user_behavior_df = orders_df.join(users_df, "user_id", "left") \
                                .groupBy("user_id", 
                                        F.coalesce(users_df.name, F.lit("Unknown")).alias("name"),
                                        F.coalesce(users_df.Age, F.lit(0)).alias("Age"),
                                        F.coalesce(users_df.Gender, F.lit("UNKNOWN")).alias("Gender"),
                                        F.coalesce(users_df.Occupation, F.lit("Unknown")).alias("Occupation")) \
                                .agg(F.sum("sales_amount").alias("total_spent"),
                                     F.sum("sales_qty").alias("total_orders"),
                                     F.avg("sales_amount").alias("avg_order_value"),
                                     F.countDistinct("r_id").alias("unique_restaurants"),
                                     F.max("order_date").alias("last_order_date"),
                                     F.min("order_date").alias("first_order_date"))
    
    user_behavior_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage3", "user_behavior_summary")
    )
    
    
    # 6. City-wise restaurant summary
    city_summary_df = restaurant_df.groupBy("city") \
                                   .agg(F.count("id").alias("total_restaurants"),
                                        F.avg("rating").alias("avg_city_rating"),
                                        F.avg("cost").alias("avg_city_cost"),
                                        F.collect_set("cuisine").alias("available_cuisines"))
    
    city_summary_df.write.mode("overwrite").parquet(
        os.path.join(output_dir, "stage3", "city_restaurant_summary")
    )
    
    logger.info("Stage 3: All query-optimized tables created and saved")




if __name__ == "__main__":
    logger = setup_logging("transform.log")
    start = time.time()

    if len(sys.argv) != 3:
        logger.error("Usage: python script.py <input_dir> <output_dir>")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]

    spark = create_spark_session()

    # Stage 1: Load and clean data
    food_df, restaurant_df, menu_df, users_df, orders_df = load_and_clean(spark, input_dir, output_dir)
    
    #  DEBUG Code
    logger.info("=== DEBUGGING BEFORE MASTER TABLE CREATION ===")
    logger.debug(f"Food count: {food_df.count()}")
    logger.debug(f"Restaurant count: {restaurant_df.count()}")
    logger.debug(f"Menu count: {menu_df.count()}")
    logger.debug(f"Users count: {users_df.count()}")
    logger.debug(f"Orders count: {orders_df.count()}")
    
    # Showing sample data for debugging
    logger.debug("Sample menu data:")
    menu_df.select("menu_id", "r_id", "f_id", "price").show(5, truncate=False)
    
    logger.debug("Sample orders data:")
    orders_df.select("user_id", "r_id", "sales_amount").show(5, truncate=False)
    
    # Stage 2: Create master tables
    create_master_table(food_df, restaurant_df, menu_df, users_df, orders_df, output_dir)
    
    # Stage 3: Create query-optimized tables
    create_query_table(output_dir, food_df, restaurant_df, menu_df, users_df, orders_df)

    end = time.time()
    logger.info("Transformation pipeline completed")
    logger.info(f"Total time taken: {format_time(end-start)}")









