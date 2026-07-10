from pyspark.sql import functions as F
from pyspark.sql.types import *

rentals = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "s3a://berlin-realestate-data/raw/rentals.csv"
    )
)

display(rentals)

secondary_sales = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "s3a://berlin-realestate-data/raw/secondary_sales.csv"
    )
)

new_construction = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "s3a://berlin-realestate-data/raw/new_construction.csv"
    )
)

kiez_prices = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "s3a://berlin-realestate-data/raw/kiez_prices_monthly.csv"
    )
)

transit = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(
        "s3a://berlin-realestate-data/raw/transit_stations.csv"
    )
)

silver_rentals = (
    rentals
    .dropDuplicates()
    .withColumn(
        "bezirk",
        F.initcap(F.trim("bezirk"))
    )
    .withColumn(
        "ortsteil",
        F.initcap(F.trim("ortsteil"))
    )
    .filter(
        (F.col("lat").between(52,53)) &
        (F.col("lon").between(13,14))
    )
)

silver_rentals.write \
.format("delta") \
.mode("overwrite") \
.save(
"s3a://berlin-realestate-data/silver/silver_rentals"
)
silver_rentals.createOrReplaceTempView("silver_rentals")

silver_secondary_sales = (
    secondary_sales
    .dropDuplicates()
    .withColumn(
        "bezirk",
        F.initcap(F.trim("bezirk"))
    )
    .withColumn(
        "ortsteil",
        F.initcap(F.trim("ortsteil"))
    )
    .filter(
        (F.col("lat").between(52,53)) &
        (F.col("lon").between(13,14))
    )
)


silver_secondary_sales.write \
.format("delta") \
.mode("overwrite") \
.save(
"s3a://berlin-realestate-data/silver/silver_secondary_sales"
)
silver_secondary_sales.createOrReplaceTempView("silver_secondary_sales")

silver_new_construction = (
    new_construction
    .dropDuplicates()
    .withColumn(
        "bezirk",
        F.initcap(F.trim("bezirk"))
    )
    .withColumn(
        "ortsteil",
        F.initcap(F.trim("ortsteil"))
    )
    .filter(
        (F.col("lat").between(52,53)) &
        (F.col("lon").between(13,14))
    )
)


silver_new_construction.write \
.format("delta") \
.mode("overwrite") \
.save(
"s3a://berlin-realestate-data/silver/silver_new_construction"
)
silver_new_construction.createOrReplaceTempView("silver_new_construction")

rental_segment = silver_rentals.select(
"id",
"bezirk",
"rent_per_m2_kalt_eur"
)

rental_segment = (
rental_segment
.withColumn(
"segment",
F.lit("Rental")
)
.withColumn(
"value_per_m2",
F.col("rent_per_m2_kalt_eur")
)
)


sales_segment = silver_secondary_sales.select(
"id",
"bezirk",
"price_per_m2_eur"
)

sales_segment = (
sales_segment
.withColumn(
"segment",
F.lit("Secondary Sale")
)
.withColumn(
"value_per_m2",
F.col("price_per_m2_eur")
)
)


new_segment = silver_new_construction.select(
"id",
"bezirk",
"price_per_m2_eur"
)

new_segment = (
new_segment
.withColumn(
"segment",
F.lit("New Construction")
)
.withColumn(
"value_per_m2",
F.col("price_per_m2_eur")
)
)


portfolio_summary = (
rental_segment
.unionByName(
sales_segment,
allowMissingColumns=True
)
.unionByName(
new_segment,
allowMissingColumns=True
)
)

portfolio_summary.write \
.format("delta") \
.mode("overwrite") \
.save(
"s3a://berlin-realestate-data/gold/portfolio_summary"
)
rental_segment.createOrReplaceTempView("rental_segment")

portfolio_kpis = (
portfolio_summary
.groupBy("segment")
.agg(
F.count("value_per_m2")
.alias("listings"),

F.avg("value_per_m2")
.alias("avg_value_per_m2")
)
)
portfolio_kpis.write \
.format("delta") \
.mode("overwrite") \
.save(
"s3a://berlin-realestate-data/gold/portfolio_kpis"
)
portfolio_kpis.createOrReplaceTempView("portfolio_kpis")

market_trends = (
kiez_prices
.groupBy("year_month")
.agg(
F.avg(
"secondary_price_per_m2_eur"
)
.alias("secondary_price"),

F.avg(
"new_construction_price_per_m2_eur"
)
.alias("newbuild_price"),

F.avg(
"kaltmiete_per_m2_monthly_eur"
)
.alias("rent_price"),

F.avg(
"avg_mortgage_rate_pct"
)
.alias("mortgage_rate")
)
.orderBy("year_month")
)

market_trends.write \
.format("delta") \
.mode("overwrite") \
.save(
"s3a://berlin-realestate-data/gold/market_trends"
)
market_trends.createOrReplaceTempView("market_trends")

sales_locations = (
silver_secondary_sales
.select(
"lat",
"lon",
"bezirk",
"price_per_m2_eur",
"transit_distance_min"
)
.withColumn(
"segment",
F.lit("Secondary Sale")
)
)


new_locations = (
silver_new_construction
.select(
"lat",
"lon",
"bezirk",
"price_per_m2_eur",
"transit_distance_min"
)
.withColumn(
"segment",
F.lit("New Construction")
)
)


property_locations = (
sales_locations
.unionByName(new_locations)
)

property_locations = property_locations.withColumn(
"transit_bucket",

F.when(
F.col("transit_distance_min")<=5,
"0-5 min"
)

.when(
F.col("transit_distance_min")<=10,
"5-10 min"
)

.when(
F.col("transit_distance_min")<=15,
"10-15 min"
)

.otherwise("15+ min")
)

property_locations.write \
.format("delta") \
.mode("overwrite") \
.save(
"s3a://berlin-realestate-data/gold/property_locations"
)
property_locations.createOrReplaceTempView("property_locations")
