import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def spark_app(ds, table_in, table_out, bucket_name):
    TASK_NAME = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

    spark = SparkSession.builder.appName(TASK_NAME).getOrCreate()

    run_ds = ds  # '20250912' (строка вида yyyyMMdd)
    ds_date = F.to_date(F.lit(run_ds), "yyyyMMdd")  # -> колонка-дата

    # Читаем данные из ClickHouse через catalog
    df = spark.read.table(f"clickhouse.{table_in}").withColumn(
        "purchase_date", F.to_date("purchase_datetime")
    )

    customer_stats = df.groupBy("customer_id").agg(
        F.count("*").alias("total_purchases"),
        F.avg("total_amount").alias("avg_cart_value"),
        F.max("total_amount").alias("max_cart_value"),
        F.min("total_amount").alias("min_cart_value"),
        F.countDistinct("store_city").alias("unique_cities"),
        F.countDistinct("store_id").alias("unique_stores"),
        F.countDistinct(F.col("purchase_date")).alias("unique_purchase_days"),
        F.sum(F.when(F.col("is_delivery") == 1, 1).otherwise(0)).alias(
            "delivery_purchases"
        ),
        F.sum(F.when(F.col("payment_method") == "cash", 1).otherwise(0)).alias(
            "cash_purchases"
        ),
        F.sum(F.when(F.col("payment_method") == "card", 1).otherwise(0)).alias(
            "card_purchases"
        ),
        F.sum(
            F.when(F.dayofweek("purchase_datetime").isin([1, 7]), 1).otherwise(0)
        ).alias("weekend_purchases"),
        F.sum(
            F.when(F.dayofweek("purchase_datetime").between(2, 6), 1).otherwise(0)
        ).alias("weekday_purchases"),
        F.sum(F.when(F.hour("purchase_datetime") >= 20, 1).otherwise(0)).alias(
            "night_purchases"
        ),
        F.sum(F.when(F.hour("purchase_datetime") < 10, 1).otherwise(0)).alias(
            "morning_purchases"
        ),
        F.sum(
            F.when(F.hour("purchase_datetime").between(12, 15), 1).otherwise(0)
        ).alias("early_bird_purchases"),
        F.sum(
            F.when(F.col("purchase_date") >= F.date_sub(ds_date, 7), 1).otherwise(0)
        ).alias("recent_7d_purchases"),
        F.sum(
            F.when(F.col("purchase_date") >= F.date_sub(ds_date, 14), 1).otherwise(0)
        ).alias("recent_14d_purchases"),
        F.sum(
            F.when(F.col("purchase_date") >= F.date_sub(ds_date, 30), 1).otherwise(0)
        ).alias("recent_30d_purchases"),
        F.sum(
            F.when(F.col("purchase_date") >= F.date_sub(ds_date, 90), 1).otherwise(0)
        ).alias("recent_90d_purchases"),
        F.max("purchase_datetime").alias("last_purchase_date"),
        F.min("purchase_datetime").alias("first_purchase_date"),
    )

    category_stats = df.groupBy("customer_id").agg(
        F.sum(
            F.when(F.col("items").rlike(".*Молочные продукты.*"), 1).otherwise(0)
        ).alias("milk_purchases"),
        F.sum(F.when(F.col("items").rlike(".*Фрукты и ягоды.*"), 1).otherwise(0)).alias(
            "fruits_purchases"
        ),
        F.sum(F.when(F.col("items").rlike(".*Овощи и зелень.*"), 1).otherwise(0)).alias(
            "veggies_purchases"
        ),
        F.sum(
            F.when(
                F.col("items").rlike(".*Зерновые и хлебобулочные изделия.*"), 1
            ).otherwise(0)
        ).alias("bakery_purchases"),
        F.sum(
            F.when(F.col("items").rlike(".*Мясо, рыба, яйца и бобовые.*"), 1).otherwise(
                0
            )
        ).alias("meat_purchases"),
        F.sum(F.when(F.col("items").rlike(".*organic.*"), 1).otherwise(0)).alias(
            "organic_purchases"
        ),
    )

    recent_category_stats = (
        df.filter(
            F.col("purchase_datetime")
            >= F.current_timestamp() - F.expr("INTERVAL 30 DAYS")
        )
        .groupBy("customer_id")
        .agg(
            F.sum(
                F.when(F.col("items").rlike(".*Молочные продукты.*"), 1).otherwise(0)
            ).alias("milk_purchases_30d"),
            F.sum(
                F.when(F.col("items").rlike(".*Фрукты и ягоды.*"), 1).otherwise(0)
            ).alias("fruits_purchases_30d"),
        )
    )

    recent_14d_stats = (
        df.filter(
            F.col("purchase_datetime")
            >= F.current_timestamp() - F.expr("INTERVAL 14 DAYS")
        )
        .groupBy("customer_id")
        .agg(
            F.sum(
                F.when(F.col("items").rlike(".*Фрукты и ягоды.*"), 1).otherwise(0)
            ).alias("fruits_purchases_14d"),
            F.sum(
                F.when(F.col("items").rlike(".*Овощи и зелень.*"), 1).otherwise(0)
            ).alias("veggies_purchases_14d"),
        )
    )

    recent_7d_stats = (
        df.filter(
            F.col("purchase_datetime")
            >= F.current_timestamp() - F.expr("INTERVAL 7 DAYS")
        )
        .groupBy("customer_id")
        .agg(
            F.sum(
                F.when(
                    F.col("items").rlike(".*Мясо, рыба, яйца и бобовые.*"), 1
                ).otherwise(0)
            ).alias("meat_purchases_7d"),
            F.sum("total_amount").alias("total_spent_7d"),
        )
    )

    recent_90d_stats = (
        df.filter(
            F.col("purchase_datetime")
            >= F.current_timestamp() - F.expr("INTERVAL 90 DAYS")
        )
        .groupBy("customer_id")
        .agg(
            F.sum(
                F.when(
                    F.col("items").rlike(".*Мясо, рыба, яйца и бобовые.*"), 1
                ).otherwise(0)
            ).alias("meat_purchases_90d")
        )
    )

    cart_size_stats = df.groupBy("customer_id").agg(
        F.avg(F.size(F.split(F.col("items"), ","))).alias("avg_cart_size"),
        F.sum(F.when(F.size(F.split(F.col("items"), ",")) == 1, 1).otherwise(0)).alias(
            "single_item_purchases"
        ),
    )

    category_diversity = (
        df.select("customer_id", "items")
        .withColumn("item_categories", F.split(F.col("items"), ","))
        .withColumn("category", F.explode("item_categories"))
        .groupBy("customer_id")
        .agg(F.countDistinct("category").alias("unique_categories"))
    )

    features_df = (
        customer_stats.join(category_stats, "customer_id", "left")
        .join(recent_category_stats, "customer_id", "left")
        .join(recent_14d_stats, "customer_id", "left")
        .join(recent_7d_stats, "customer_id", "left")
        .join(recent_90d_stats, "customer_id", "left")
        .join(cart_size_stats, "customer_id", "left")
        .join(category_diversity, "customer_id", "left")
        .fillna(0)
    )

    result_df = features_df.select(
        "customer_id",
        F.when(F.col("milk_purchases_30d") > 0, 1)
        .otherwise(0)
        .alias("bought_milk_last_30d"),
        F.when(F.col("fruits_purchases_14d") > 0, 1)
        .otherwise(0)
        .alias("bought_fruits_last_14d"),
        F.when(F.col("veggies_purchases_14d") == 0, 1)
        .otherwise(0)
        .alias("not_bought_veggies_14d"),
        F.when(F.col("recent_30d_purchases") > 2, 1)
        .otherwise(0)
        .alias("recurrent_buyer"),
        F.when(
            (F.col("recent_30d_purchases") > 0) & (F.col("recent_14d_purchases") == 0),
            1,
        )
        .otherwise(0)
        .alias("inactive_14_30"),
        F.when(F.datediff(F.current_timestamp(), F.col("first_purchase_date")) <= 30, 1)
        .otherwise(0)
        .alias("new_customer"),
        F.when(F.col("delivery_purchases") > 0, 1).otherwise(0).alias("delivery_user"),
        F.when(F.col("organic_purchases") > 0, 1)
        .otherwise(0)
        .alias("organic_preference"),
        F.when(F.col("avg_cart_value") > 1000, 1).otherwise(0).alias("bulk_buyer"),
        F.when(F.col("avg_cart_value") < 200, 1).otherwise(0).alias("low_cost_buyer"),
        F.when(F.col("bakery_purchases") > 0, 1).otherwise(0).alias("buys_bakery"),
        F.when((F.col("total_purchases") >= 3) & (F.col("card_purchases") > 0), 1)
        .otherwise(0)
        .alias("loyal_customer"),
        F.when(F.col("unique_cities") > 1, 1).otherwise(0).alias("multicity_buyer"),
        F.when(F.col("meat_purchases_7d") > 0, 1)
        .otherwise(0)
        .alias("bought_meat_last_week"),
        F.when(F.col("night_purchases") > 0, 1).otherwise(0).alias("night_shopper"),
        F.when(F.col("morning_purchases") > 0, 1).otherwise(0).alias("morning_shopper"),
        F.when(F.col("cash_purchases") >= F.col("total_purchases") * 0.7, 1)
        .otherwise(0)
        .alias("prefers_cash"),
        F.when(F.col("card_purchases") >= F.col("total_purchases") * 0.7, 1)
        .otherwise(0)
        .alias("prefers_card"),
        F.when(F.col("weekend_purchases") >= F.col("total_purchases") * 0.6, 1)
        .otherwise(0)
        .alias("weekend_shopper"),
        F.when(F.col("weekday_purchases") >= F.col("total_purchases") * 0.6, 1)
        .otherwise(0)
        .alias("weekday_shopper"),
        F.when(F.col("single_item_purchases") >= F.col("total_purchases") * 0.5, 1)
        .otherwise(0)
        .alias("single_item_buyer"),
        F.when(F.col("unique_categories") >= 4, 1).otherwise(0).alias("varied_shopper"),
        F.when(F.col("unique_stores") == 1, 1).otherwise(0).alias("store_loyal"),
        F.when(F.col("unique_stores") > 1, 1).otherwise(0).alias("switching_store"),
        F.when(F.col("avg_cart_size") >= 4, 1).otherwise(0).alias("family_shopper"),
        F.when(F.col("early_bird_purchases") > 0, 1).otherwise(0).alias("early_bird"),
        F.when(F.col("total_purchases") == 0, 1).otherwise(0).alias("no_purchases"),
        F.when(F.col("total_spent_7d") > 2000, 1)
        .otherwise(0)
        .alias("recent_high_spender"),
        F.when(F.col("fruits_purchases_30d") >= 3, 1).otherwise(0).alias("fruit_lover"),
        F.when(F.col("meat_purchases_90d") == 0, 1)
        .otherwise(0)
        .alias("vegetarian_profile"),
    ).withColumn("ds", F.lit(run_ds))

    # запись в S3/MinIO parquet, партиционирование по ds
    (
        result_df.write.mode("overwrite")
        .partitionBy("ds")
        .option("compression", "snappy")
        .parquet(f"s3a://{bucket_name}/{table_out}/")
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table_in", required=True, help="Входная таблица")
    parser.add_argument("--table_out", required=True, help="Выходная таблица")
    parser.add_argument("--ds", required=True, help="Дата в формате YYYYMMDD")
    parser.add_argument("--bucket_name", required=True, help="Имя бакета")

    args = parser.parse_args()

    # Запуск основного приложения
    spark_app(
        args.ds,
        args.table_in,
        args.table_out,
        args.bucket_name,
    )
