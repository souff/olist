import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OlistCli {
  def listDelayedDeliveries(spark: SparkSession, in: String, out: String) = {
    val df_order = spark.read.option("header", true).csv(in + "olist_orders_dataset.csv").withColumn("order_utc", to_utc_timestamp(col("order_purchase_timestamp"), "Brazil/East"))
    df_order.createOrReplaceTempView("orders")
    val delivrey_date_null = spark.sql(
      """
        SELECT *,
               NULL AS delivered_utc,
               Datediff(To_timestamp("2018-09-03 12:06:57"), order_utc) AS delay
        FROM   (SELECT order_id,
                       customer_id,
                       order_status,
                       order_utc
                FROM   orders
                WHERE  order_status NOT IN ( 'canceled', 'unavailable' ) AND order_delivered_customer_date IS NULL)
        WHERE Datediff(To_timestamp("2018-09-03 12:06:57"), order_utc) >10
        ORDER  BY delay DESC
        """)

    val df_customer = spark.read.option("header", true).csv(in + "olist_customers_dataset.csv")
    df_customer.createOrReplaceTempView("customer")


    val df_timezone = spark.read.option("header", true).csv(in + "time_zone.csv")
    df_timezone.createOrReplaceTempView("timezone")
    val delevery_with_date = spark.sql(
      """
      SELECT *,
             Datediff(delivered_utc, order_utc) AS delay
      FROM   (SELECT o.order_id,
                     o.customer_id,
                     o.order_status,
                     order_utc,
                     To_utc_timestamp(o.order_delivered_customer_date, t.tz) AS delivered_utc
              FROM   customer c
                     JOIN timezone t
                       ON c.customer_state = t.state
                     JOIN orders o
                       ON c.customer_id = o.customer_id
              WHERE  o.order_delivered_customer_date IS NOT NULL)
      WHERE  Datediff(delivered_utc, order_utc) > 10
      ORDER  BY delay DESC
    """
    )
    val df_delivery = delivrey_date_null.union(delevery_with_date).sort(desc("delay"))
    df_delivery.repartition(1).write.mode("Overwrite").option("header", "true").csv(out)
  }

  def run(f: SparkSession => Unit) = {
    val builder = SparkSession.builder.appName("Spark Batch")
    val spark = builder.getOrCreate()
    f(spark)
    spark.close
  }

  def main(args: Array[String]) = {
    val arguments = if (args.isEmpty) Array("data/", "csv") else args
    println("Olist Cli")
    run((spark: SparkSession) => listDelayedDeliveries(spark, arguments(0), arguments(1)))
  }
}
