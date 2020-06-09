package com.github.arbitrary_dev.marketing_analytics

import java.util.UUID

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Job extends SparkSessionWrapper {

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("ERROR")

    val events = spark.read
      .option("escape", "\"")
      .option("header", "true")
      .csv("mobile-app-clickstream.csv")
      .select(
        $"userId",
        $"eventId",
        'eventTime.cast(TimestampType),
        $"eventType",
        from_json(
          regexp_replace(
            $"attributes",
            """(\{)\{|(})}""",
            """$1$2""",
          ),
          schema = MapType(StringType, StringType)
        ).as("attributes"),
      )
      .where($"eventType".isin("app_open", "purchase", "app_close"))
      .repartition($"userId")
      .sortWithinPartitions($"eventTime")
      .as[Event]

    // events.printSchema()
    // events.show(numRows = 5, truncate = false)

    val purchases = spark.read
      .option("header", value = true)
      .csv("purchases.csv")
      .select(
        $"purchaseId",
        'purchaseTime.cast(TimestampType),
        'billingCost.cast(DoubleType),
        'isConfirmed.cast(BooleanType),
      )
      .as[Purchase]

    // purchases.printSchema()
    // purchases.show(numRows = 5, truncate = false)

    // Task #1.1. Implement it by utilizing default Spark SQL capabilities.
    val attributions = events
      .groupByKey(_.userId)
      .flatMapGroups { case (_, events) =>
        val f = events.foldLeft[(List[String], Vector[List[String]])]((List.empty, Vector.empty)) {
          case ((currentSession, acc), event) =>
            event.eventType match {
              case "app_open"  =>
                val newSession = for {
                  attrs <- event.attributes
                  campaignId <- attrs.get("campaign_id")
                  channelId <- attrs.get("channel_id")
                } yield List(UUID.randomUUID().toString, campaignId, channelId)
                newSession.fold((List.empty[String], acc))((_, acc))
              case "app_close" =>
                (List.empty, acc)
              case "purchase"  =>
                val cols = for {
                  attrs <- event.attributes
                  pid <- attrs.get("purchase_id")
                } yield currentSession :+ pid
                (currentSession, cols.fold(acc)(acc :+ _))
            }
        }
        f._2
      }
      .map {
        case sid :: cid :: chid :: pid :: Nil => (sid, cid, chid, pid)
        case _                                => ("null", "null", "null", "null")
      }
      .toDF("sessionId", "campaignId", "channelId", "purchaseId")
      .join(purchases, "purchaseId")
      .as[Attribution]
      .persist()

    attributions.show(truncate = false)

    // TODO Task #1.2. Implement it by using a custom Aggregator or UDAF.

    // Task #2.1. Top Campaigns
    attributions
      .filter(_.isConfirmed)
      .groupBy($"campaignId")
      .agg(sum($"billingCost").as("revenue"))
      .orderBy($"revenue".desc)
      .limit(10)
      .show(truncate = false)


    // Task #2.2. Channels engagement performance
    attributions
      .dropDuplicates("sessionId")
      .groupBy($"campaignId", $"channelId")
      .agg(count("*").as("count"))
      .orderBy($"count".desc)
      .limit(1)
      .show(truncate = false)

    spark.sparkContext.stop()
  }
}
