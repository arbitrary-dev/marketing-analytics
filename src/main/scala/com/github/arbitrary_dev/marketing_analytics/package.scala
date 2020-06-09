package com.github.arbitrary_dev

import java.sql.Timestamp

package object marketing_analytics {

  case class Event(
    userId: String,
    eventId: String,
    eventTime: Timestamp,
    eventType: String,
    attributes: Option[Map[String, String]],
  )

  case class Purchase(
    purchaseId: String,
    purchaseTime: Timestamp,
    billingCost: Double,
    isConfirmed: Boolean,
  )

  case class Attribution(
    purchaseId: String,
    purchaseTime: Timestamp,
    billingCost: Double,
    isConfirmed: Boolean,
    sessionId: String,
    campaignId: String,
    channelId: String,
  )
}
