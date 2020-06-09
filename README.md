# Marketing Analytics

## Given datasets

### Mobile App clickstream projection

`mobile-app-clickstream.csv`
- userId: String
- eventId: String
- eventTime: Timestamp
- eventType: String
- attributes: Option[Map[String, String]]

There could be events of the following types that form a user engagement session:
- app_open
- search_product
- view_product_details
- purchase
- app_close

Events of `app_open` type may contain the attributes relevant to the marketing analysis:
- campaign_id`
- channel_id

Events of purchase type contain `purchase_id` attribute.

### Purchases projection

`purchases.csv`
- purchaseId: String
- purchaseTime: Timestamp
- billingCost: Double
- isConfirmed: Boolean

## Tasks #1. Build Purchases Attribution Projection

The projection is dedicated to enabling a subsequent analysis of marketing campaigns and channels.

The target schema:
- purchaseId: String
- purchaseTime: Timestamp
- billingCost: Double
- isConfirmed: Boolean
- sessionId: String // a session starts with app_open event and finishes with app_close
- campaignId: String // derived from app_open#attributes#campaign_id
- channelId: String // derived from app_open#attributes#channel_id

Requirements to implementation of the projection building logic:
- Task #1.1. Implement it by utilizing default Spark SQL capabilities.
- Task #1.2. Implement it by using a custom Aggregator or UDAF.

## Tasks #2. Calculate Marketing Campaigns And Channels Statistics

Use the purchases-attribution projection to build aggregates that provide the following
insights:
- Task #2.1. Top Campaigns:  
  What are the Top 10 marketing campaigns that bring the biggest revenue
  (based on `billingCost` of confirmed purchases)?
- Task #2.2. Channels engagement performance:  
  What is the most popular (i.e. Top) channel that drives the highest amount of
  unique sessions (engagements) with the App in each campaign?

Requirements to the tasks #2:
- Should be implemented by using plain SQL on top of Spark DataFrame API
- Will be a plus: an additional alternative implementation of the same tasks by using Spark
  Scala DataFrame / Datasets API only (without plain SQL)
