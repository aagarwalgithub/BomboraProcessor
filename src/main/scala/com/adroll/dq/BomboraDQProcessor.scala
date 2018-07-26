package com.adroll.dq

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import core.logs.Logline
import core.logs.filters.BadCookieFilter
import org.apache.hadoop.io.LongWritable
import org.apache.spark.sql.{Column, SparkSession}
import segments.hadoop.input.BomboraAttributesInputFormat
import segments.hadoop.io.BomboraCookieAttributesWritable

object BomboraDQProcessor {
  def main(args: Array[String]): Unit = {
    println("BomboraDQProcessor")

    val spark = SparkSession
      .builder()
      .appName("BomboraDQProcessor")
      .getOrCreate()


    spark.sparkContext.hadoopConfiguration.set("adroll.config", "production-hadoop.xml")
    spark.sparkContext.hadoopConfiguration.addResource("production-hadoop.xml")
    spark.sparkContext.hadoopConfiguration.setLong("bombora.input.format.start_date", 1532131200000L)
    spark.sparkContext.hadoopConfiguration.setLong("bombora.input.format.end_date", 1532131200000L)

    spark.sparkContext.hadoopConfiguration.setStrings(BomboraAttributesInputFormat.BOMBORA_LOGS_PREFIXES_CONF, "bombora-domain-data")

    val rdd = spark.sparkContext.newAPIHadoopRDD(spark.sparkContext.hadoopConfiguration, classOf[BomboraAttributesInputFormat],
      classOf[LongWritable], classOf[BomboraCookieAttributesWritable])

    val rowsRdd = rdd.map(x => (isNone(x._2.getCookie), isNoneAttribute(x._2.getAttributes.get("domain")))).repartition(500)
    val df = spark.createDataFrame(rowsRdd).toDF("cookie", "domain")
    df.repartition(500).cache()

    val recCount: Long = df.count()
    val cookieNullCount = df.filter(new Column("cookie").isNull).count
    //    val cookieValidCount = df.filter(x => isValidCookie(x.getAs[String](0))).count // isValidCookie true
    val cookieInvalidCount = df.filter(x => !isValidCookie(x.getAs[String](0))).count
    val cookieNotNullCount = df.filter(new Column("cookie").isNotNull).count
    val distinctCookieCount = df.groupBy(new Column("cookie")).sum() //.agg(count(new Column("cookie")))
    val dupCookieCount = df.groupBy(new Column("cookie")).count.filter(new Column("count") > 1).count
    val uniqueCookieCount = df.groupBy(new Column("cookie")).count.filter(new Column("count").equalTo(1)).count
    val domainNullCount = df.filter(new Column("domain").isNull).count
    val domainNotNullCount = df.filter(new Column("domain").isNotNull).count
    val distinctDomainCount = df.select("domain").distinct.count //df.groupBy(new Column("domain")).agg(count(lit(1)))
    val dupDomainCount = df.filter(new Column("domain").isNotNull).groupBy(new Column("domain")).count.filter(new Column("count") > 1).count
    val dupWithNullDomainCount = df.groupBy(new Column("domain")).count.filter(new Column("count") > 1).count
    val uniqueDomainCount = df.groupBy(new Column("domain")).count.filter(new Column("count").equalTo(1)).count

    println("recCount : " + recCount)
    println("cookieNullCount : " + cookieNullCount)
    println("cookieInvalidCount : " + cookieInvalidCount)
    println("cookieNotNullCount : " + cookieNotNullCount)
    println("distinctCookieCount : " + distinctCookieCount)
    println("dupCookieCount : " + dupCookieCount)
    println("uniqueCookieCount : " + uniqueCookieCount)
    println("domainNullCount : " + domainNullCount)
    println("distinctDomainCount : " + distinctDomainCount)
    println("domainNotNullCount : " + domainNotNullCount)
    println("dupDomainCount : " + dupDomainCount)
    println("dupWithNullDomainCount : " + dupWithNullDomainCount)
    println("uniqueDomainCount : " + uniqueDomainCount)

    df.write.option("sep", Logline.DELIMITER).option("compression", "gzip").csv("/output")
    spark.close()
  }

  def isNone(param: String): Option[String] = {
    if (param != null && !param.trim().isEmpty())
      Some(param.trim.replaceAll("(\\t|\\r?\\n|\\s+)+", " "))
    else
      None
  }

  def isNoneAttribute(param: AttributeValue): Option[String] = {
    if (param != null && !param.getS.isEmpty)
      Some(param.getS.replaceAll("(\\t|\\r?\\n|\\s+)+", " "))
    else
      None
  }

  def isValidCookie(cookie: String): Boolean = {
    val m = BadCookieFilter.VALID_COOKIE_PATTERN.matcher(cookie)
    m.matches
  }

  def isNull(param: String): String = {
    if (param != null && !param.trim().isEmpty())
      param.trim.replaceAll("(\\t|\\r?\\n|\\s+)+", " ")
    else
      null
  }

  def isNullAttribute(param: AttributeValue): String = {
    if (param != null && !param.getS.isEmpty)
      param.getS.replaceAll("(\\t|\\r?\\n|\\s+)+", " ")
    else
      null
  }

  def isValidCookie(cookie: Option[String]): Boolean = {
    cookie match {
      case Some(cookie) => {
        val m = BadCookieFilter.VALID_COOKIE_PATTERN.matcher(cookie)
        m.matches
      }
      case None => false
    }
  }
}