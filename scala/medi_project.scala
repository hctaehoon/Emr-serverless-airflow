import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.Row
//import spark.implicits._

object MainApp {
  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("ChasagoLog") {
      head("ChasagoLog", "1.0")
      opt[String]("path").action((x, c) => c.copy(path = x)).text("airflow에서 받아온 prefix")
      // opt[String]("token").action((x, c) => c.copy(token = x)).text("slack token")
    }
    parser.parse(args, Config()) match {
      case Some(config) =>
        val basePath = "s3://taehun-s3-bucket-230717"
        val s3Path = basePath + config.path + "/*"
//        val token = config.token
        val spark = SparkSession.builder.appName(s"${config.path}/chasago_log_app").getOrCreate()



        // Create segment.log / log.session Table 만들기 위한 Schema 지정
        val contextPageSchema = StructType(Array(StructField("url", StringType)))

        val contextSchema = StructType(Array(
          StructField("userAgent", StringType),
          StructField("ip", StringType),
          StructField("locale", StringType),
          StructField("page", contextPageSchema)
        ))
        val propertiesButtonSchema = StructType(Array(
          StructField("name", StringType),
          StructField("hospital", ArrayType(StringType))
        ))
        val propertiesActionSchema = StructType(Array(
          StructField("name", StringType),
          StructField("hospital", ArrayType(StringType))
        ))
        val propertiesPropertiesSchema = StructType(Array(
          StructField("hospitalId", StringType),
          StructField("pageName", StringType),
          StructField("routeName", StringType),
          StructField("to", StringType)
        ))
        val propertiesSchema = StructType(Array(
          StructField("path", StringType),
          StructField("page", StringType),
          StructField("button", propertiesButtonSchema),
          StructField("action", propertiesActionSchema),
          StructField("properties", propertiesPropertiesSchema)
        ))
        val overallSchema = StructType(Array(
          StructField("anonymousId", StringType),
          StructField("context", contextSchema),
          StructField("messageId", StringType),
          StructField("hospitalId", StringType),
          StructField("properties", propertiesSchema),
          StructField("event", StringType),
          StructField("timestamp", StringType),
          StructField("type", StringType),
          StructField("userId", StringType)
          StructField("partition_0", StringType) 
        ))
        var df = spark.read.option("mergeSchema", "true").schema(overallSchema).json(s3Path)
        df = df.cache()

        // logtable 생성
        logTable = df.withColumn("timestamp_cov", col("timestamp").cast("timestamp"))
          .withColumn("utc_timestamp", F.to_utc_timestamp(col("timestamp_cov"), "UTC"))
          .withColumn("kst_timestamp", F.from_utc_timestamp(col("utc_timestamp"), "Asia/Seoul"))
          .withColumn("add_kst_timestamp", F.date_add(col("kst_timestamp"), 1))
          .withColumn("ywd", F.date_sub(F.date_trunc("week", col("add_kst_timestamp")), 1))
          .withColumn("yw", F.concat(F.year(col("add_kst_timestamp")), F.lit("-"), F.weekofyear(col("add_kst_timestamp")).cast("string")))
          .withColumn("ym", F.date_format(col("timestamp"), "yyyy-MM").cast("string"))
          .withColumn("ymd", F.date_format(col("timestamp"), "yyyy-MM-dd").cast("string"))
          .withColumn("partition_0", col(config.path))
        logTable = logTable.selectExpr(
          "anonymousId",
          "context.userAgent AS context_useragent",
          "context.ip AS context_ip",
          "context.locale AS context_locale",
          "context.page.url AS context_page_url",
//          "context",
          "messageId",
          "properties.path AS properties_path",
          "properties.page AS properties_page",
          "properties.button.name AS properties_button_name",
          "properties.action.name AS properties_action_name",
          "properties.action.hospital[0] AS properties_action_hospital_id",
          "properties.action.hospital[1] AS properties_action_hospital_name",
          "properties.button.hospital[0] AS properties_button_hospital_num",
          "properties.button.hospital[1] AS properties_button_hospital_name",
          "properties.properties.hospitalid AS properties_properties_hospitalid",
          "properties.properties.pagename AS properties_properties_pagename",
          "properties.properties.routename AS properties_properties_routename",
          "properties.properties.to AS properties_properties_to",
          "properties",
          "event",
          "kst_timestamp AS timestamp",
          "yw",
          "ywd",
          "ym",
          "ymd",
          "type",
          "userId",
          "partition_0"
        )
12:45


         // log_session table 생성
        val window = Window.partitionBy(df("anonymousid")).orderBy(df("timestamp"))

        var session_df = df.withColumn("inactivity_time",
          (unix_timestamp(df("timestamp")) - lag(unix_timestamp(df("timestamp")),1).over(window))/60)

        session_df = session_df.filter((session_df("inactivity_time") > 30) || session_df("inactivity_time").isNull)

        // Add session_id column
        session_df = session_df.withColumn("session_id", concat(session_df("anonymousid"), lit('-'),
          row_number().over(window)))

        // Add next_session_start_at column
        session_df = session_df.withColumn("next_session_start_at",
          lead(session_df("timestamp"),1).over(window))

        // Drop the inactivity_time column
        session_df = session_df.drop("inactivity_time")

        val logSessionTable = session_df.selectExpr("session_id","anonymousid","timestamp AS session_start_at","next_session_start_at")

// 파일 지정 경로
        val tempdir = s"s3://taehun-s3-bucket-230717/tempdir/${config.path}"


        logTable.write
          .option("header", "true")
          .mode("overwrite")
          .json(tempdir + "/log")

        logSessionTable.write
          .option("header", "true")
          .mode("overwrite")
          .json(tempdir + "/log_session")



        //procedure 추가 테이블 생성 ( medi에서 제공한 S3 폴더 중 3번째 폴더 데이터 사용 , 위 테이블들과 데이터가 다름, 자동화 구현 x)
        val context_page_search_schema = StructType(Array(
          StructField("keyword", StringType),
          StructField("boardId", StringType),
          StructField("type", StringType ),
        ))

        val context_page_schema = StructType(Array(
          StructField("path", StringType),
          StructField("referrer", StringType),
          StructField("title", StringType),
          StructField("url", StringType),
          StructField("search", context_page_search_schema)

        ))

        val context_schema = StructType(Array(
          StructField("page", context_page_schema),
          StructField("userAgent", StringType),
          StructField("ip", StringType),

        ))

        val properties_schema = StructType(Array(
          StructField("url", StringType),
          StructField("creative", StringType),
          StructField("name", StringType),
          StructField("position", StringType),
          StructField("promotion_id", StringType),

        ))
        val overall_schema = StructType(Array(
          StructField("category", StringType),
          StructField("context", context_schema),
          StructField("event", StringType),
          StructField("messageId", StringType),
          StructField("name", StringType),
          StructField("properties", properties_schema),
          StructField("integrations",
            StructType(Array(
              StructField("Actions Amplitude",
                StructType(Array(
                  StructField("session_id", StringType)
                ))
              )
            ))
          ),
          StructField("timestamp", StringType),
          StructField("traits",
            StructType(Array(
              StructField("customerId", StringType)
            ))
          ),
          StructField("type", StringType),
          StructField("userId", StringType),
          StructField("version", StringType)

        ))
        var procedureData = spark.read.option("mergeSchema","true").schema(overall_schema).json("s3://taehun-s3-bucket-230717/1652313600000/*")
        procedureData = procedureData.cache()
        procedureData = procedureData.withColumn("timestamp_cov", col("timestamp").cast("timestamp"))
          .withColumn("utc_timestamp", to_utc_timestamp(col("timestamp_cov"), "UTC"))
          .withColumn("kst_timestamp", from_utc_timestamp(col("utc_timestamp"), "Asia/Seoul"))
          .withColumn("yw", concat(year(col("kst_timestamp")), lit("-"), weekofyear(col("kst_timestamp")).cast("string")))
          .withColumn("ywd", date_trunc("week", col("kst_timestamp")))
          .withColumn("ym", date_format(col("timestamp"), "yyyy-MM").cast("string"))
          .withColumn("ymd", date_format(col("timestamp"), "yyyy-MM-dd").cast("string"))
        procedureData = procedureData.selectExpr(
          "context.page.path AS context_page_path",
          "context.page.referrer AS context_page_referrer",
          "context.page.search AS context_page_search",
          "context.page.search.keyword AS context_page_search_keyword",
          "context.page.search.boardId AS context_page_search_board",
          "context.page.search.type AS context_page_search_type",
          "context.page.title AS context_page_title",
          "context.page.url AS context_page_url",
          "context.userAgent AS context_useragent",
          "context.ip AS context_ip",
          "messageId",
          "kst_timestamp AS timestamp",
          "yw",
          "ywd",
          "ym",
          "ymd",
          "properties.url AS properties_url",
          "properties.creative AS properties_creative",
          "properties.name AS properties_name",
          "properties.position AS properties_position",
          "properties.promotion_id AS properties_promotion_id",
          "integrations.`Actions Amplitude`.session_id AS integrations_session_id",
          "type",
          "userId",
          "version",
          "traits.customerId AS traits_customerid",
          "category",
          "name",
          "event",
          "partition_0"

        )
        val tempdirTest = s"s3://taehun-s3-bucket-230717/csv/procedure"
        procedureData.write
          .option("header", "true")
          .mode("overwrite")
          .json(tempdirTest + "/log")





// 데이터 정합성 체크 ( log , log_session 테이블만 구현)
val dataLakeMessageidCount = df.select("messageId").count()
        val distinctDataLakeCount = df.select("messageId").distinct().count()
        val dataLakeTotalCount = df.count()
        val dataLakeSessionidCount = session_df.select("session_id").count()
        val dataLakeTotalSessionCount = session_df.count()

        val testLogData = spark.read.json(tempdir + "/log")
        val testLogSessionData = spark.read.json(tempdir + "/log_session")

        val dataWareHouseMessageidCount = testLogData.select("messageId").count()
        val distinctDataWareHouseCount = testLogData.select("messageId").distinct().count()
        val dataWareHouseTotalCount = testLogData.count()
        val dataWareHouseSessionidCount = testLogSessionData.select("session_id").count()
        val dataWareHouseTotalSessionCount = testLogSessionData.count()

        val data = Seq(
          Row("s3MessageidCount", dataLakeMessageidCount),
          Row("distinctCount", distinctDataLakeCount),
          Row("TotalCount", dataLakeTotalCount),
          Row("redShiftMessageidCount", dataWareHouseMessageidCount),
          Row("distinctCount", distinctDataWareHouseCount),
          Row("TotalCount", dataWareHouseTotalCount),
          Row("s3SessionidCount", dataLakeSessionidCount),
          Row("totalCount", dataLakeTotalSessionCount),
          Row("redShiftSessionidCount", dataWareHouseSessionidCount),
          Row("totalCount", dataWareHouseTotalSessionCount)
        )

        val test_df = spark.createDataFrame(
          spark.sparkContext.parallelize(data),
          StructType(List(StructField("metric", StringType), StructField("value", LongType)))
        )

        test_df.write
          .option("header", "true")
          .mode("overwrite")
          .json(tempdir + "/checkData")
      case None =>
    }
  }
}
case class Config(path: String = "", token: String = "")