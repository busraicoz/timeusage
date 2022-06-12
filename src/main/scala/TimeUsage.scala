import java.nio.file.Paths
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object TimeUsage {

  import org.apache.spark.sql.SparkSession
  import org.apache.spark.sql.functions._


  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()


  import spark.implicits._

  def main(args: Array[String]): Unit = {
    timeUsageByLifePeriod()
  }

  def timeUsageByLifePeriod(): Unit = {
    val (columns, initDf) = read("/timeusage/atussum.csv")
    val (primaryneedsColumns, workColumns, otherColumns) = classifiedColumns(columns)
    val summaryDf = timeUsageSummary(primaryneedsColumns, workColumns, otherColumns, initDf)
    timeUsageSummaryToWriteCassandra(summaryDf)
    val finalDf = timeUsageGroupedTyped(timeUsageSummaryTyped(summaryDf))
    finalDf.show()
  }

  //Summary Data Frame write into cassandra timeusage.time_usage table.
  def timeUsageSummaryToWriteCassandra(summaryDf: DataFrame)={
    summaryDf.write.format("org.apache.spark.sql.cassandra").options(Map( "table" -> "time_usage", "keyspace" -> "timeusage")).save()
  }


  def read(resource: String): (List[String], DataFrame) = {
    val rdd = spark.sparkContext.textFile(fsPath(resource))

    val headerColumns = rdd.first().split(",").to[List]
    // Compute the schema based on the first line of the CSV file
    val schema = dfSchema(headerColumns)

    val data =
      rdd
        .mapPartitionsWithIndex((i, it) => if (i == 0) it.drop(1) else it) // skip the header line
        .map(_.split(",").to[List])
        .map(row)

    val dataFrame =
      spark.createDataFrame(data, schema)

    (headerColumns, dataFrame)
  }

  def fsPath(resource: String): String =
    Paths.get(getClass.getResource(resource).toURI).toString

  def dfSchema(columnNames: List[String]): StructType = {
    val firstField = StructField(columnNames.head, StringType, nullable = false)
    val fields = columnNames.tail.map(field => StructField(field, DoubleType, nullable = false))
    StructType(firstField::fields)
  }


  def row(line: List[String]): Row = {
    val list = line.head::line.tail.map(_.toDouble)
    Row.fromSeq(list)
  }




  def classifyColumnsRec(columnNames: List[String], primary: List[Column], working: List[Column], leisure: List[Column]): (List[Column], List[Column], List[Column]) =
    columnNames match {
      case Nil => (primary, working, leisure)

      case x::xs =>
        if (x.startsWith("t01") || x.startsWith("t03") || x.startsWith("t11") || x.startsWith("t1801") || x.startsWith("t1803"))
          classifyColumnsRec(xs,  col(x)::primary, working, leisure)
        else if (x.startsWith("t05") || x.startsWith("t1805"))
          classifyColumnsRec(xs, primary, col(x)::working, leisure)
        else if (x.startsWith("t02") || x.startsWith("t04") || x.startsWith("t06") || x.startsWith("t07") || x.startsWith("t08") || x.startsWith("t09") ||
          x.startsWith("t10") || x.startsWith("t12") || x.startsWith("t13") || x.startsWith("t14") || x.startsWith("t15") || x.startsWith("t16") || x.startsWith("t18"))
          classifyColumnsRec(xs, primary, working,  col(x)::leisure)
        else
          classifyColumnsRec(xs, primary, working, leisure)
    }

  def classifiedColumns(columnNames: List[String]): (List[Column], List[Column], List[Column]) = {
    val primary = List[Column]()
    val working = List[Column]()
    val leisure = List[Column]()

    classifyColumnsRec(columnNames, primary, working, leisure)
  }


  def timeUsageSummary(
                        primaryneedsColumns: List[Column],
                        workColumns: List[Column],
                        otherColumns: List[Column],
                        df: DataFrame
                      ): DataFrame = {
    val workingStatusProjection: Column = when(df("telfs") < 3, "working").otherwise("not working").as("working")
    val sexProjection: Column = when(df("tesex") === 1, "male").otherwise("female").as("sex")
    val ageProjection: Column = when(df("teage") >= 15 && df("teage") <= 22 , "young").
      when(df("teage") >= 23 && df("teage") <= 55 , "active").
      otherwise("elder").as("age")

    def sum_udf(columns: List[Column], sum: Column): Column = columns match {
      case Nil => sum
      case x::xs => sum_udf(xs, sum + df(x.toString()))
    }
    val primaryneedsProjection: Column = (sum_udf(primaryneedsColumns, lit(0.0d)) / 60.0).as("primaryneeds")
    val workProjection: Column = (sum_udf(workColumns, lit(0.0d)) / 60.0d).as("work")
    val otherProjection: Column = (sum_udf(otherColumns, lit(0.0d)) / 60.0d).as("other")

    df
      .select(workingStatusProjection, sexProjection, ageProjection, primaryneedsProjection, workProjection, otherProjection)
      .where($"telfs" <= 4) // Discard people who are not in labor force
  }

  def timeUsageSummaryTyped(timeUsageSummaryDf: DataFrame): Dataset[TimeUsageRow] =
    timeUsageSummaryDf.map(r => TimeUsageRow(r.getAs("working"), r.getAs("sex"), r.getAs("age"), r.getAs("primaryneeds"), r.getAs("work"), r.getAs("other")))

  def timeUsageGroupedTyped(summed: Dataset[TimeUsageRow]): Dataset[TimeUsageRow] = {
    import org.apache.spark.sql.expressions.scalalang.typed
    summed.groupByKey(r => (r.working, r.sex, r.age))
      .agg(round(typed.avg[TimeUsageRow](_.primaryneeds), 1).as(Encoders.DOUBLE),
        round(typed.avg[TimeUsageRow](_.work), 1).as(Encoders.DOUBLE),
        round(typed.avg[TimeUsageRow](_.other), 1).as(Encoders.DOUBLE))
      .map(k => TimeUsageRow(k._1._1, k._1._2, k._1._3, k._2, k._3, k._4))
      .sort($"working", $"sex", $"age")
  }
}

case class TimeUsageRow(
                         working: String,
                         sex: String,
                         age: String,
                         primaryneeds: Double,
                         work: Double,
                         other: Double
                       )