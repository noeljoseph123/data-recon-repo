package com.test.spark.reconciler

import com.test.spark.reconciler.MathUtil.CountUtils.percentage
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column



object Reconciler {

  def reconcileDataFrames(oldTable: DataFrame, newTable: DataFrame, primaryKey: Seq[String],
                          spark: SparkSession):
  DataFrame = {

    import spark.implicits._

    val oldTableCount: Long = oldTable.count
    val newTableCount: Long = newTable.count
    val oldTableColumnsList: Seq[String] = oldTable.columns.toSeq diff primaryKey
    val reconcilerColumnsList: Seq[String] = oldTableColumnsList ++ oldTableColumnsList.map(c => "new_" + c)
    val newTableColumnsList = newTable.columns.toSeq.map(c => {
      if(primaryKey.contains(c)) { c }
      else { "new_" + c}
    })
    val newTableWithColumnsRenamed = newTable.toDF(newTableColumnsList: _*)


    val joinDataFrames = oldTable.join(
      newTableWithColumnsRenamed,
      primaryKey
    )
      .select(reconcilerColumnsList.map(c => col(c)): _*)

    val commonRecordCount = joinDataFrames.count

    val matchColumnsList: Seq[String] = oldTableColumnsList.map(column => column + "_match")
    val matchColumnFunctions: Seq[Column] = oldTableColumnsList.map(column => when(col(column) <=> col("new_" + column), lit(1))
      .otherwise(lit(0)))

    val dataframeWithMatchColumns = selectColumns(joinDataFrames, matchColumnsList, matchColumnFunctions, spark)

    val matchingRecordCounts = dataframeWithMatchColumns.groupBy().sum(matchColumnsList: _*).toDF(oldTableColumnsList: _*)
    //matchingRecordCounts.cache

    oldTableColumnsList.map(
      column => {
        val recordsWithSameValues: Long = matchingRecordCounts.select(col(column)).head().getLong(0)
        val recordsWithDifferentValues: Long = commonRecordCount - recordsWithSameValues
        val sameValuesPercentage: Double = percentage(recordsWithSameValues, commonRecordCount)
        Seq(ReconModel(column, recordsWithSameValues, recordsWithDifferentValues, sameValuesPercentage)).toDS
      }
    ).reduce(_ union _)
      .union(
        Seq(
          ReconModel("matching_record_count", commonRecordCount, oldTableCount-commonRecordCount, percentage(commonRecordCount, oldTableCount)),
          ReconModel("dropped_records", oldTableCount-commonRecordCount, 0, percentage(oldTableCount-commonRecordCount, oldTableCount)),
          ReconModel("new_records", newTableCount-commonRecordCount, 0, percentage(newTableCount-commonRecordCount, newTableCount))
        ).toDS).toDF()
  }

  def selectColumns(df: DataFrame, columnNames: Seq[String], columnFunctions: Seq[Column], spark: SparkSession): DataFrame = {
    df.select(columnFunctions: _*).toDF(columnNames: _*)
  }

  def main(args: Array[String]): Unit = {
    val path1 = args(0)
    val path2 = args(1)
    val primaryKeys = args(2)
    val primaryKeysSeq = primaryKeys.split(",").toSeq
    val builder = SparkSession.builder.appName("recon app").config("spark.driver.maxResultSize", "5g")
    builder.master("local[*]")
    //builder.enableHiveSupport()
    val spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val df1 = spark.read.parquet(path1)
    val df2 = spark.read.parquet(path2)

    reconcileDataFrames(df1, df2, primaryKeysSeq, spark).show()

  }

}
