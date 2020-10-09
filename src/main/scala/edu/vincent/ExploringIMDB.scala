package edu.vincent

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.types.{ByteType, IntegerType, StringType, StructType}

object ExploringIMDB {

  final case class Movie(tconst:String,titleType:String, primaryTitle:String, originalTitle:String,
                         isAdult:Byte, startYear:Int, endYear:Int, runtimeMinutes: Int, genres:String)

  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("ExploringIMDB")
      .master("local[*]")
      .getOrCreate()

    val movieSchema = new StructType()
      .add("tconst", StringType, nullable = true)
      .add("titleType", StringType, nullable = true)
      .add("primaryTitle", StringType, nullable = true)
      .add("originalTitle", StringType, nullable = true)
      .add("isAdult", ByteType, nullable = true)
      .add("startYear", IntegerType, nullable = true)
      .add("endYear", IntegerType, nullable = true)
      .add("runtimeMinutes", IntegerType, nullable = true)
      .add("genres", StringType, nullable = true)

    import spark.implicits._
    val ds = spark.read
      .option("sep", "\t")
      .option("header", "true")
      .schema(movieSchema)
      .csv("data/title.basics.tsv")
      .as[Movie]

    val horrorMoviesOnly = ds
      .select("primaryTitle",  "startYear", "runtimeMinutes")
      .filter($"titleType"=== "movie" &&
        $"genres".contains("Horror") && $"runtimeMinutes".isNotNull)
      .sort(desc("startYear"))
      .show()

  }

}
