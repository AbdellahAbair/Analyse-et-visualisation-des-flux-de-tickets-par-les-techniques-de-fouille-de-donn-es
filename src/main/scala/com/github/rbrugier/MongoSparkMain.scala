package com.github.rbrugier

import com.mongodb.spark._
import com.mongodb.spark.config.ReadConfig
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object MongoSparkMain extends App with LazyLogging {
  val conf = new SparkConf()
    .setAppName("mongozips")
    .setMaster("local[*]")

  val sc = new SparkContext(conf)
  val readConfig = ReadConfig(Map("uri" -> "mongodb://127.0.0.1/", "database" -> "Elis", "collection" -> "elis")) // 1)
  val zipDf = sc.loadFromMongoDB(readConfig).toDF() // 2)
  val sqlContext = new SQLContext(sc)
  zipDf.printSchema() // 3)
  //zipDf.show()
  println( "SparkSQL" )
  zipDf.registerTempTable("elis") // 1)
  sqlContext.sql( // 2)
    """Select Courte_Description,round(avg(DATEDIFF(TO_DATE(CAST(UNIX_TIMESTAMP(Ferme, 'dd/MM/yyyy') AS TIMESTAMP)),TO_DATE(CAST(UNIX_TIMESTAMP(Date_de_creation, 'dd/MM/yyyy') AS TIMESTAMP))))) datediff
       from elis e
       where e.Etat_de_incident like"%Ferm%"
       group by Courte_Description
       having count(Courte_Description)>5

      """
  )
    //.show(500)
    .coalesce(1).
    write.
    format("com.databricks.spark.csv").
    option("header", "true").
    save("C:/Documents/la_duree_moyen_d'un_ticket.csv")
 /*
 zipDf.registerTempTable("elis") // 1)
  sqlContext.sql( // 2)
    """SELECT YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(Date_de_creation, 'dd/MM/yyyy') AS TIMESTAMP))) Year,Groupe_affectation,count(Courte_Description)
      FROM elis e
group by YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(Date_de_creation, 'dd/MM/yyyy') AS TIMESTAMP))),Groupe_affectation
      """
  )
    //.show(500)
  .coalesce(1).
    write.
    format("com.databricks.spark.csv").
    option("header", "true").
    save("C:/Documents/Year_tickets.csv")

  println( "SparkSQL" )
  zipDf.registerTempTable("elis") // 1)
  sqlContext.sql( // 2)
    """SELECT count(Courte_Description),Affecte,Courte_Description
      FROM elis e
group by Affecte,Courte_Description
having count(Courte_Description)>5
      """
  )
  //  .show(500)
  println( "SparkSQL" )
  zipDf.registerTempTable("elis") // 1)
  sqlContext.sql( // 2)
    """SELECT Type,count(Type)
      FROM elis e
group by Type

      """
  )
    //.show(500)
    .coalesce(1).
    write.
    format("com.databricks.spark.csv").
    option("header", "true").
    save("C:/Documents/ Type,count(Type).csv")
  println( "Type,Groupe_affectation" )
  zipDf.registerTempTable("elis") // 1)
  sqlContext.sql( // 2)
    """SELECT Type,count(Type),Groupe_affectation,YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(Date_de_creation, 'dd/MM/yyyy') AS TIMESTAMP))) Year
      FROM elis e
group by Type,Groupe_affectation,YEAR(TO_DATE(CAST(UNIX_TIMESTAMP(Date_de_creation, 'dd/MM/yyyy') AS TIMESTAMP)))

      """
  )
    //.show(500)
    .coalesce(1).
    write.
    format("com.databricks.spark.csv").
    option("header", "true").
    save("C:/Documents/Type_Groupe_affectation_YEAR.csv")
  println( "SparkSQL" )
  zipDf.registerTempTable("elis") // 1)
  sqlContext.sql( // 2)
    """SELECT Type,Courte_Description,count(Courte_Description),Groupe_affectation
      FROM elis e
group by Type,Courte_Description,Groupe_affectation
having count(Courte_Description)>5
      """
  )
    //.show(500)
    .coalesce(1).
    write.
    format("com.databricks.spark.csv").
    option("header", "true").
    save("C:/Documents/Type_Courte_Description_Groupe_affectation.csv")

  println( "Average City Population by State" )
  zipDf
    .groupBy("state", "city")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count")
    .groupBy("state")
    .avg("count")
    .withColumnRenamed("avg(count)", "avgCityPop")
    .show()

  // Query 3
  println( "Largest and Smallest Cities by State" )
  val popByCity = zipDf // 1)
    .groupBy("state", "city")
    .sum("pop")
    .withColumnRenamed("sum(pop)", "count")

  val minMaxCities = popByCity.join(
    popByCity
      .groupBy("state")
      .agg(max("count") as "max_pop", min("count") as "min_pop") // 2)
      .withColumnRenamed("state", "r_state"),
    $"state" === $"r_state" && ( $"count" === $"max_pop" || $"count" === $"min_pop") // 3)
  )
    .drop($"r_state")
    .drop($"max_pop")
    .drop($"min_pop") // 4)
  minMaxCities.show()


  // SparkSQL:
  println( "SparkSQL" )
  zipDf.registerTempTable("zips") // 1)
  sqlContext.sql( // 2)
    """SELECT state, sum(pop) AS count
      FROM zips
      GROUP BY state
      HAVING sum(pop) > 10000000"""
  )
    .show()

  // Aggregation pipeline integration:
  println( "Aggregation pipeline integration" )
  zipDf
    .filter($"pop" > 0)
    .show()

  println( "RDD with Aggregation pipeline" )
  val mongoRDD = sc.loadFromMongoDB(readConfig) // 1)
  mongoRDD
    .withPipeline(List( // 2)
      Document.parse("""{ $group: { _id: "$state", totalPop: { $sum: "$pop" } } }"""),
      Document.parse("""{ $match: { totalPop: { $gte: 10000000 } } }""")
    ))
    .collect()
    .foreach(println)

  // Writing data in MongoDB:
  MongoSpark
    .write(minMaxCities)
    .option("spark.mongodb.output.uri", "mongodb://127.0.0.1/test" )
    .option("collection","minMaxCities")
    .mode("overwrite")
    .save()
    */

}
