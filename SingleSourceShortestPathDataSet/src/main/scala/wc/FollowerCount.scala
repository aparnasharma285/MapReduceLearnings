// To get single source shortest path
package wc

import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object FollowersCountMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.FollowersCountMain <source> <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Followers Count")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate

    import spark.implicits._
    var source: String = args(0)

    // read text file and create adjacency file for the graph
    val textFile = spark.read.format("csv").load(args(1)).toDF("vertex1", "vertex2")
    val graph = textFile.groupBy("vertex1").agg(collect_list("vertex2").as("adjacentNodes")).cache()

    // get initial distances
    var distances = graph.withColumn("distance", when(col("vertex1") === source, 0.0) otherwise (Double.PositiveInfinity)).drop(col("adjacentNodes"))


    var hasNotConverged = true

    // iterate till convergence is reached
    while (hasNotConverged) {

      //join graph and distances and update the distance
      var newDistances = graph.as("g").join(distances.as("d"), col("g.vertex1") === col("d.vertex1"), "rightOuter")
        .select(col("d.vertex1"), col("g.adjacentNodes"), col("d.distance"))
        .withColumn("adjacentNode", explode($"adjacentNodes")).drop(col("adjacentNodes"))
        .map { case Row(node: String, distanceOfNode: Double, adjacentNode: String) => extractVertices(adjacentNode, source, distanceOfNode) }
        .groupBy("_1").agg(min("_2").as("distance"))
        .withColumnRenamed("_1", "vertex1")

      var count = newDistances.as("a").join(distances.as("b"), col("a.vertex1") === col("b.vertex1"))
        .filter(col("a.distance") !== col("b.distance")).count()

      if (count > 0) {
        hasNotConverged = true;
      } else {
        hasNotConverged = false;
      }

      distances = newDistances
    }


    distances.show()


  }

  def extractVertices(m: String, source: String, distanceOfNode: Double): (String, Double) = {

    if (m.equals(source)) {
      return (m, 0.0)
    } else if (distanceOfNode != Double.PositiveInfinity) {
      return (m, distanceOfNode + 1)
    } else {
      return (m, Double.PositiveInfinity)
    }
  }

}


