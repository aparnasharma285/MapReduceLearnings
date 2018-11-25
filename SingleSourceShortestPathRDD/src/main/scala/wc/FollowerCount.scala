// To get single source shortest path
package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.HashPartitioner

object FollowersCountMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.FollowersCountMain <source> <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Followers Count")
    val sc = new SparkContext(conf)
    var source: String = args(0)

    // read text file and create adjacency file for the graph
    val textFile = sc.textFile(args(1))
    val hashPartitioner = new HashPartitioner(3)
    val graph = textFile.map(line => (line.split(",")(0), line.split(",")(1))).groupByKey().mapValues(_.toList).partitionBy(hashPartitioner).cache() // split line by comma
    // get initial distances
    var distances = graph.map { case (node, adjList) => (node, hasSourceFlag(source, node) match {
      case true => 0
      case _ => Double.PositiveInfinity
    })
    }

    var hasNotConverged = true

    // iterate till convergence is reached
    while (hasNotConverged) {

      var newDistances = graph.rightOuterJoin(distances)
        .flatMap { case (node, (adjList, distanceOfNode)) => extractVertices(node, adjList.getOrElse(List[String]()), source, distanceOfNode) }
        .reduceByKey((a, b) => Math.min(a, b))

      val count = newDistances.join(distances).collect {
        case (k, (v1, v2)) if v1 != v2 => (k, v1)
      }.count();
      if (count > 0) {
        hasNotConverged = true;
      } else {
        hasNotConverged = false;
      }
      distances = newDistances
    }
    // distances.foreach(println)
    distances.saveAsTextFile(args(2))
  }

  // check if the node is source or not
  def hasSourceFlag(source: String, node: String): Boolean = {
    return node.equals(source)
  }

  // emit new distances for all vertices in the adjacency list
  def extractVertices(n: String, adjList: List[String], source: String, distanceOfNode: Double): List[(String, Double)] = {
    var result: List[(String, Double)] = List((n, distanceOfNode))
    if (adjList.nonEmpty) {
      for (m <- adjList) {
        if (m.equals(source)) {
          result = result :+ ((m, 0.0))
        } else if (distanceOfNode != Double.PositiveInfinity) {
          result = result :+ ((m, distanceOfNode + 1))
        } else {
          result = result :+ ((m, Double.PositiveInfinity))
        }
      }
    }
    return result
  }
}


