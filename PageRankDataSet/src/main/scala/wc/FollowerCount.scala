// To find the page rank using dataframes
package wc

import org.apache.spark.SparkConf
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object FollowersCountMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.FollowersCountMain <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Followers Count")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder
  			     .config(conf)
                             .getOrCreate;

import spark.implicits._

// initializing variables   
var graphMap: Map[Int,Int]= Map()
var ranksMap: Map[Int,Double]= Map()
var vertexCount: Int = 0
var key: Int = 0
var value: Int = 0
var k: Int = args(0).toInt
var totalVertices = Math.pow(k,2)
var pr = 1/totalVertices


// creating graph as a map
for (b <- 1 to k) {
	vertexCount += 1
	for (a <- 1 to k){
	key = vertexCount
	if (a == k){
	value = 0
	} else{
	vertexCount += 1
	value = vertexCount
	}
graphMap += (key -> value)
}
}

//creating Ranks as a map 
ranksMap= graphMap.map(item => (item._1,pr))
ranksMap += (0 -> 0)

// converting ranks and graph map as Dataframes
val graph = graphMap.toSeq.toDF("vertex1","vertex2").persist()
var ranks = ranksMap.toSeq.toDF("vertex","rank")

// Logic for Page rank
for (iter <- 1 to 10){
var temp = graph.as("g").join(ranks.as("r"), col("g.vertex1") === col("r.vertex")).select(col("vertex2"), col("rank")).groupBy("vertex2").agg(sum("rank").as("rank"))
// getting delta
var delta = temp.filter(col("vertex2") === 0).select(col("rank")).first().getDouble(0)
var toBeDistributed = delta/vertexCount

ranks = ranks.withColumn("rank", lit(0))
ranks = ranks.union(temp).groupBy("vertex").agg(sum("rank").as("rank")).withColumn("rank", when(col("vertex") === 0, 0)otherwise(col("rank"))+toBeDistributed)

println("Sum of Page Rank "+ranks.agg(sum("rank")).first.getDouble(0))
}

// save top 100 page ranks
ranks = ranks.orderBy(desc("rank")).limit(100)
ranks.write.csv(args(1))
  }
}

