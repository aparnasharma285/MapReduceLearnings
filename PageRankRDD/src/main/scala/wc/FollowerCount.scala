// To calculate page rank using RDD
package wc

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.LogManager
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
object FollowersCountMain {
  
  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.FollowersCountMain <input dir> <output dir>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Followers Count")
    val sc = new SparkContext(conf)

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

// converting ranks and graph map as RDD
val hashPartitioner = new HashPartitioner(3)
val graph = sc.parallelize(graphMap.toSeq).partitionBy(hashPartitioner).cache()
var ranks = sc.parallelize(ranksMap.toSeq).partitionBy(hashPartitioner)

// logic for Page Rank
for (iter <- 1 to 10){
var temp = graph.join(ranks).values.reduceByKey(_+_)

// retrieving delta from the dummy node 0 and equally distribute it to all nodes
var delta = temp.lookup(0)(0)
var toBeDistributed = delta/vertexCount
ranks = ranks.mapValues(x => 0)

var temp2 = ranks.union(temp).reduceByKey(_+_)
ranks = temp2.map{case (vertex, rank)  => if (vertex == 0) (vertex,0) else (vertex, rank + toBeDistributed)}

println("Sum of Page Ranks " + ranks.map(_._2).sum())
}

// save only 100 page ranks
ranks = ranks.filter({case (key, value) => key  <= 100})
ranks.saveAsTextFile(args(1))
  }
}

