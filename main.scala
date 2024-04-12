package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}
import scala.util.Random

object main{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)


def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
  var g = g_in.mapVertices((id, _) => (false, false, 0.0)) // (inMIS, deactivated, bid)
  var iterations = 0
  while (g.vertices.filter(!_._2._2).count() > 0) {
    val activeVertexCount = g.vertices.filter(!_._2._2).count()
    println("====================")
    println(s"Number of active vertices at iteration $iterations: $activeVertexCount")
    println("====================")
    g = g.mapVertices((id, attr) => {
      if (!attr._2) {
        val bid = scala.util.Random.nextDouble()
        (attr._1, attr._2, bid)
      } else {
        attr
      }
    })

    val messages: VertexRDD[Double] = g.aggregateMessages[Double](
      triplet => {
        if (!triplet.srcAttr._2) {
          triplet.sendToDst(triplet.srcAttr._3)
          triplet.sendToSrc(triplet.dstAttr._3)
        }
      },
      (a, b) => math.max(a, b) // Keep the highest bid
    )

    g = g.outerJoinVertices(messages) {
      (id, attr, maxBid) => {
        maxBid match {
          case Some(bid) if attr._3 > bid => (true, true, attr._3) // Add to MIS
          case _ => (attr._1, attr._2, attr._3) // Otherwise, no change
        }
      }
    }
     iterations += 1
    g = g.outerJoinVertices(g.triplets.flatMap(triplet => {
      if (triplet.srcAttr._1) Iterator((triplet.dstId, true))
      else if (triplet.dstAttr._1) Iterator((triplet.srcId, true))
      else Iterator.empty
    }).distinct) {
      (id, attr, deactivate) => {
        deactivate match {
          case Some(_) => (attr._1, true, attr._3) // Deactivate
          case None => attr // No change
        }
      }
    }
  }
  println("====================")
  println("Number of Iterations = " + iterations)
  println("====================")
  // Map the vertices to either 1 (in MIS) or -1 (not in MIS)
  val finalGraph = g.mapVertices((id, attr) => if (attr._1) 1 else -1)
  //finalGraph.vertices.collect.foreach {
   // case (vertexId, misValue) => println(s"Vertex: $vertexId, MIS Value: $misValue")
  //}
  finalGraph
}

  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    for ((vertexId, label) <- g_in.vertices.collect()){
      if (label == 1){
        val neighbors = g_in.collectNeighbors(EdgeDirection.Out).lookup(vertexId).flatten
        for((neighborID, neighborlabel) <- neighbors){
          if (neighborlabel == 1){
            return false
          }
        }
      }
    }
    return true
  }
  


  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("project_3")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
/* You can either use sc or spark */

    if(args.length == 0) {
      println("Usage: project_3 option = {compute, verify}")
      sys.exit(1)
    }
    if(args(0)=="compute") {
      if(args.length != 3) {
        println("Usage: project_3 compute graph_path output_path")
        sys.exit(1)
      }
      val startTimeMillis = System.currentTimeMillis()
      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val g = Graph.fromEdges[Int, Int](edges, 0, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)
      val g2 = LubyMIS(g)

      val endTimeMillis = System.currentTimeMillis()
      val durationSeconds = (endTimeMillis - startTimeMillis) / 1000
      println("==================================")
      println("Luby's algorithm completed in " + durationSeconds + "s.")
      println("==================================")

      val g2df = spark.createDataFrame(g2.vertices)
      g2df.coalesce(1).write.format("csv").mode("overwrite").save(args(2))
    }
    else if(args(0)=="verify") {
      if(args.length != 3) {
        println("Usage: project_3 verify graph_path MIS_path")
        sys.exit(1)
      }

      val edges = sc.textFile(args(1)).map(line => {val x = line.split(","); Edge(x(0).toLong, x(1).toLong , 1)} )
      val vertices = sc.textFile(args(2)).map(line => {val x = line.split(","); (x(0).toLong, x(1).toInt) })
      val g = Graph[Int, Int](vertices, edges, edgeStorageLevel = StorageLevel.MEMORY_AND_DISK, vertexStorageLevel = StorageLevel.MEMORY_AND_DISK)

      val ans = verifyMIS(g)
      if(ans){
        println("==================================")
        println("Yes")
        println("==================================")
      }
      else{
        println("==================================")
        println("No")
        println("==================================")
      }
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
