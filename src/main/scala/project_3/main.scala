package project_3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark.storage.StorageLevel

object main{

  def LubyMIS(g_in: Graph[Int, Int]): Graph[Int, Int] = {
    var remaining_vertices = 2L;
    var iter = 0;
    var g = g_in.mapVertices((id, x) => (0, scala.util.Random.nextInt(Integer.MAX_VALUE)))
//    var g = g_in
    g.cache()
    while (remaining_vertices >= 1) {
      val v = g.aggregateMessages[Int](e => {
        if (e.srcAttr._1 == 0 && e.dstAttr._1 == 0) {
          e.sendToDst(e.srcAttr._2)
          e.sendToSrc(e.dstAttr._2)
        }
      }, scala.math.max(_, _))
      val g2 = g.outerJoinVertices(v) {
        (vid, data, maxNeighborBid) => (data._1, if (data._2 > maxNeighborBid.getOrElse(0)) 1 else 0)
      }

      val v2 = g2.aggregateMessages[Int](e => {
        if (e.srcAttr._1 == 0 && e.dstAttr._1 == 0) {
          e.sendToDst(e.srcAttr._2)
          e.sendToSrc(e.dstAttr._2)
        }
      }, scala.math.max(_, _))

      val g3 = g2.outerJoinVertices(v2) {
        (vid, data, anyNeighborWin) => {
          if (data._1 != 0)
            data._1
          else if (data._2 == 1)
            1
          else if (anyNeighborWin.getOrElse(0) == 1)
            -1
          else
            0
        }
      }

      g = g3.mapVertices({ case (id, x) => (x, scala.util.Random.nextInt(Integer.MAX_VALUE)) })
      g.cache()

      remaining_vertices = g.vertices.filter({ case (id, x) => (x._1 == 0) }).count()
      iter += 1
      println("end of iter: " + iter + ". remaining vertices:" + remaining_vertices)
    }
    println("#iterations: " + iter)
    return g.mapVertices({ case (id, x) => (x._1) })
  }


  def verifyMIS(g_in: Graph[Int, Int]): Boolean = {
    if(g_in.vertices.filter({case (id,x) => (x == 1 || x == -1)}).count < g_in.vertices.count )
      return false

    val v = g_in.aggregateMessages[(Int, Int)](e => {
        e.sendToDst((e.srcAttr, e.srcAttr))
        e.sendToSrc((e.dstAttr, e.dstAttr))
    }, (x,y) => (scala.math.max(x._1,y._1), scala.math.min(x._2,y._2)))

    val g2 = g_in.outerJoinVertices(v) {
      (vid, self, maxMin) => {
        if (self == 1) {
          if(maxMin.getOrElse((-1,-1))._1 == -1)
            1
          else
            0
        }
        else {
          if(maxMin.getOrElse((-1,-1))._1 == 1)
            1
          else
            0
        }
      }
    }
    if(g2.vertices.filter({case (id,x) => x == 0}).count >= 1 )
      return false
    else
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
      if(ans)
        println(" =========================Yes=====================")
      else
        println("====================No===============================")
    }
    else
    {
        println("Usage: project_3 option = {compute, verify}")
        sys.exit(1)
    }
  }
}
