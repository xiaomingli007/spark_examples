package spark.graphx
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
//import org.apache.spark.graphx.Graph
/**
 * Created by Lee on 2014/4/18.
 */
object graphxTest {

    def main(args: Array[String]) {

        val sc = new SparkContext(args(0),
        "vertexRDD and EdgeRDD",
        System.getenv("SPARK_HOME"),
        SparkContext.jarOfClass(this.getClass()))

        val vertexArray = Array(
            (1L, ("Alice", 28)),
            (2L, ("Bob", 27)),
            (3L, ("Charlie", 65)),
            (4L, ("David", 42)),
            (5L, ("Ed", 55)),
            (6L, ("Fran", 50))
        )
        val edgeArray = Array(
        Edge(2L,1L,7),
        Edge(2L,4L,2),
        Edge(3L,2L,4),
        Edge(3L, 6L, 3),
        Edge(4L, 1L, 1),
        Edge(5L, 2L, 2),
        Edge(5L, 3L, 8),
        Edge(5L, 6L, 3)
    )
      //  val vertexRDD :RDD[(Long,(String,Int))] = sc.parallelize(vertexArray)
        val vertexRDD:RDD[(Long,(String,Int))] = sc.parallelize(vertexArray)
        val edgeRDD:RDD[Edge[Int]] = sc.parallelize(edgeArray)
        val graph:Graph[(String,Int),Int] = Graph(vertexRDD,edgeRDD)
        graph.vertices.filter{
            case (id,(name,age))=> age>30
        }.collect.foreach{
            case (id,(name,age))=>println(name+" is "+age)
        }
        graph.vertices.filter{
            vertex => vertex._2._2 > 30
        }.collect.foreach{
            vertex => println(vertex._2._1+" is "+vertex._2._2)
        }
        for (triplet <- graph.triplets.collect){
             println(triplet.srcAttr._1+" likes "+triplet.dstAttr._1)
        }

  }

}