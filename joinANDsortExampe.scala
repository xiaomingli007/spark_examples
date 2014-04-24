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
        "joinVertices & outerJoinVertices and sort vertices by ID",
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

        val vertexRDD:RDD[(Long,(String,Int))] = sc.parallelize(vertexArray)
        val edgeRDD:RDD[Edge[Int]] = sc.parallelize(edgeArray)
        val graph:Graph[(String,Int),Int] = Graph(vertexRDD,edgeRDD)
        val inDegrees:VertexRDD[Int] = graph.inDegrees

        case  class User(name:String,age:Int,inDeg:Int,outDeg:Int)
        val initialUserGraph:Graph[User,Int] = graph.mapVertices{case (id,(name,age))=>User(name,age,0,0)}
        val userGraph = initialUserGraph.outerJoinVertices(initialUserGraph.inDegrees){
            case (id,u,inDegOpt) => User(u.name,u.age,inDegOpt.getOrElse(0),u.outDeg)
        }.outerJoinVertices(initialUserGraph.outDegrees){
            case (id,u,outDegOpt)=>User(u.name,u.age,u.inDeg,outDegOpt.getOrElse(0))
        }
        userGraph.vertices.collect().sortBy(_._1).foreach{
            v =>println("USer "+v._1+" is called "+v._2.name+" and is liked by "+v._2.inDeg+" people")
        }
        val degreeGraph = initialUserGraph.joinVertices(initialUserGraph.inDegrees){
            case (id,u,inDegOpt) => User(u.name,u.age,inDegOpt,u.outDeg)
        }.joinVertices(initialUserGraph.outDegrees){
            case (id,u,outDegOpt) => User(u.name,u.age,u.inDeg,outDegOpt)
        }
        userGraph.vertices.collect().sortBy(_._1).foreach{
            v =>println("User "+v._1+" is called "+v._2.name+" and is liked by "+v._2.inDeg+" people")
        }

  }

}