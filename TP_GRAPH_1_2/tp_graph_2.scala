import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD



sc.setLogLevel("ERROR")
val file = "/home/poulain/Documents/Cours DAC/BDLE/TP6/facebook_edges_prop.csv"

val lines = sc.textFile(file, 4)
val edgesList= lines.map{s=> val parts = s.split(",")
Edge(parts(0).toLong, parts(1).toLong, (parts(2), parts(3).toInt)) }.distinct()
var lines = sc.textFile("/home/poulain/Documents/Cours DAC/BDLE/TP6/facebook_users_prop.csv", 4)
val vertexList = lines.map{s=> val parts = s.split(",")
(parts(0).toLong, (parts(1), parts(2), parts(3).toInt))
}.distinct()
val graph = Graph.apply(vertexList, edgesList)

#Afficher avec Kendall nom & age
graph.vertices.filter{case(id,(p,n,a)) => p=="Kendall"}.collect.foreach(u=>println(u._1+" "+u._2._2+" "+u._2._3))

2058 Brewbaker 49


#Afficher les prénoms des amis de Kendall, On considère le graph non dirigé
graph.triplets.filter{t =>t.srcAttr._1 =="Kendall" || t.dstAttr._1 == "Kendall"}.map(t=>if(t.srcAttr._1=="Kendall") t.dstAttr._1 else
t.srcAttr._1).collect.foreach(u=>println(u))

graph.triplets.filter{t=> t.dstAttr._1 == "Kendall" && t.attr._2>=70 && t.attr._1=="colleague"}.map(t=>t.srcAttr._1).collect.foreach(u=>println(u))

graph.triplets.filter{t=> t.attr._2>=80}.map(t=>t.attr._1).collect.foreach(u=>println(u))

graph.triplets.filter(t=>t.srcID._1)
