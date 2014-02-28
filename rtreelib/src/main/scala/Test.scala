//package main.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import rtreelib._

object Test {
	def main(args : Array[String]) = {
//		val inputFile = "hdfs://spark-master-001:8020/user/ubuntu/input/bus.txt"
//		val outputDir = "hdfs://spark-master-001:8020/user/ubuntu/output/testing"
	    
//		val context = new SparkContext("spark://spark-master-001:7077", "rtree example", "/opt/spark/", List("target/scala-2.9.3/rtree-example_2.9.3-1.0.jar"))
		//val context = new SparkContext("local", "rtree example", "/opt/spark/", List("target/scala-2.9.3/rtree-example_2.9.3-1.0.jar"))
		
	    // local mode
	    val context = new SparkContext("local", "rtree example");
	    
	    // cluster mode
	    //val context = new SparkContext("local", "rtree example", "/opt/spark/", List("target/scala-2.9.3/rtree-example_2.9.3-1.0.jar"))
	    
	    var stime : Long = 0
	    
	    // TEST WITH PLAYGOLF DATASET 
	    val dataInputURL = "data/playgolf.csv"
	    val playgolf_data = context.textFile(dataInputURL, 1)
	    val playgolf_metadata = context.textFile("data/playgolf.tag", 1)
	    
	    //TEST WITH BODYFAT DATASET 
	    val bodyfat_data = context.textFile("data/bodyfat.csv", 1)
	    val bodyfat_metadata = context.textFile("data/bodyfat.tag", 1) // PM: this file should be small, I would broadcast it
	    
	    
	    val tree = new RegressionTree()
	    tree.setDataset(bodyfat_data)
	    //tree.setAttributeTypes("c,c,c,c,c,c,c,c,c,c,c")
	    //tree.setAttributeNames(",age,DEXfat,waistcirc,hipcirc,elbowbreadth,kneebreadth,anthro3a,anthro3b,anthro3c,anthro4")
	    tree.treeBuilder.setMinSplit(10)
	    tree.treeBuilder.setMaximumParallelJobs(10)
	    println(tree.buildTree("DEXfat", Set("age", "waistcirc","hipcirc","elbowbreadth","kneebreadth")))
	    println("Predict:" + tree.predict("53,56,29.83,81,103,6.9,8.9,4.14,4.52,4.31,5.69".split(",")))
	    tree.writeModelToFile("/tmp/test.tree")
	    
	    //val tree2 = new RegressionTree();
	    //tree2.loadModelFromFile("/tmp/test.tree")
	    //tree2.evaluate(bodyfat_data)
	    
	}
}