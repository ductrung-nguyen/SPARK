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
	    /*
	    val tree = new RegressionTree2(playgolf_metadata)
	    tree.setMinSplit(1)
	    stime = System.nanoTime()
	    println(tree.buildTree(playgolf_data))
	    println("Build tree in %f second(s)".format((System.nanoTime() - stime)/1e9))
	    println("Predict:" + tree.predict("cool,sunny,normal,false,30,1".split(",")))
	    */
	    
	    //TEST WITH BODYFAT DATASET 
	    val bodyfat_data = context.textFile("data/bodyfat.csv", 1)
	    val bodyfat_metadata = context.textFile("data/bodyfat.tag", 1) // PM: this file should be small, I would broadcast it
	    
	    /*
	    val tree2 = new RegressionTree2(bodyfat_metadata.collect())
	    stime = System.nanoTime()
	    //tree2.setMinSplit(10)
	    println(tree2.buildTree(bodyfat_data, "DEXfat", Set("age", "waistcirc","hipcirc","elbowbreadth","kneebreadth")))
	    println("Build tree in %f second(s)".format((System.nanoTime() - stime)/1e9))
	    println("Predict:" + tree2.predict("53,56,29.83,81,103,6.9,8.9,4.14,4.52,4.31,5.69".split(",")))
		*/
	    
	    val tree = new RegressionTree3(bodyfat_metadata.collect())
	    tree.setMinSplit(10)
	    println(tree.buildTree(bodyfat_data, "DEXfat", Set("age", "waistcirc","hipcirc","elbowbreadth","kneebreadth")))
	    println("Predict:" + tree.predict("53,56,29.83,81,103,6.9,8.9,4.14,4.52,4.31,5.69".split(",")))
	    tree.evaluate(bodyfat_data)
	    //val tree = new RegressionTree3(playgolf_metadata.collect())
	    //tree.setMinSplit(1)
	    //println(tree.buildTree(playgolf_data))
	    //println("Predict:" + tree.predict("cool,sunny,normal,false,30,1".split(",")))
	    
	    // write model to file
	    //tree2.writeTreeToFile("/Users/michiard/work/research/github/group/tree2.model")
	    
	    
	    /*
	    // LOAD TREE MODEL FROM FILE
	    // create a new regression tree; context is any spark context
	    val tree3 = RegressionTree.loadTreeFromFile("/home/loveallufev/Documents/tree2.model")
	    
	    println(tree3)
	    // load tree model from a file
	    println("Load tree from model and use that tre to predict")
	    
	    // use loaded model to predict
	    println("Predict:" + tree3.predict("53,56,29.83,81,103,6.9,8.9,4.14,4.52,4.31,5.69".split(",")))
	    
	    // evaluate with trained dataset
	    //tree3.evaluate(bodyfat_data)
		*/
	}
}