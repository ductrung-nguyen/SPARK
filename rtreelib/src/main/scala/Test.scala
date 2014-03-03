//package main.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import rtreelib._
import rtreelib.core.RegressionTree
import rtreelib.evaluation.Evaluation

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
	    
	    //TEST WITH BODYFAT DATASET 
	    val bodyfat_data = context.textFile("data/bodyfat.csv", 1)

	    
	    val tree = new RegressionTree()
	    tree.setDataset(bodyfat_data)
	    //tree.setAttributeTypes("c,c,c,c,c,c,c,c,c,c,c")
	    //tree.setAttributeNames(",age,DEXfat,waistcirc,hipcirc,elbowbreadth,kneebreadth,anthro3a,anthro3b,anthro3c,anthro4")
	    tree.treeBuilder.setMinSplit(10)
	    tree.treeBuilder.setMaximumParallelJobs(10)
	    println(tree.buildTree("DEXfat", Set("age", "waistcirc","hipcirc","elbowbreadth","kneebreadth")))
	    //println(tree.buildTree())
	    //println("Predict:" + tree.predict("53,56,29.83,81,103,6.9,8.9,4.14,4.52,4.31,5.69".split(",")))
	    //tree.writeModelToFile("/tmp/test.tree")
	    val predictRDD = tree.predict(bodyfat_data)
	    val actualValueRDD = bodyfat_data.map(line => line.split(',')(2))
	    Evaluation.evaluate(predictRDD, actualValueRDD)    
	    
	    /* TEST RECOVER MODE
	    val recoverTree = new RegressionTree()
	    println("Model after re-run from the last state:\n" +
	            recoverTree.continueFromIncompleteModel(bodyfat_data, "/tmp/model.temp2"))
	    */
	    //val tree2 = new RegressionTree();
	    //tree2.loadModelFromFile("/tmp/test.tree")
	    //tree2.evaluate(bodyfat_data)
	    
	}
}