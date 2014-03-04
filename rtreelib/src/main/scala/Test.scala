//package main.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import rtreelib._
import rtreelib.core.RegressionTree
import rtreelib.evaluation.Evaluation
import rtreelib.core.DataMarkerTreeBuilder
import rtreelib.core.FeatureSet

object Test {
	def main(args : Array[String]) = {
	    
	    
	    val IS_LOCAL = true
	    
	    
	    val inputFile = (
	        if (IS_LOCAL)
	        	"data/bodyfat.csv"
	        else
	            "hdfs://spark-master-001:8020/user/ubuntu/input/bus.txt"
	    )
	    
	    val context = ( 
	            if (IS_LOCAL)
	                new SparkContext("local", "rtree example")
	            else
	                new SparkContext("spark://spark-master-001:7077", "rtree example", "/opt/spark/", List("target/scala-2.9.3/rtree-example_2.9.3-1.0.jar"))
	    )

	    var stime : Long = 0
	    
	    val trainingData = context.textFile(inputFile, 1)
	    val testingData = context.textFile(inputFile, 1)


	    /* TEST BUILDING TREE */
	    
	    val tree = new RegressionTree()
	    tree.setDataset(trainingData)
	    tree.treeBuilder = new DataMarkerTreeBuilder(tree.featureSet)
	    //tree.setAttributeTypes("c,c,c,c,c,c,c,c,c,c,c")
	    //tree.setAttributeNames(",age,DEXfat,waistcirc,hipcirc,elbowbreadth,kneebreadth,anthro3a,anthro3b,anthro3c,anthro4")
	    tree.treeBuilder.setMinSplit(10)
	    tree.treeBuilder.setMaximumParallelJobs(10)
	    println(tree.buildTree("DEXfat", Set("age", "waistcirc","hipcirc","elbowbreadth","kneebreadth")))
	    // build tree with target feature is the last feature
	    //println(tree.buildTree()) 

	    
	    /* TEST WRITING TREE TO MODEL */
	    
	    //tree.writeModelToFile("/tmp/test.tree")
	    
	    
	    
	    /* TEST PREDICTING AND EVALUATION */
	    
	    // predict a single value
	    //println("Predict:" + tree.predict("53,56,29.83,81,103,6.9,8.9,4.14,4.52,4.31,5.69".split(",")))
	    val predictRDD = tree.predict(testingData)
	    val actualValueRDD = testingData.map(line => line.split(',')(2))
	    Evaluation.evaluate(predictRDD, actualValueRDD)    
	    
	    
	    
	    /* TEST RECOVER MODE */
	    
	    //val recoverTree = new RegressionTree()
	    //recoverTree.treeBuilder = new DataMarkerTreeBuilder(new FeatureSet(Array[String]()))
	    //recoverTree.continueFromIncompleteModel(bodyfat_data, "/tmp/model.temp3")	// temporary model file
	    //println("Model after re-run from the last state:\n" + recoverTree.treeModel)
	    
	    
	    /* TEST LOADING TREE FROM MODEL FILE */
	    
	    //val tree2 = new RegressionTree();
	    //tree2.loadModelFromFile("/tmp/test.tree")
	    //tree2.evaluate(bodyfat_data)
	    
	}
}