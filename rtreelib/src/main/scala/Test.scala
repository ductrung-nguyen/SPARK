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
	    
	    
	    val IS_LOCAL = false
	    
	    
	    val inputTrainingFile = (
	        if (IS_LOCAL)
	        	"data/bodyfat.csv"
	        else
	            "hdfs://spark-master-001:8020/user/ubuntu/input/AIRLINES/2006.csv"
	    )
	    
	    val inputTestingFile = (
	        if (IS_LOCAL)
	        	"data/bodyfat.csv"
	        else
	            "hdfs://spark-master-001:8020/user/ubuntu/input/AIRLINES/2006.csv"
	    )
	    
	    val conf = (
	        if (IS_LOCAL)
	        	new SparkConf()
	        		.setMaster("local").setAppName("rtree example")
	        else
	            new SparkConf()
	        		.setMaster("spark://spark-master-001:7077")
	        		.setAppName("rtree example")
	        		.setSparkHome("/opt/spark")
	        		.setJars(List("target/scala-2.10/rtree-example_2.10-1.0.jar"))
	        		.set("spark.executor.memory", "2222m")
	    )
	    
	    val context = new SparkContext(conf)

	    var stime : Long = 0
	    
	    val trainingData = context.textFile(inputTrainingFile, 1)
	    val testingData = context.textFile(inputTestingFile, 1)


	    /* TEST BUILDING TREE */
	    
	    val tree = new RegressionTree()
	    tree.setDataset(trainingData)
	    //tree.treeBuilder = new DataMarkerTreeBuilder(tree.featureSet) // change the default tree builder
        //tree.setAttributeTypes("c,c,c,c,c,c,c,c,c,c,c")
        //tree.setAttributeNames(",age,DEXfat,waistcirc,hipcirc,elbowbreadth,kneebreadth,anthro3a,anthro3b,anthro3c,anthro4")
        tree.treeBuilder.setMinSplit(10)
        tree.treeBuilder.setMaximumParallelJobs(10)

        if (IS_LOCAL)
            println(tree.buildTree("DEXfat", Set("age", "waistcirc", "hipcirc", "elbowbreadth", "kneebreadth")))
        else
            println(tree.buildTree("ArrDelay"))
	    
	    // build tree with target feature is the last feature
	    //println(tree.buildTree()) 

	    
	    /* TEST WRITING TREE TO MODEL */
	    
	    //tree.writeModelToFile("/tmp/test.tree")
	    
	    
	    
	    /* TEST PREDICTING AND EVALUATION */
	    
	    // predict a single value
	    //println("Predict:" + tree.predict("53,56,29.83,81,103,6.9,8.9,4.14,4.52,4.31,5.69".split(",")))
	    //val predictRDD = tree.predict(testingData)
	    //val actualValueRDD = testingData.map(line => line.split(',')(2))
	    //Evaluation.evaluate(predictRDD, actualValueRDD)    
	    
	    
	    
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