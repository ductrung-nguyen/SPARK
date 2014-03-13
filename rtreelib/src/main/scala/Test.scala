//package main.scala

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import rtreelib._
import rtreelib.core.RegressionTree
import rtreelib.evaluation.Evaluation
import rtreelib.core._

object Test {
	def main(args : Array[String]) = {
	    
	    
	    val IS_LOCAL = true
	    
	    
	    val inputTrainingFile = (
	        if (IS_LOCAL)
	        	"data/training-bodyfat.csv"
	        else
	            "hdfs://spark-master-001:8020/user/ubuntu/input/AIRLINES/2006.csv"
	    )
	    
	    val inputTestingFile = (
	        if (IS_LOCAL)
	        	"data/testing-bodyfat.csv"
	        else
	            "hdfs://spark-master-001:8020/user/ubuntu/input/AIRLINES/2007.csv"
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
	    
	    val pathOfTreeModel = "/tmp/tree.model"


	    /* TEST BUILDING TREE */
	    
	    val tree = new RegressionTree()
	    tree.setDataset(trainingData)
	    //tree.treeBuilder = new DataMarkerTreeBuilder(tree.featureSet) // change the default tree builder

        if (IS_LOCAL){
            tree.treeBuilder.setMinSplit(10)

            stime = System.nanoTime()
            println(tree.buildTree("DEXfat", Set("age", "waistcirc", "hipcirc", "elbowbreadth", "kneebreadth")))
            println("\nOK: Build tree in %f second(s)".format((System.nanoTime() - stime)/1e9))
            
            /* TEST WRITING TREE TO MODEL */
            tree.writeModelToFile(pathOfTreeModel)
            
            /* TEST LOADING TREE FROM MODEL FILE */
            val treeFromFile = new RegressionTree()
            try{
            	treeFromFile.loadModelFromFile(pathOfTreeModel)
            	println("OK: Load tree from '%s' successfully".format(pathOfTreeModel))
            }catch {
                case e: Throwable => {
                    println("ERROR: Couldn't load tree from '%s'".format(pathOfTreeModel))
                	e.printStackTrace()
                }
            }
            
            /* TEST PREDICTING AND EVALUATION */
            println("Evaluation:")
            // testing data must have the same format with the training data, and don't include header !!!
            val predictRDD = treeFromFile.predict(testingData)	
            val actualValueRDD = testingData.map(line => line.split(',')(2))	// 2 is the index of DEXfat in csv file, based 0
            Evaluation.evaluate(predictRDD, actualValueRDD)
            
            
            /* TEST RECOVER MODE */
		    //val recoverTree = new RegressionTree()
		    //recoverTree.treeBuilder = new DataMarkerTreeBuilder(new FeatureSet(Array[String]()))
		    //recoverTree.continueFromIncompleteModel(bodyfat_data, "/tmp/model.temp3")	// temporary model file
		    //println("Model after re-run from the last state:\n" + recoverTree.treeModel)
            
        }
        else{
            tree.treeBuilder.setMinSplit(1000)
            tree.treeBuilder.setThreshold(0.3) // coefficient of variation
            
            /* TEST BUILDING */
            stime = System.nanoTime()
            println(tree.buildTree("ArrDelay", 
                    Set(as.String("Month"), as.String("DayofMonth"), as.String("DayOfWeek"), "DepTime", "ArrTime", 
                            "UniqueCarrier", "Origin", "Dest", "Distance")))
            println("\nBuild tree in %f second(s)".format((System.nanoTime() - stime)/1e9))
            
            /* TEST WRITING TREE TO MODEL */
            tree.writeModelToFile(pathOfTreeModel)
            
            /* TEST LOADING TREE FROM MODEL FILE */
            val treeFromFile = new RegressionTree()
            try{
            	treeFromFile.loadModelFromFile(pathOfTreeModel)
            	println("OK: Load tree from '%s' successfully".format(pathOfTreeModel))
            }catch {
                case e: Throwable => {
                    println("ERROR: Couldn't load tree from '%s'".format(pathOfTreeModel))
                	e.printStackTrace()
                }
            }
            
            /* TEST PREDICTING AND EVALUATION */
            println("Evaluation:")
            val predictRDD = treeFromFile.predict(testingData)
            val actualValueRDD = testingData.map(line => line.split(',')(14))	// 14 is the index of ArrDelay in csv file, based 0
            Evaluation.evaluate(predictRDD, actualValueRDD)
            
            
            /* TEST RECOVER MODE */
		    //val recoverTree = new RegressionTree()
		    //recoverTree.treeBuilder = new DataMarkerTreeBuilder(new FeatureSet(Array[String]()))
		    //recoverTree.continueFromIncompleteModel(bodyfat_data, "/tmp/model.temp3")	// temporary model file
		    //println("Model after re-run from the last state:\n" + recoverTree.treeModel)
        }
	    
	}
}