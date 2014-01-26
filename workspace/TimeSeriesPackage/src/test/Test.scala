package test
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import machinelearning.RegressionTree

object Test {
	def main(args : Array[String]) = {
	    val context = new SparkContext("local", "SparkContext")
	    
	    val dataInputURL = "data/playgolf.csv"
	
	    val myDataFile = context.textFile(dataInputURL, 1)
	    val metadata = context.textFile("data/playgolf.tag", 1)
	
	    val tree = new RegressionTree(myDataFile, metadata, context)
	    tree.setMinSplit(1)
	    var stime = System.nanoTime()
	    println(tree.buildTree())
	    println("Build tree in %f second(s)".format((System.nanoTime() - stime)/1e9))
	    println("Predict:" + tree.predict("cool,sunny,normal,false,30,1".split(",")))
	    
	    val bodyfat_data = context.textFile("data/bodyfat.csv", 1)
	    val bodyfat_metadata = context.textFile("data/bodyfat.tag", 1)
	    
	    //var i = -1; 
	    //val training_bodyfat = bodyfat_data.keyBy(x => {i = i + 1; i}).filter(x => x._1 <50).map(x => x._2)
	    //val evaluate_bodyfat = bodyfat_data.keyBy(x => {i = i + 1; i}).filter(x => x._1 >= 50).map(x => x._2)
	    
	    val tree2 = new RegressionTree(bodyfat_data, bodyfat_metadata, context)
	    stime = System.nanoTime()
	    println(tree2.buildTree("DEXfat", Set("age", "waistcirc","hipcirc","elbowbreadth","kneebreadth")))
	    println("Build tree in %f second(s)".format((System.nanoTime() - stime)/1e9))
	    println("Predict:" + tree2.predict("53,56,29.83,81,103,6.9,8.9,4.14,4.52,4.31,5.69".split(",")))
	    // evaluate with training set
	    //tree2.evaluate(evaluate_bodyfat)
	    
	    tree2.writeTreeToFile("/home/loveallufev/Documents/tree2.model")
	    
	    
	    val tree3 = RegressionTree.loadTreeFromFile("/home/loveallufev/Documents/tree2.model")
	    println("Load tree from model and use that tre to predict")
	    println("Predict:" + RegressionTree.predict("53,56,29.83,81,103,6.9,8.9,4.14,4.52,4.31,5.69".split(",")))
	    
	}
}