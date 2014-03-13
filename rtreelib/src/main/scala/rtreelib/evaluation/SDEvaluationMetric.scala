package rtreelib.evaluation

import org.apache.spark.rdd.RDD
import rtreelib.core._

/**
 * This class use standard deviation and square error to determine the accuracy of a prediction
 */
class SDEvaluationMetric extends BaseEvaluation {
    /**
     * Evaluate the accuracy of a prediction
     * 
     * @param predictedResult the predicted values
     * @param actualResult the actual result
     */
	override def Evaluate(predictedResult: RDD[String], actualResult: RDD[String]) = {
	    var newRDD = predictedResult zip actualResult
        newRDD = newRDD.filter(v => (v._1 != "???" 
            && (Utility.parseDouble(v._2) match {
                case Some(d :Double) => true 
                case None => false 
        }))) // filter invalid record, v._1 is predicted value
        
        var invalidRDD = newRDD.filter(v => (v._1 == "???" 
            || !(Utility.parseDouble(v._2) match {
                case Some(d :Double) => true 
                case None => false 
        }))) // filter invalid record, v._1 is predicted value

        val numTest = newRDD.count

        val diff = newRDD.map(x => (x._1.toDouble, x._2.toDouble)).map(x => (x._2 - x._1, (x._2 - x._1) * (x._2 - x._1)))

        val sums = diff.reduce((x, y) => (x._1 + y._1, x._2 + y._2))

        val meanDiff = sums._1 / numTest
        val meanDiffPower2 = sums._2 / numTest
        val deviation = math.sqrt(meanDiffPower2 - meanDiff * meanDiff)
        val SE = deviation / numTest

        println("Mean of different:%f\nDeviation of different:%f\nSE of different:%f".format(meanDiff, deviation, SE))
	    println("Number of invalid records:" + invalidRDD.count)

	}
}