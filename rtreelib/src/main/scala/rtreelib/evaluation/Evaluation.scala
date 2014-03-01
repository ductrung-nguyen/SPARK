package rtreelib.evaluation

import org.apache.spark.rdd._

/**
 * Use to evaluate a prediction
 */
object Evaluation {
    /**
     * Evaluate the accuracy of regression tree
     *
     * @param input an input record (uses the same delimiter with trained data set)
     */
    def evaluate(predictedResult: RDD[String], actualResult: RDD[String]) = {
        var newRDD = predictedResult zip actualResult
        newRDD = newRDD.filter(v => v._1 != "???") // filter invalid record, v._1 is predicted value

        val numTest = newRDD.count

        val diff = newRDD.map(x => (x._1.toDouble, x._2.toDouble)).map(x => (x._2 - x._1, (x._2 - x._1) * (x._2 - x._1)))

        val sums = diff.reduce((x, y) => (x._1 + y._1, x._2 + y._2))

        val meanDiff = sums._1 / numTest
        val meanDiffPower2 = sums._2 / numTest
        val deviation = math.sqrt(meanDiffPower2 - meanDiff * meanDiff)
        val SE = deviation / numTest

        println("Mean of different:%f\nDeviation of different:%f\nSE of different:%f".format(meanDiff, deviation, SE))

    }
}