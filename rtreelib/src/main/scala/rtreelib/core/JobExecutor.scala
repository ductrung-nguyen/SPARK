package rtreelib.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._

/**
 * The information of a job, which will find the best feature and the best split point of this feature to split
 * @param id The unique id of jobs
 * @param xConditions The list of condition , which will be used to get the 'right dataset' from the whole dataset
 */
class JobInfo(id: BigInt, xConditions: List[Condition]) extends Serializable {

    /**
     *  unique ID of this job
     */
    var ID = id;

    /**
     *  condition of input dataset for this job
     */
    var conditions_of_input_dataset = xConditions;

    /**
     *  the node which associate to this job can not expand more ?
     */
    var isStopNode = true;

    /**
     *  success or error
     */
    var isSuccess = false;

    /**
     *  error message if job fail
     */
    var errorMessage = ""

    /**
     *  best splitpoint (result of this job)
     */
    var splitPoint = new SplitPoint(-9, 0, 0); // -9 is only a mock value

    /**
     * Constructor
     */
    def this() = this(0, List[Condition]())

    /**
     * Convert this instance into string
     */
    override def toString() = ("Job ID=" + ID.toString + " condition = " + xConditions.toString
        + " splitpoint:" + splitPoint.toString + "\n")
}

/**
 * This class will launch a job and try to find the best attribute and the best split point for this attribute
 * and submit new job to continue to expand node (if any)
 *
 * @param job The information of job which will be launched
 * @param inputdata The whole data set
 * @param caller The ThreadTreeBuilder instance, which called instance of this class
 */
class JobExecutor(job: JobInfo, inputData: RDD[Array[FeatureValueAggregate]], 
    caller: ThreadTreeBuilder)
    extends Serializable with Runnable {

    /**
     * Check a sub data set has meet stop criterion or not
     *
     * @param data data set
     * @return <code>true</code>/<code>false</code> and the average of value of target feature
     */
    def checkStopCriterion(data: RDD[FeatureValueAggregate]): (Boolean, Double) = { //PM: since it operates on RDD it is parallel

        val yFeature = data.filter(x => x.index == caller.yIndex)

        //yFeature.collect.foreach(println)

        val sum = yFeature.reduce(_ + _)

        val numTotalRecs = sum.frequency
        val sumOfYValue = sum.yValue
        val sumOfYValuePower2 = sum.yValuePower2

        val meanY = sumOfYValue / numTotalRecs
        val meanOfYPower2 = sumOfYValuePower2 / numTotalRecs
        val standardDeviation = math.sqrt(meanOfYPower2 - meanY * meanY)

        // return tuple (isStop, meanY)
        (( // the first component of tuple
            (numTotalRecs <= caller.minsplit) // or the number of records is less than minimum
            || (((standardDeviation < caller.threshold) && (meanY == 0)) || (standardDeviation / meanY < caller.threshold)) // or standard devariance of values of Y feature is small enough
            ),
            meanY // the second component of tuple
            )
    }

    /**
     * This function will be call when the parent thread start
     */
    @Override
    def run() {
        try {
            println("job executor starts")
            var data = inputData.filter(
                x => job.conditions_of_input_dataset.forall(
                    sp => {
                        sp.check(x(sp.splitPoint.index).xValue)
                    })).flatMap(x => x.toSeq)

            data = data.filter(f => f.index >= 0)

            println("after checking stop condition")
            val (stopExpand, eY) = checkStopCriterion(data)
            if (stopExpand) {

                var splittingPointFeature = new SplitPoint(-1, eY, 1) // splitpoint for left node
                job.splitPoint = splittingPointFeature
                job.isSuccess = true;
                caller.addJobToFinishedQueue(job)
            } else {
                val ndata = data.map(x => (x.index, x)).reduceByKey((x,y) => x + y)
                ndata.foreach(println)
                println("Result of checking stop condition:(" + stopExpand + " - " + eY + ")")
                val groupFeatureByIndexAndValue = ndata.map(f => ((f._1, f._2.xValue), f._2))
                //val groupFeatureByIndexAndValue = data.keyBy(f => (f.index, f.xValue))
                    .groupByKey(20) // PM: this operates on an RDD => in parallel

                println("after group feature by index and value" + groupFeatureByIndexAndValue.count)
                var featureValueSorted = (
                    //data.groupBy(x => (x.index, x.xValue))
                    groupFeatureByIndexAndValue // PM: this is an RDD hence you do the map and fold in parallel (in MapReduce this would be the "reducer")

                    .map(x => x._2.reduce((f1, f2) => f1 + f2)))

                var splitPointOfEachFeature = caller.xIndexes.toList.map(index => {
                    var valuesOfFeature = featureValueSorted.filter(_.index == index)

                    caller.featureSet.data(index) match {
                        case n: NumericalFeature => {
                          findBestSplitPointForNumericalFeature(index, valuesOfFeature)
                        }

                        case c: CategoricalFeature => {
                          findBestSplitPointForCategoricalFeature(index, valuesOfFeature)
                        }
                    }
                })

                val splittingPointFeature = splitPointOfEachFeature.filter(_.index != caller.yIndex).maxBy(_.weight)
                println("after finding best split point")
                if (splittingPointFeature.index < 0) {
                    splittingPointFeature.point = eY
                    job.splitPoint = splittingPointFeature
                    job.isSuccess = true
                    if (splittingPointFeature.index == -9) // this is unused feature, so ignore it when updating model
                        job.isStopNode = false

                    caller.addJobToFinishedQueue(job)
                } else {
                    //val chosenFeatureInfo = caller.featureSet.data.filter(f => f.index == splittingPointFeature.index).take(0)

                    val leftCondition = job.conditions_of_input_dataset :+ new Condition(splittingPointFeature, true)
                    val rightCondition = job.conditions_of_input_dataset :+ new Condition(splittingPointFeature, false)
                    val leftJob = new JobInfo(job.ID << 1, leftCondition)
                    val rightJob = new JobInfo((job.ID << 1) + 1, rightCondition)

                    job.splitPoint = splittingPointFeature
                    job.isStopNode = false
                    job.isSuccess = true
                    caller.addJobToFinishedQueue(job)

                    caller.addJobToExpandingQueue(leftJob)
                    caller.addJobToExpandingQueue(rightJob)

                } // end of if index == -1
            }

        } catch {
            case e: Exception => {
                job.errorMessage = e.getMessage()
                job.isSuccess = false;
                caller.addJobToFinishedQueue(job)
                e.printStackTrace()
            }
        }

    }
  
  private def findBestSplitPointForNumericalFeature(index: Int, feature: org.apache.spark.rdd.RDD[rtreelib.core.FeatureValueAggregate]): rtreelib.core.SplitPoint = {
      val sortedValueFeature = feature.keyBy(v => v.xValue.asInstanceOf[Double]).sortByKey(true, 20)
      var allValues = sortedValueFeature.map(x => x._2).collect
      var acc: Int = 0 // number of records on the left of the current element
      var currentSumY: Double = 0
      //val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency)
      //val sumY = x._2.foldLeft(0.0)(_ + _.yValue)
      var temp = allValues.reduce((f1, f2) => f1 + f2)
      val numRecs = temp.frequency
      val sumY = temp.yValue

      var posibleSplitPoint: Double = 0
      var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0, 0)

      var bestSplitPoint = new SplitPoint(index, posibleSplitPoint, 0)
      var maxWeight = Double.MinValue
      var currentWeight: Double = 0

      println("before mapping to get all possible splitpoints")
      if (allValues.length == 1) {
          new SplitPoint(-1, 0.0, 0.0) // sign of stop node
      } else {
          allValues.view.foreach(f => {

              if (lastFeatureValue.index == -1) {
                  lastFeatureValue = f
              } else {
                  posibleSplitPoint = (f.xValue.asInstanceOf[Double] + lastFeatureValue.xValue.asInstanceOf[Double]) / 2;
                  currentSumY = currentSumY + lastFeatureValue.yValue
                  acc = acc + lastFeatureValue.frequency
                  currentWeight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                  lastFeatureValue = f
                  if (currentWeight > maxWeight) {
                      bestSplitPoint.point = posibleSplitPoint
                      bestSplitPoint.weight = currentWeight
                      maxWeight = currentWeight
                  }
              }
          })
          bestSplitPoint
      }
    }
  
  private def findBestSplitPointForCategoricalFeature(index: Int, feature: org.apache.spark.rdd.RDD[rtreelib.core.FeatureValueAggregate]): rtreelib.core.SplitPoint = {
      val sortedValueFeature = feature.keyBy(v => v.yValue / v.frequency).sortByKey(true, 20)
      val allValues = sortedValueFeature.map(x => x._2).collect
      if (allValues.length == 1) {
          new SplitPoint(-1, 0.0, 0.0) // sign of stop node
      } else {

          var currentSumY: Double = 0 // current sum of Y of elements on the left of current feature
          var temp = allValues.reduce((f1, f2) => f1 + f2)
          val numRecs = temp.frequency
          val sumY = temp.yValue
          var splitPointIndex: Int = 0
          var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0, 0)
          var acc: Int = 0
          println("before mapping to get all possible splitpoints")
          var bestSplitPoint = new SplitPoint(index, splitPointIndex, 0)
          var maxWeight = Double.MinValue
          var currentWeight: Double = 0

          allValues.view.foreach(f => {

              if (lastFeatureValue.index == -1) {
                  lastFeatureValue = f
              } else {
                  currentSumY = currentSumY + lastFeatureValue.yValue
                  splitPointIndex = splitPointIndex + 1
                  acc = acc + lastFeatureValue.frequency
                  currentWeight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                  lastFeatureValue = f
                  if (currentWeight > maxWeight) {
                      bestSplitPoint.point = splitPointIndex
                      bestSplitPoint.weight = currentWeight
                      maxWeight = currentWeight
                  }
              }
          })

          var splitPointValue = allValues.map(f => f.xValue).take(splitPointIndex).toSet
          bestSplitPoint.point = splitPointValue
          bestSplitPoint
      }
    }

}
