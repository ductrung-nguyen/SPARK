package spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
/*
class RegressionTree {

    val context = new SparkContext("local", "SparkContext")
    val dataInputURL = "/home/loveallufev/semester_project/input/small_input2"
    var featureSet = new FeatureSet("/home/loveallufev/semester_project/input/tag_small_input2", context)

    val myDataFile = context.textFile(dataInputURL, 1)
    var myDataFile2 = scala.io.Source.fromFile(dataInputURL).getLines.toStream

    var mydata = myDataFile2.map(line => line.split(","))
    val number_of_features = mydata(0).length

    if (number_of_features >= 2) println(BuildTree(mydata))
    else println("Need more than 1 feature")

    def BuildTree(data: Stream[Array[String]]): Node = {
        var i = 0;
        featureSet.data.map(x => x.clear)

        data.foreach(line => processLine(line))
        
        val yFeatureInfo = featureSet.data(number_of_features - 1)
        val yPossibleSplitPoints = yFeatureInfo.getPossibleSplitPoints
        if (yPossibleSplitPoints.length < 2) // all records has the same Y value
            if (yPossibleSplitPoints.length == 1) new Empty(yPossibleSplitPoints(0).toString) else new Empty()
        else {
            val bestSplitPoints = featureSet.data.map(x => (x, x.getBestSplitPoint)).dropRight(1)

            bestSplitPoints.foreach(println)
            val bestSplitPoint = (bestSplitPoints.maxBy(_._2._3))
            println("Best SplitPoint:" + bestSplitPoint)
            var left: Stream[Array[String]] = Stream[Array[String]]()
            var right: Stream[Array[String]] = Stream[Array[String]]()

            bestSplitPoint._2._2 match {
                case d: Double => { // This is a splitting point on numerical feature
                    left = data filter (x => (x(bestSplitPoint._1.index).toDouble < d))
                    right = data filter (x => (x(bestSplitPoint._1.index).toDouble >= d))

                    new NonEmpty(
                        bestSplitPoint._1, // featureInfo
                        (bestSplitPoint._1.Name + " < " + d, bestSplitPoint._1.Name + " >= " + d), // left + right conditions
                        BuildTree(left), // left
                        BuildTree(right) // right
                        )
                }
                case s: Set[Any] => {
                    left = data filter (x => s.contains(x(bestSplitPoint._1.index)))
                    right = data filter (x => !s.contains(x(bestSplitPoint._1.index)))
                    new NonEmpty(
                        bestSplitPoint._1, // featureInfo
                        (s.toString, (bestSplitPoint._1.getPossibleSplitPoints.toSet &~ s).toString), // left + right conditions
                        BuildTree(left), // left
                        BuildTree(right) // right
                        )
                }

            }
        }

    }

    def processLine(fields: Array[String]): Unit = {
        var i = 0;
        var yValue = parseDouble(fields(fields.length - 1))
        yValue match {
            case Some(yValueDouble) =>
                fields.map(f => {
                    featureSet.data(i).addValue(f, yValueDouble)
                    i = (i + 1) % featureSet.numberOfFeature
                })
            case None => "Invalid data"
        }
    }
    
    implicit def toString(s : Set[Any]) = "{" + s.mkString(",") + "}"

    private def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }
}
*/

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class MyRegistrator extends KryoRegistrator {
    override def registerClasses(kryo: Kryo) {
        kryo.register(classOf[RegressionTree])
        kryo.register(classOf[FeatureAggregateInfo])
        kryo.register(classOf[SplitPoint])
        kryo.register(classOf[FeatureSet])
    }
}

class FeatureAggregateInfo(val index: Int, var xValue: Any, var yValue: Double, var frequency: Int) extends Serializable {
    def addFrequency(acc: Int): FeatureAggregateInfo = { this.frequency = this.frequency + acc; this }
    def +(that: FeatureAggregateInfo) = {
        this.frequency = this.frequency + that.frequency
        this.yValue = this.yValue + that.yValue
        this
    }
    override def toString() = "Feature(index:" + index + " | xValue:" + xValue +
        " | yValue" + yValue + " | frequency:" + frequency + ")";
}

// index : index of feature
// point: the split point of this feature
// weight: the weight we get if we apply this splitting
class SplitPoint(val index: Int, val point: Any, val weight: Double) extends Serializable {
    override def toString = index.toString + "," + point.toString + "," + weight.toString
}

class RegressionTree(dataRDD: RDD[String], metadataRDD: RDD[String], context: SparkContext) extends Serializable {

    val mydata = dataRDD.map(line => line.split(","))
    val number_of_features = context.broadcast(mydata.take(1)(0).length)
    val featureSet = context.broadcast(new FeatureSet(metadataRDD))
    val featureTypes = context.broadcast(Array[String]() ++ featureSet.value.data.map(x => x.Type))
    val context1 = context.broadcast(context)

    private def processLine(line: Array[String], numberFeatures: Int, fTypes: Array[String]): Array[FeatureAggregateInfo] = {
        val length = numberFeatures
        var i = -1;
        parseDouble(line(length - 1)) match {
            case Some(yValue) => { // check type of Y : if isn't continuos type, return nothing
                line.map(f => {
                    i = (i + 1) % length
                    fTypes(i) match {
                        case "0" => { // If this is a numerical feature => parse value from string to double
                            val v = parseDouble(f);
                            v match {
                                case Some(d) => new FeatureAggregateInfo(i, d, yValue, 1)
                                case None => new FeatureAggregateInfo(-1, f, 0, 0)
                            }
                        }
                        // if this is a categorial feature => return a FeatureAggregateInfo
                        case "1" => new FeatureAggregateInfo(i, f, yValue, 1)
                    }
                })
            }
            case None => Array[FeatureAggregateInfo]()
        }
    }

    def buildTree(): Node = {
        def buildIter(rawdata: RDD[Array[String]]): Node = {
            
            var data = rawdata.flatMap(processLine(_, number_of_features.value, featureTypes.value)).cache
            //data.collect.sortBy(_.index).foreach(println)
            val yFeature = data.filter(x => x.index == number_of_features.value - 1).groupBy(_.yValue)
            // yFeature: (Index, [Array of value])
            val yCount = yFeature.count	// how many value of Y
            if (yCount < 2)
                // pre-prune tree :  don't need to expand node if every Y values are the same
                if (yCount == 1) new Empty(yFeature.first._2(0).yValue.toString) // create leave node with value of Y
                else new Empty("Unknown")
            else {
            	
                var featureValueSorted = (
                    data
                    .groupBy(x => (x.index, x.xValue))
                    .map(x => (new FeatureAggregateInfo(x._1._1, x._1._2, 0, 0)
                        + x._2.foldLeft(new FeatureAggregateInfo(x._1._1, x._1._2, 0, 0))(_ + _)))
                    // sample results
                    //Feature(index:2 | xValue:normal | yValue6.0 | frequency:7)
                    //Feature(index:1 | xValue:sunny | yValue2.0 | frequency:5)
                    //Feature(index:4 | xValue:14.5 | yValue1.0 | frequency:1)
                    //Feature(index:2 | xValue:high | yValue3.0 | frequency:7)

                    .groupBy(x => x.index)
                    .map(x =>
                        (x._1, x._2.toSeq.sortBy(
                            v => v.xValue match {
                                case d: Double => d // sort by xValue if this is numerical feature
                                case s: String => v.yValue / v.frequency // sort by the average of Y if this is categorical value
                            }))))

                var splittingPointFeature = featureValueSorted.map(x =>
                    x._2(0).xValue match {
                        case s: String => // process with categorical feature
                            {
                                var acc: Int = 0; // the number records on the left of current feature
                                var currentSumY: Double = 0 // current sum of Y of elements on the left of current feature
                                val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency) // number of records
                                val sumY = x._2.foldLeft(0.0)(_ + _.yValue) // total sum of Y

                                var splitPoint: Set[String] = Set[String]()
                                var lastFeatureValue = new FeatureAggregateInfo(-1, 0, 0, 0)
                                try {
                                    x._2.map(f => {

                                        if (lastFeatureValue.index == -1) {
                                            lastFeatureValue = f
                                            new SplitPoint(x._1, Set(), 0.0)
                                        } else {
                                            currentSumY = currentSumY + lastFeatureValue.yValue
                                            splitPoint = splitPoint + lastFeatureValue.xValue.asInstanceOf[String]
                                            acc = acc + lastFeatureValue.frequency
                                            val weight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                                            lastFeatureValue = f
                                            new SplitPoint(x._1, splitPoint, weight)
                                        }
                                    }).drop(1).maxBy(_.weight) // select the best split
                                    // we drop 1 element because with Set{A,B,C} , the best split point only be {A} or {A,B}
                                } catch {
                                    case e: UnsupportedOperationException => new SplitPoint(-1, 0.0, 0.0)
                                }
                            }
                        case d: Double => // process with numerical feature
                            {
                                var acc: Int = 0 // number of records on the left of the current element
                                val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency)
                                var currentSumY: Double = 0
                                val sumY = x._2.foldLeft(0.0)(_ + _.yValue)
                                var posibleSplitPoint: Double = 0
                                var lastFeatureValue = new FeatureAggregateInfo(-1, 0, 0, 0)
                                try {
                                    x._2.map(f => {

                                        if (lastFeatureValue.index == -1) {
                                            lastFeatureValue = f
                                            new SplitPoint(x._1, 0.0, 0.0)
                                        } else {
                                            posibleSplitPoint = (f.xValue.asInstanceOf[Double] + lastFeatureValue.xValue.asInstanceOf[Double]) / 2;
                                            currentSumY = currentSumY + lastFeatureValue.yValue
                                            acc = acc + lastFeatureValue.frequency
                                            val weight = currentSumY * currentSumY / acc + (sumY - currentSumY) * (sumY - currentSumY) / (numRecs - acc)
                                            lastFeatureValue = f
                                            new SplitPoint(x._1, posibleSplitPoint, weight)
                                        }
                                    }).drop(1).maxBy(_.weight) // select the best split
                                } catch {
                                    case e: UnsupportedOperationException => new SplitPoint(-1, 0.0, 0.0)
                                }
                            } // end of matching double
                    } // end of matching xValue
                    ).
                    filter(_.index != number_of_features.value - 1).collect.maxBy(_.weight) // select best feature to split

                println("Best splitpoint:" + splittingPointFeature)
                if (splittingPointFeature.index == -1){ // the chosen feature has only one value
                    val commonValueY = yFeature.reduce((x, y) => if (x._2.length > y._2.length) x else y)._1
                    new Empty(commonValueY.toString)
                }
                else {
                    val chosenFeatureInfo = context1.value.broadcast(featureSet.value.data.filter(f => f.index == splittingPointFeature.index).first)

                    splittingPointFeature.point match {
                        case s: Set[String] => { // split on categorical feature
                            val left = rawdata.filter(x => s.contains(x(chosenFeatureInfo.value.index)))
                            val right = rawdata.filter(x => !s.contains(x(chosenFeatureInfo.value.index)))
                            new NonEmpty(
                                chosenFeatureInfo.value, // featureInfo
                                (s.toString, "Not in " + s.toString), // left + right conditions
                                buildIter(left), // left
                                buildIter(right) // right
                                )
                        }
                        case d: Double => { // split on numerical feature
                            val left = rawdata.filter(x => (x(chosenFeatureInfo.value.index).toDouble < d))
                            val right = rawdata.filter(x => (x(chosenFeatureInfo.value.index).toDouble >= d))
                            new NonEmpty(
                                chosenFeatureInfo.value, // featureInfo
                                (chosenFeatureInfo.value.Name + " < " + d, chosenFeatureInfo.value.Name + " >= " + d), // left + right conditions
                                buildIter(left), // left
                                buildIter(right) // right
                                )
                        }
                    } // end of matching
                } // end of if index == -1
            }
            //new Empty
        }
        buildIter(mydata.cache())
    }
    private def parseDouble(s: String) = try { Some(s.toDouble) } catch { case _ => None }
}
