package rtreelib

import scala.collection.immutable.Queue
/*
class TreeBuilder(val featureSet: FeatureSet) extends Serializable {
	var root: Node = new Empty("None");

    var expandingJobs: Queue[Job] = Queue[Job]();
    var finishedJobs: Queue[Job] = Queue[Job]();

    private def updateModel(finishJob: Job) {

        val chosenFeatureInfo = featureSet.data.filter(f => f.index == finishJob.splitPoint.index).first
        val newnode = new NonEmpty(chosenFeatureInfo,
            finishJob.splitPoint.point,
            new Empty("left"),
            new Empty("right"));

        // If tree has zero node, create a root node
        if (root.isEmpty) {
            root = newnode;

        } else //  add new node to current model
        {
            val level = (Math.log(finishJob.ID.toDouble) / Math.log(2)).toInt;
            var i: Int = level - 1;
            var parent = root;
            while (i > 0) {
                if ((finishJob.ID / (2 << i)) % 2 == 0) {
                    // go to the left
                    parent = parent.left;
                } else {
                    // go go the right
                    parent = parent.right;
                }
            }

            if (finishJob.ID % 2 == 0) {
                parent.setLeft(newnode)
            } else {
                parent.setRight(newnode);
            }
        }
    }

    private def launchJob(job: Job,
        inputData: RDD[Array[FeatureValueAggregate]]) {

        var data = inputData.filter(
            x => job.conditions_of_input_dataset.forall(
                sp => {
                    sp.check(x(sp.splitPoint.index).xValue)
                })).flatMap(x => x.toSeq)
        //var data = rawdata.flatMap(x => x.toSeq)

        val (stopExpand, eY) = checkStopCriterion(data)
        if (stopExpand) {
            new Empty(eY.toString)
        } else {
            val groupFeatureByIndexAndValue =
                data.groupBy(x => (x.index, x.xValue)) // PM: this operates on an RDD => in parallel

            var featureValueSorted = (
                //data.groupBy(x => (x.index, x.xValue))
                groupFeatureByIndexAndValue // PM: this is an RDD hence you do the map and fold in parallel (in MapReduce this would be the "reducer")
                .map(x => (new FeatureValueAggregate(x._1._1, x._1._2, 0, 0)
                    + x._2.foldLeft(new FeatureValueAggregate(x._1._1, x._1._2, 0, 0))(_ + _)))
                // sample results
                //Feature(index:2 | xValue:normal | yValue6.0 | frequency:7)
                //Feature(index:1 | xValue:sunny | yValue2.0 | frequency:5)
                //Feature(index:2 | xValue:high | yValue3.0 | frequency:7)

                .groupBy(x => x.index) // This is again operating on the RDD, and actually is like the continuation of the "reducer" code above
                .map(x =>
                    (x._1, x._2.toSeq.sortBy(
                        v => v.xValue match {
                            case d: Double => d // sort by xValue if this is numerical feature
                            case s: String => v.yValue / v.frequency // sort by the average of Y if this is categorical value
                        }))))

            var splittingPointFeature = featureValueSorted.map(x => // operates on an RDD, so this is in parallel
                x._2(0).xValue match {
                    case s: String => // process with categorical feature
                        {
                            var acc: Int = 0; // the number records on the left of current feature
                            var currentSumY: Double = 0 // current sum of Y of elements on the left of current feature
                            val numRecs: Int = x._2.foldLeft(0)(_ + _.frequency) // number of records
                            val sumY = x._2.foldLeft(0.0)(_ + _.yValue) // total sum of Y

                            var splitPoint: Set[String] = Set[String]()
                            var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0)
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
                                }).drop(1).maxBy(_.weight) // select the best split // PM: please explain this trick with an example
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
                            var lastFeatureValue = new FeatureValueAggregate(-1, 0, 0, 0)
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
                filter(_.index != yIndex).collect.
                maxBy(_.weight) // select best feature to split
            // PM: collect here means you're sending back all the data to a single machine (the driver).

            if (splittingPointFeature.index == -1) { // the chosen feature has only one value
                //val commonValueY = yFeature.reduce((x, y) => if (x._2.length > y._2.length) x else y)._1
                //new Empty(commonValueY.toString)
                new Empty(eY.toString)
                job.continueExpand = false
                addJobToFinishedQueue(job)
            } else {
                val chosenFeatureInfo = featureSet.data.filter(f => f.index == splittingPointFeature.index).first

                val leftCondition = job.conditions_of_input_dataset :+ new Condition(splittingPointFeature, true)
                val rightCondition = job.conditions_of_input_dataset :+ new Condition(splittingPointFeature, false)
                val leftJob = new Job(job.ID + 1, leftCondition)
                val rightJob = new Job(job.ID + 2, rightCondition)
                addJobToExpandingQueue(leftJob)
                addJobToExpandingQueue(rightJob)
                job.continueExpand = true
                addJobToFinishedQueue(job)
            } // end of if index == -1
        }

    }

    private def addJobToExpandingQueue(job: Job) {

    }
    
    private def addJobToFinishedQueue(job : Job){
        
    }
}
*/