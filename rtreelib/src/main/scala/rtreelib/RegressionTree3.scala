package rtreelib

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.immutable.Queue
import java.util.{ Timer, TimerTask }
import java.util.Date
import scala.concurrent._
import scala.concurrent.duration.Duration
import rtreelib._

class RegressionTree3(metadata: Array[String]) extends BaseRegressionTree(metadata) {

  var expandingJobs: Queue[JobInfo] = Queue[JobInfo]();
  var finishedJobs: Queue[JobInfo] = Queue[JobInfo]();
  var numberOfRunningJobs = 0
  
  private val ERROR_SPLITPOINT_VALUE = ",,,@,,," 
  private var MAXIMUM_PARALLEL_JOBS = 9999
  
  def setMaximumParallelJobs(value : Int) = { MAXIMUM_PARALLEL_JOBS = value }

  private def updateModel(finishJob: JobInfo) {
	  
    println("Update model with finished job:" + finishJob)
    val newnode = (
      if (finishJob.splitPoint.index < 0) {
        if (finishJob.splitPoint.index == -1)	// stop node
          new Empty(finishJob.splitPoint.point.toString)
        else	// error case
          new Empty(ERROR_SPLITPOINT_VALUE)
      } else {
        val chosenFeatureInfoCandidate = featureSet.data.find(f => f.index == finishJob.splitPoint.index)

        chosenFeatureInfoCandidate match {
          case Some(chosenFeatureInfo) => {
            new NonEmpty(chosenFeatureInfo,
              finishJob.splitPoint.point,
              new Empty("empty.left"),
              new Empty("empty.right"));
          }
          case None => { new Empty(this.ERROR_SPLITPOINT_VALUE)}
        }
      })

    if (newnode.value == this.ERROR_SPLITPOINT_VALUE)
      return
      
    // If tree has zero node, create a root node
    if (tree.isEmpty) {
      tree = newnode;

    } else //  add new node to current model
    {

      val level = (Math.log(finishJob.ID.toDouble) / Math.log(2)).toInt
      var i: Int = level - 1
      var parent = tree; // start adding from root node
      while (i > 0) {

        if ((finishJob.ID / (2 << i - 1)) % 2 == 0) {
          // go to the left
          parent = parent.left
        } else {
          // go go the right
          parent = parent.right
        }
        i -= 1
      } // end while

      if (finishJob.ID % 2 == 0) {
        parent.setLeft(newnode)
      } else {
        parent.setRight(newnode)
      }
    }
  }

  private def launchJob(job: JobInfo, inputData: RDD[Array[FeatureValueAggregate]]) {
    var thread = new Thread(new JobExecutor(job, inputData, this))
    thread.start()
  }

  def addJobToExpandingQueue(job: JobInfo) {
    this.synchronized {
      this.expandingJobs = this.expandingJobs :+ job
      println("Add job id=" + job.ID + " into expanding queue")

    }
  }

  def addJobToFinishedQueue(job: JobInfo) {
    this.synchronized {
      this.finishedJobs = this.finishedJobs :+ job
      println("Add job id=" + job.ID + " into finished queue")

    }
  }

  /**
   * Building tree bases on:
   * @yFeature: predicted feature
   * @xFeature: input features
   * @return: root of tree
   */
  def buildTree(trainingData: RDD[String],
    yFeature: String = featureSet.data(yIndex).Name,
    xFeatures: Set[String] = Set[String]()): Node = {

    /* INITIALIZE */

    // parse raw data
    val mydata = trainingData.map(line => line.split(delimiter))

    // scala 2.9
    //var fYindex = featureSet.data.findIndexOf(p => p.Name == yFeature)
    var fYindex = featureSet.data.indexWhere(p => p.Name == yFeature)

    // PM: You're sending from the "driver" to all workers the index of the Y feature, the one you're trying to predict
    if (fYindex >= 0) yIndex = featureSet.data(fYindex).index

    xIndexs =
      if (xFeatures.isEmpty) // if user didn't specify xFeature, we will process on all feature, include Y feature (to check stop criterion)
        featureSet.data.map(x => x.index).toSet[Int]
      else
        xFeatures.map(x => featureSet.getIndex(x)) + yIndex

    val transformedData = mydata.map(x => processLine(x, featureSet.numberOfFeature, featureSet))
    // if I cache the RDD here, the algorithm turns out wrong result

    println("YIndex:" + yIndex + " xIndexes:" + xIndexs)
    tree = new Empty("None")
    val firstJob = new JobInfo(1, List[Condition]())

    this.addJobToExpandingQueue(firstJob)
    numberOfRunningJobs = 0

    /* END OF INIT */

    def finish() = {
      //hasAnyJobStop && 
      (this.numberOfRunningJobs == 0)
    }

    /* START ALGORITHMS */
    do {

      //get jobs from finishedJobs and update tree model
      this.synchronized {
        while (!finishedJobs.isEmpty) {
          finishedJobs.dequeue match {
            case (j, xs) => {
              this.numberOfRunningJobs = this.numberOfRunningJobs - 1
              println("Dequeue finished jobs id=" + j.ID + ". NumJob = " + this.numberOfRunningJobs.toString)
              updateModel(j)
              finishedJobs = xs
            }
          }
        }
      }

      //get jobs from expandingJobs and launch them
      this.synchronized {
        while (!expandingJobs.isEmpty && numberOfRunningJobs < MAXIMUM_PARALLEL_JOBS) {
          expandingJobs.dequeue match {
            case (j, xs) => {
              println("Dequeue expanding jobs id=" + j.ID)
              this.numberOfRunningJobs = this.numberOfRunningJobs + 1
              expandingJobs = xs
              println("Launch job id=" + j.ID + " numberJobs=" + this.numberOfRunningJobs.toString)
              launchJob(j, transformedData)

            }
          }
        }
      }

      //DelayedFuture( 5000L )(println("iter"))
    } while (!finish())

    /* END OF ALGORITHM */

    println("DONE")

    tree
  }

}