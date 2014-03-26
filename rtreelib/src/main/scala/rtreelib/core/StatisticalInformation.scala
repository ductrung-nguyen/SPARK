package rtreelib.core

class StatisticalInformation (
        var sumY : Double = 0,
        var sumOfYPower2 : Double = 0,
        var numberOfInstances : Int = 0
        ) extends Serializable
        {
	override def toString() : String = { "(%f,%f,%d)".format(sumY, sumOfYPower2, numberOfInstances)}
}