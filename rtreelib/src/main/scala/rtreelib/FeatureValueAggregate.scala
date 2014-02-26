package rtreelib

/**
 * This class is representative for each value of each feature in the data set
 * @index: index of the feature in the whole data set, based zero
 * @xValue: value of the current feature
 * @yValue: value of the Y feature associated (target, predicted feature)
 * @frequency : frequency of this value
 */
class FeatureValueAggregate(val index: Int, var xValue: Any, var yValue: Double, var frequency: Int) extends Serializable {
    def addFrequency(acc: Int): FeatureValueAggregate = { FeatureValueAggregate.this.frequency = FeatureValueAggregate.this.frequency + acc; FeatureValueAggregate.this }
    def +(that: FeatureValueAggregate) = {
        FeatureValueAggregate.this.frequency = FeatureValueAggregate.this.frequency + that.frequency
        FeatureValueAggregate.this.yValue = FeatureValueAggregate.this.yValue + that.yValue
        FeatureValueAggregate.this
    }
    override def toString() = "Feature(index:" + index + " | xValue:" + xValue +
        " | yValue" + yValue + " | frequency:" + frequency + ")";
}
