package rtreelib.core


/**
 * This class is representative for features
 * We have two feature categories : Numerical and Categorical
 * which are presented by NumericalFeature and CategoricalFeature, respectively
 *
 * @param Name 	Name of feature
 * @param Type 	Type of feature: "d" is discrete feature( categorical feature); "c" is continuous feature (numerical feature)
 * @param index Index of this feature in the whole data set, based zero
 */

/**
 * The information of a feature
 *
 * @param Name	Name of feature, such as temperature, weather...
 * @param Type	Type of feature: 0 (Continuous) or 1 (Category)
 */
abstract class FeatureInfo(val Name: String, val Type: String, val index: Int) extends Serializable {

    override def toString() = " (Index:" + index + " | Name: " + Name +
        " | Type: " + (if (Type == "c") "continuous" else "discrete") + ") ";

}

/**
 * Object instance FeatureInfo. It can be considered as a feature factory
 */
object FeatureInfo extends Serializable {
    //lazy val numericalTag : String = "c"
    //lazy val categoricalTag : String = "d"

    def apply(Name: String, Type: String, idx: Int) = {
        var nType = Type.trim
        var nName = Utility.normalizeString(Name)
        nType match {
            case "c" => new NumericalFeature(nName, nType, idx) // continuous feature
            case "d" => new CategoricalFeature(nName, nType, idx) // discrete feature
        }
    }
}

/**
 * This class is representative for numerical feature
 */
case class NumericalFeature(fName: String, fType: String, fIndex: Int) extends FeatureInfo(fName, fType, fIndex) {

}

/**
 * This class is representative for categorical feature
 */
case class CategoricalFeature(fName: String, fType: String, fIndex: Int) extends FeatureInfo(fName, fType, fIndex) {

}
