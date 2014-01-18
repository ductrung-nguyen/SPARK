package learnscala1

object test9 {
  def show(e : Expr) : String = e match {
  	case Number(x) => x.toString
  	case Sum(left, right) => show(left) + " + " + show(right)
  	case Product(left, right) => {
  		(if (e.priority > left.priority) "(" + show(left) + ")" else show(left)) +
  		" * " +
  		(if (e.priority > right.priority) "(" + show(right) + ")" else show(right))
  	}
  	case Var(x) => x
  }                                               //> show: (e: learnscala1.Expr)String
  
  show(new Sum(Number(1), Number(2)))             //> res0: String = 1 + 2
  var x = SomeType1(3)                            //> x  : learnscala1.SomeType1 = learnscala1.SomeType1@1bba6451
  show(Sum(Product(Number(2), Var("X")), Var("Y")))
                                                  //> res1: String = 2 * X + Y
  show(Product(Sum(Number(2), Var("X")), Var("Y")))
                                                  //> res2: String = (2 + X) * Y
}

trait Expr {
		var priority = 10
	}
	case class Number(n : Int) extends Expr
	case class Sum(e1 : Expr, e2: Expr) extends Expr { this.priority = 1 }
	case class Var(x:String) extends Expr
	case class Product(e1: Expr, e2: Expr) extends Expr {this.priority = 2}


trait SomeType {}
class SomeType1(n: Int) extends SomeType {}

object SomeType1 {
	def apply(n : Int) = new SomeType1(n)
}