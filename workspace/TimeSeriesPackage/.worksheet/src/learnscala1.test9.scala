package learnscala1

object test9 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(404); 
  def show(e : Expr) : String = e match {
  	case Number(x) => x.toString
  	case Sum(left, right) => show(left) + " + " + show(right)
  	case Product(left, right) => {
  		(if (e.priority > left.priority) "(" + show(left) + ")" else show(left)) +
  		" * " +
  		(if (e.priority > right.priority) "(" + show(right) + ")" else show(right))
  	}
  	case Var(x) => x
  };System.out.println("""show: (e: learnscala1.Expr)String""");$skip(41); val res$0 = 
  
  show(new Sum(Number(1), Number(2)));System.out.println("""res0: String = """ + $show(res$0));$skip(23); 
  var x = SomeType1(3);System.out.println("""x  : learnscala1.SomeType1 = """ + $show(x ));$skip(52); val res$1 = 
  show(Sum(Product(Number(2), Var("X")), Var("Y")));System.out.println("""res1: String = """ + $show(res$1));$skip(52); val res$2 = 
  show(Product(Sum(Number(2), Var("X")), Var("Y")));System.out.println("""res2: String = """ + $show(res$2))}
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