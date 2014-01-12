package learnscala1

object test8 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(79); 
  println("Welcome to the Scala worksheet");$skip(22); 
  
  var x = new Zero;System.out.println("""x  : learnscala1.Zero = """ + $show(x ));$skip(42); 
  var y = new Succ(new Succ(new Succ(x)));System.out.println("""y  : learnscala1.Succ = """ + $show(y ));$skip(62); 
  var z = new Succ(new Succ(new Succ(new Succ(new Succ(x)))));System.out.println("""z  : learnscala1.Succ = """ + $show(z ));$skip(8); val res$0 = 
  y + z;System.out.println("""res0: learnscala1.Nat = """ + $show(res$0))}
  
  
}

abstract class Nat {
	def value : Int
	def isZero() : Boolean
	def predecessor : Nat
	def successor = new Succ(this)
	def + (that : Nat) : Nat
	def - (that : Nat) : Nat
}

class Zero extends Nat {
	def value = 0
	
	override def isZero() = true
	override def predecessor() = throw new Error(" No predecessor")
	override def + (that :Nat) = that
	override def - (that : Nat) = if (that isZero) new Zero else throw new Error("Negative")
}

class Succ(val n: Nat) extends Nat {
	def value = n.value + 1
	println("Create succ value " + value)
	override def isZero() = false
	override def predecessor = n
	
	override def + (that : Nat) : Nat = {
		println("In function with value = " + value);
		new Succ(n + that);
	}
	
	override def - (that : Nat) = if (that.isZero) this else n - that.predecessor
	
	override def toString() = value.toString()
}