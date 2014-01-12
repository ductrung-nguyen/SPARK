package learnscala1

object test8 {
  println("Welcome to the Scala worksheet")       //> Welcome to the Scala worksheet
  
  var x = new Zero                                //> x  : learnscala1.Zero = learnscala1.Zero@6bee8099
  var y = new Succ(new Succ(new Succ(x)))         //> Create succ value 1
                                                  //| Create succ value 2
                                                  //| Create succ value 3
                                                  //| y  : learnscala1.Succ = 3
  var z = new Succ(new Succ(new Succ(new Succ(new Succ(x)))))
                                                  //> Create succ value 1
                                                  //| Create succ value 2
                                                  //| Create succ value 3
                                                  //| Create succ value 4
                                                  //| Create succ value 5
                                                  //| z  : learnscala1.Succ = 5
  y + z                                           //> In function with value = 3
                                                  //| In function with value = 2
                                                  //| In function with value = 1
                                                  //| Create succ value 6
                                                  //| Create succ value 7
                                                  //| Create succ value 8
                                                  //| res0: learnscala1.Nat = 8
  
  
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