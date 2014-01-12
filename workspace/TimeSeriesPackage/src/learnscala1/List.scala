package learnscala1
/*
trait List[T] {
	def isEmpty : Boolean
	def head : T
	def tail : List[T]
	def findElementAtIndex(n : Int) : T;
}

class Cons[T](val head : T, val tail: List[T]) extends List[T] {
    def isEmpty = false
    
    def findElementAtIndex(n : Int) : T = {
            if (n == 0) this.head
            else this.tail.findElementAtIndex(n - 1)      
    }
}

class Nil[T] extends List[T] {
    def isEmpty : Boolean = true
    def head : Nothing = throw new NoSuchElementException ("nil.head")
    def tail : Nothing = throw new NoSuchElementException ("nil.tail")
    def findElementAtIndex(n : Int) : Nothing = throw new IndexOutOfBoundsException("index out of bound");
}

object List {
	def apply[T](x1 : T, x2 : T) = new Cons(x1, new Cons(x2, new Nil))
	def apply[T]() = new Nil
}
*/