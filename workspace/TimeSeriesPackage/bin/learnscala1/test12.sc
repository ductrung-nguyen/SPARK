package learnscala1

object test12 {
    println("Welcome to the Scala worksheet")     //> Welcome to the Scala worksheet

    
    def range(start: Int, end: Int): Stream[Int] =
        if (start >= end) Stream.empty
        else
            Stream.cons(start, range(start + 1, end))
                                                  //> range: (start: Int, end: Int)Stream[Int]
            
    def isPrime(n: Int) : Boolean =
        !((range(2,  n / 2) exists (x => n % x == 0)))
                                                  //> isPrime: (n: Int)Boolean
                                 
    val x = 10                                    //> x  : Int = 10
    isPrime(x)                                    //> res0: Boolean = false
    isPrime(1009)                                 //> res1: Boolean = true
    ((1000 until 10000) filter isPrime)  apply 3  //> res2: Int = 1021
    
    def constructStream(n : Int) :Stream[Int] = n #:: constructStream(n+1)
                                                  //> constructStream: (n: Int)Stream[Int]
    
    val infi = (constructStream(100) takeWhile (x => x < 120)).toList
                                                  //> infi  : List[Int] = List(100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 1
                                                  //| 10, 111, 112, 113, 114, 115, 116, 117, 118, 119)
    
    var s1 = Stream(1,2,3,4,5)                    //> s1  : scala.collection.immutable.Stream[Int] = Stream(1, ?)
    var s2 = Stream(10,20,30,40)                  //> s2  : scala.collection.immutable.Stream[Int] = Stream(10, ?)
    
    
    def getAllPrimeNumber(xs : Stream[Int]) : Stream[Int] = {
    	xs.head #:: getAllPrimeNumber(xs.tail filter (_ % xs.head != 0))
    }                                             //> getAllPrimeNumber: (xs: Stream[Int])Stream[Int]
    
    val primes = getAllPrimeNumber(constructStream(2))
                                                  //> primes  : Stream[Int] = Stream(2, ?)
		def sqrt(n : Int) : Double = {
			def improve(x : Double) = (x + n/x)/2
			lazy val guesses : Stream[Double] = 1 #:: (guesses map improve)
			
			def iter(xs : Stream[Double]) : Double = {
				if (xs.tail == Stream.Empty || xs.tail.head == xs.head) xs.head
				else if (xs.tail != xs.head) iter(xs.tail)
				else xs.head
			}
			iter(guesses)
		}                                 //> sqrt: (n: Int)Double
		
		var pair = (1,2)                  //> pair  : (Int, Int) = (1,2)
}