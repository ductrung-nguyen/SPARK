package learnscala1

object test12 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(82); 
    println("Welcome to the Scala worksheet");$skip(163); 

    
    def range(start: Int, end: Int): Stream[Int] =
        if (start >= end) Stream.empty
        else
            Stream.cons(start, range(start + 1, end));System.out.println("""range: (start: Int, end: Int)Stream[Int]""");$skip(104); 
            
    def isPrime(n: Int) : Boolean =
        !((range(2,  n / 2) exists (x => n % x == 0)));System.out.println("""isPrime: (n: Int)Boolean""");$skip(49); 
                                 
    val x = 10;System.out.println("""x  : Int = """ + $show(x ));$skip(15); val res$0 = 
    isPrime(x);System.out.println("""res0: Boolean = """ + $show(res$0));$skip(18); val res$1 = 
    isPrime(1009);System.out.println("""res1: Boolean = """ + $show(res$1));$skip(49); val res$2 = 
    ((1000 until 10000) filter isPrime)  apply 3;System.out.println("""res2: Int = """ + $show(res$2));$skip(80); 
    
    def constructStream(n : Int) :Stream[Int] = n #:: constructStream(n+1);System.out.println("""constructStream: (n: Int)Stream[Int]""");$skip(75); 
    
    val infi = (constructStream(100) takeWhile (x => x < 120)).toList;System.out.println("""infi  : List[Int] = """ + $show(infi ));$skip(36); 
    
    var s1 = Stream(1,2,3,4,5);System.out.println("""s1  : scala.collection.immutable.Stream[Int] = """ + $show(s1 ));$skip(33); 
    var s2 = Stream(10,20,30,40);System.out.println("""s2  : scala.collection.immutable.Stream[Int] = """ + $show(s2 ));$skip(148); 
    
    
    def getAllPrimeNumber(xs : Stream[Int]) : Stream[Int] = {
    	xs.head #:: getAllPrimeNumber(xs.tail filter (_ % xs.head != 0))
    };System.out.println("""getAllPrimeNumber: (xs: Stream[Int])Stream[Int]""");$skip(60); 
    
    val primes = getAllPrimeNumber(constructStream(2));System.out.println("""primes  : Stream[Int] = """ + $show(primes ));$skip(349); 
		def sqrt(n : Int) : Double = {
			def improve(x : Double) = (x + n/x)/2
			lazy val guesses : Stream[Double] = 1 #:: (guesses map improve)
			
			def iter(xs : Stream[Double]) : Double = {
				if (xs.tail == Stream.Empty || xs.tail.head == xs.head) xs.head
				else if (xs.tail != xs.head) iter(xs.tail)
				else xs.head
			}
			iter(guesses)
		};System.out.println("""sqrt: (n: Int)Double""");$skip(22); 
		
		var pair = (1,2);System.out.println("""pair  : (Int, Int) = """ + $show(pair ))}
}