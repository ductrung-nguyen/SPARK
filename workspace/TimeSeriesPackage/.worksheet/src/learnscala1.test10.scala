package learnscala1

object test10 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(182); 

	def insert(x: Int, xs: List[Int]) : List[Int] = xs match {
  	case List() => x::Nil
  	case y::ys => if (x < y) x::xs else y::insert(x, ys)
  };System.out.println("""insert: (x: Int, xs: List[Int])List[Int]""");$skip(115); 
  
	def sort(xs:List[Int]) : List[Int] = xs match {
		case List() => List()
		case y::ys => insert(y, sort(ys))
	};System.out.println("""sort: (xs: List[Int])List[Int]""");$skip(25); val res$0 = 
	
	sort(List(4,2,9,6,8));System.out.println("""res0: List[Int] = """ + $show(res$0));$skip(511); 
	
	
	def msort[T](xs: List[T])(implicit ord: Ordering[T] ) : List[T] = xs match {
		case List() => List()
		case x:: Nil => xs
		case x:: xs1 => {
			val n = xs.length/2
			def merge(x1 : List[T], x2: List[T]):List[T] = (x1, x2) match {
				case (x1, Nil) => x1
				case (Nil, x2) => x2
				case (x1head::x1tail, x2head::x2tail) =>
					if (ord.lt(x1head , x2head)) x1head::merge(x1tail, x2)
					else x2head::merge(x1,x2tail)
			}
			val (left,right) = xs.splitAt(n)
			merge(msort(left), msort(right))
		}
	};System.out.println("""msort: [T](xs: List[T])(implicit ord: Ordering[T])List[T]""");$skip(37); 
	
	val l = List(4, 6, 2,-3 ,5,0,8,9);System.out.println("""l  : List[Int] = """ + $show(l ));$skip(24); val res$1 = 
	msort(l)(Ordering.Int);System.out.println("""res1: List[Int] = """ + $show(res$1));$skip(56); 
	val fruits = List("Apple", "Orange", "CoCo", "Banana");System.out.println("""fruits  : List[java.lang.String] = """ + $show(fruits ));$skip(15); val res$2 = 
	msort(fruits);System.out.println("""res2: List[java.lang.String] = """ + $show(res$2));$skip(25); val res$3 = 
	
	l filter (x => x > 0);System.out.println("""res3: List[Int] = """ + $show(res$3));$skip(26); val res$4 = 
	l filterNot (x => x > 0);System.out.println("""res4: List[Int] = """ + $show(res$4));$skip(26); val res$5 = 
	l partition (x => x > 0);System.out.println("""res5: (List[Int], List[Int]) = """ + $show(res$5));$skip(26); val res$6 = 
	l takeWhile (x => x > 2);System.out.println("""res6: List[Int] = """ + $show(res$6));$skip(26); val res$7 = 
	l dropWhile (x => x > 2);System.out.println("""res7: List[Int] = """ + $show(res$7));$skip(21); val res$8 = 
	l span (x => x > 2);System.out.println("""res8: (List[Int], List[Int]) = """ + $show(res$8));$skip(162); 
	
	def pack[T] (xs : List[T]) : List[List[T]] = xs match {
		case Nil => Nil
		case x :: xs1 => {
			val (l, r) = xs span (y => y == x)
			l :: pack[T](r)
		}
	};System.out.println("""pack: [T](xs: List[T])List[List[T]]""");$skip(116); 
	
	def encode[T] (xs : List[T]) :  List[Pair[T,Int]] = {
		val p = pack(xs)
		p map (x => (x.head, x.length))
		
	};System.out.println("""encode: [T](xs: List[T])List[(T, Int)]""");$skip(54); val res$9 = 
	
	pack(List("a", "a", "a", "b", "b", "c", "d", "d"));System.out.println("""res9: List[List[java.lang.String]] = """ + $show(res$9));$skip(55); val res$10 = 
  encode(List("a", "a", "a", "b", "b", "c", "d", "d"));System.out.println("""res10: List[(java.lang.String, Int)] = """ + $show(res$10));$skip(17); val res$11 = 
  encode(List());System.out.println("""res11: List[(Nothing, Int)] = """ + $show(res$11));$skip(67); 
  
  
  def sum(xs : List[Int]) : Int = 0 :: xs reduceLeft (_ + _);System.out.println("""sum: (xs: List[Int])Int""");$skip(60); 
  def sum1(xs : List[Int]) : Int = (xs foldLeft 0) ( _ + _);System.out.println("""sum1: (xs: List[Int])Int""");$skip(62); 
  def product(xs : List[Int]) : Int = (xs foldLeft 1) (_ * _);System.out.println("""product: (xs: List[Int])Int""");$skip(20); val res$12 = 
          
  sum(l);System.out.println("""res12: Int = """ + $show(res$12));$skip(10); val res$13 = 
  sum1(l);System.out.println("""res13: Int = """ + $show(res$13));$skip(13); val res$14 = 
  product(l);System.out.println("""res14: Int = """ + $show(res$14));$skip(26); 
	
  val s = "Hello World";System.out.println("""s  : java.lang.String = """ + $show(s ));$skip(27); val res$15 = 
  s flatMap (c => "." + c);System.out.println("""res15: String = """ + $show(res$15));$skip(32); val res$16 = 
  s flatMap (c => List('.', c));System.out.println("""res16: String = """ + $show(res$16));$skip(38); 
  val x = s flatMap (c => Array(1,2));System.out.println("""x  : scala.collection.immutable.IndexedSeq[Int] = """ + $show(x ));$skip(11); val res$17 = 
  x.length;System.out.println("""res17: Int = """ + $show(res$17));$skip(80); 
  
  def isPrime(n: Int) = (((2 to n/2) filter ( x => n % x == 0)).length == 0);System.out.println("""isPrime: (n: Int)Boolean""");$skip(13); val res$18 = 
  isPrime(1);System.out.println("""res18: Boolean = """ + $show(res$18));$skip(213); 
  
  
  def findSumPrime(n: Integer) = {
  val result : List[(Int, Int)] = Nil
  
  	(1 until n) flatMap (
  		i => (1 until i) map (
  			j => (i,j)
  		)
  	) filter (pair => isPrime(pair._1 + pair._2))
  	
  };System.out.println("""findSumPrime: (n: Integer)scala.collection.immutable.IndexedSeq[(Int, Int)]""");$skip(149); 
  def findSumPrime2(n: Integer) =
  	for {
  		i <- 1 until n	 // generator
  		j <- 1 until i	// generator
  		if (isPrime(i + j))
  	} yield (i,j);System.out.println("""findSumPrime2: (n: Integer)scala.collection.immutable.IndexedSeq[(Int, Int)]""");$skip(21); val res$19 = 
  
  findSumPrime(5);System.out.println("""res19: scala.collection.immutable.IndexedSeq[(Int, Int)] = """ + $show(res$19));$skip(19); val res$20 = 
  findSumPrime2(5);System.out.println("""res20: scala.collection.immutable.IndexedSeq[(Int, Int)] = """ + $show(res$20));$skip(122); 
  
  
  def scalaProduct(xs: List[Double], ys: List[Double]) =
  	(for {
  		x <- xs zip ys
  	} yield (x._1 * x._2)) sum;System.out.println("""scalaProduct: (xs: List[Double], ys: List[Double])Double""");$skip(45); val res$21 = 
  	
  scalaProduct(List(1,2,3), List(4,5,6));System.out.println("""res21: Double = """ + $show(res$21));$skip(540); 
  
  
  def queens(n : Int) : Set[List[Int]] = {
  	def putQueen(k : Int) : Set[List[Int]] =
  		if (k == 0) Set(List())
  		else
  			for {
  				queens <- putQueen(k - 1)
  				col <- 0 until n
  				if (isSafe(col, queens))
  			} yield col :: queens
  	
  	
  			
  	def isSafe(col : Int, queens : List[Int]) : Boolean = {
  		val numrow = queens.length
  		val pastSolution = ( numrow -1 to 0 by -1) zip queens
  		pastSolution forall {
  			case (r, c) => c != col && math.abs(col - c) != numrow - r
  		}
  	}
  	
  	putQueen(n)
  };System.out.println("""queens: (n: Int)Set[List[Int]]""");$skip(201); 
  
  def showQueens(solution: List[Int]) = {
  	val lines =
  	for ( col <- solution)
  		yield (Vector.fill(solution.length)("* ").updated(col, "X ")) mkString
  	
  	"\n" + (lines mkString "\n")
  };System.out.println("""showQueens: (solution: List[Int])java.lang.String""");$skip(47); val res$22 = 
  
  (queens(4) map showQueens ) mkString "\n";System.out.println("""res22: String = """ + $show(res$22))}
  
}
	
	