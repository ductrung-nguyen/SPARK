package learnscala1

import test._

object test7 {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(110); 
  val list = new Cons(1, new Cons(2, new Cons(3, new Nil)));System.out.println("""list  : learnscala1.Cons[Int] = """ + $show(list ));$skip(32); val res$0 = 
  
  list.findElementAtIndex(0);System.out.println("""res0: Int = """ + $show(res$0));$skip(20); 
 	var x = List(1,2);System.out.println("""x  : learnscala1.Cons[Int] = """ + $show(x ))}
}