package part_1_recap

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object ScalaRecap extends App{
  // values and variables
  val aBoolean: Boolean = true

  // expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions vs expressions
  val theUnit = println("Hello Scala") // Unit = "no meaningful value" = void in other languages

  // functions
  def myFunction (x: Int) = 42

  // OOP
  class Animal
  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("crunch")
  }

  // singleton pattern
  object MySingleton

  // companions
  object Carnivore

  // generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // functional programming
  val incrementer: Int => Int = (v1: Int) => v1 + 1
  val incremented = incrementer(42)

  // map, flatMap, filter = HOFs
  val processedList = List(1, 2, 3).map(incrementer)

  // pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "First"
    case 2 => "Second"
    case _ => "unknown"
  }


  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "safely handling"
    case _ => "something else"
  }

  // Futures
  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"I've found the $meaningOfLife")
    case Failure(exception) => println(s"I've failed $exception")
  }

  // Partial Function
//  val aPartialFunction = (x: Int) => x match {
//    case 1 => 43
//    case 8 => 56
//    case _ => 999
//  }

  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999
  }

  // Implicits

  //auto-injection
  def methodWithImplicitArg(implicit x: Int) = x + 42
  implicit val implicitVal: Int = 67

  val implicitCall = methodWithImplicitArg

  // implicit conversions - implicit defs
  case class Person(name: String) {
    def greet = println(s"Hi, my name is $name")
  }
  implicit def fromStringToPerson(name: String): Person = Person(name)
  "Bob".greet //  fromStringToPerson("Bob").greet

  // implicit conversions - implicit classes
  implicit class Dog(name: String) {
    def bark = println("bark")
  }

  "Coco".bark //  implicit classes are almost always preferable to implicit defs

  /* Looking for implicit args in defs
  * - local scope
  * - imported scope
  * - companion objects of the types involved in the method call
  * */
 }
