package playground

import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.JavaTokenParsers

trait Cardinality

object Required extends Cardinality {
  override def toString = "Required"
}

object Optional extends Cardinality {
  override def toString = "Optional"
}

case class Qualified(min: Int, max: Int) extends Cardinality

case class MinQualified(min: Int) extends Cardinality

case class Frame(uri: String, superClasses: List[String], classes: List[String], attributes: List[Attribute])

case class Attribute(uri: String, cardinality: Cardinality = Required, typesig: String) {
  def isRequired = cardinality match {
    case Required => true
    case Qualified(0, _) => false
    case MinQualified(0) => false
    case Optional => false
    case _ => true
  }
}

object FLogicParser extends JavaTokenParsers with PackratParsers {

  def frames: Parser[List[Frame]] = rep(frameExpr)

  // comments
  // namespace definitions

  def frameExpr: Parser[Frame] = frameExpr1 | frameExpr2 | frameExpr3

  def frameExpr1: Parser[Frame] =
    ident ~ ":" ~ ident ~ attrs ^^ {
      case id ~ _ ~ sid ~ attrs => Frame(id, List(), List(sid), attrs)
    }

  def frameExpr2: Parser[Frame] =
    ident ~ "::" ~ ident ~ attrs ^^ {
      case id ~ _ ~ sid ~ attrs => Frame(id, List(sid), List(), attrs)
    }

  def frameExpr3: Parser[Frame] =
    ident ~ attrs ^^ {
      case id ~ attrs => Frame(id, List(), List(), attrs)
    }

  def attrs: Parser[List[Attribute]] =
    "[" ~ repsep(attr, ",") ~ "]." ^^ { case _ ~ x ~ _ => x } |
    "." ^^ { case _ => List() }

  def attr: Parser[Attribute] =
    ident1 ~ cardinality ~ doubleArrow ~ typesig ^^ {
      case id ~ c ~ _ ~ t => Attribute(id, c, t)
    }

  def arrow: Parser[Any] = "->" | "*->"

  def doubleArrow: Parser[Any] = "=>" | "*=>"

  def cardinality: Parser[Cardinality] =
    "(" ~ """\d+?""".r ~ "," ~ ("""\d+?""".r | "*") ~ ")" ^^ {
      case _ ~ "0" ~ _ ~ "1" ~ _ => Optional
      case _ ~ "1" ~ _ ~ "1" ~ _ => Required
      case _ ~ min ~ _ ~ "*" ~ _ => MinQualified(min.toInt)
      case _ ~ min ~ _ ~ max ~ _ => Qualified(min.toInt, max.toInt)
    } |
      "" ^^ { case _ => Required }

  def typesig: Parser[String] = """_[a-zA-Z]+""".r | ident1

  override def ident: Parser[String] = ident1 | ident0

  def ident0: Parser[String] = """[a-zA-Z_/]+""".r

  def ident1: Parser[String] = "\"" ~ """[a-zA-Z_/:\#\-\.0-9]+""".r ~ "\"" ^^ { case _ ~ x ~ _ => x }

  override val whiteSpace = """(\s|#.*)+""".r

  def apply(input: String): List[Frame] = parseAll(frames, input) match {
    case Success(result, _) => result
    case failure: NoSuccess => scala.sys.error(failure.msg)
  }
}
