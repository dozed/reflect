package playground

import scala.collection.JavaConversions._
import util.control.Exception._
import java.io.{File, FileReader}
import com.hp.hpl.jena.rdf.model._

class PlainRdfReader(protected val data: Resource, val model: Model, val prefix: String = "", val separated: Separator = by.Dots) extends ValueProvider[Resource] {

  def read(key: String): Either[Throwable, Option[Any]] = allCatch either {
    def readValue0(r: RDFNode): AnyRef = r match {
      case value if value.isResource => new PlainRdfReader(value.asResource, model, key, separated)
      case value if value.isLiteral => value.asLiteral.getValue
    }
    sourceValues.filter(_._1.getURI.equals(key)).map(_._2).headOption.map(l => l map readValue0)
  }

  def forPrefix(key: String): ValueProvider[Resource] = new PlainRdfReader(data, model, separated.wrap(key, prefix), separated)

  lazy val values: Resource = data

  protected lazy val sourceValues: Map[Property, List[RDFNode]] = data.listProperties.toList.toList.groupBy(_.getPredicate).mapValues(_.map(_.getObject))

  def keySet: Set[String] = sourceValues.keys.map(_.getURI).toSet

  // not that easy operation with rdf since we cant modify the source, maybe add a filter
  def --(keys: Iterable[String]) = this //new MapValueReader(data -- keys.map(separated.wrap(_, prefix)), prefix, separated)

  def isComplex(key: String) = {
    val pref = separated.wrap(key, prefix)
    if (pref != null && pref.trim.nonEmpty) {
      val prop = model.getProperty(key)
      if (data.hasProperty(prop)) {
        separated.stripPrefix(key, prefix).contains(separated.beginning) && key.startsWith(pref + separated.beginning)
      } else false
    } else false
  }

  def contains(key: String): Boolean = false //(data contains separated.wrap(key, prefix)) || isComplex(key)

  private[this] def stripPrefix(d: Map[String, Any]): Map[String, Any] = {
    if (prefix != null && prefix.trim.nonEmpty) {
      d collect {
        case (k, v) if k startsWith (prefix + separated.beginning) => separated.stripPrefix(k, prefix) -> v
      }
    } else d
  }

}

class SchemaBasedRdfReader(protected val data: Resource, val model: Model, val schema: Frame, val schemaSource: String => Option[Frame], val prefix: String = "", val separated: Separator = by.Dots) extends ValueProvider[Resource] {

  def read(key: String): Either[Throwable, Option[Any]] = allCatch either {
    def readValue0(value: RDFNode, attr: Attribute): Any = (value, attr.typesig) match {
      case (value, "_string") if value.isLiteral => value.asLiteral.getString
      case (value, "_decimal") if value.isLiteral => value.asLiteral.getDouble
      case (value, "_boolean") if value.isLiteral => value.asLiteral.getBoolean
      case (value, x) if value.isLiteral => value.asLiteral.getValue
      case (value, "_iri") if value.isResource => value.asResource.getURI
      case (value, uri) if value.isResource =>
        schemaSource(uri).map { s =>
          new SchemaBasedRdfReader(value.asResource, model, s, schemaSource, key, separated)
        }.get
      case x => x
    }

    for {
      attr <- schema.attributes.filter(_.uri.equals(key)).headOption
      (prop, values) <- sourceValues.filter(_._1.getURI.equals(key)).headOption
    } yield {
      (values, attr.cardinality) match {
        case (List(x), Required) => readValue0(x, attr)
        case (List(x), Optional) => readValue0(x, attr)
        case (l: List[RDFNode], Qualified(min, max)) if (l.size >= min && l.size <= max) =>
            l map (v => readValue0(v, attr))
        case (l: List[RDFNode], MinQualified(min)) if (l.size >= min) =>
            l map (v => readValue0(v, attr))
        case _ =>
          throw new Error("Cardinality violation on %s property %s.".format(data.getURI, prop.getURI))
      }
    }
  }

  def forPrefix(key: String): ValueProvider[Resource] = new SchemaBasedRdfReader(data, model, schema, schemaSource, separated.wrap(key, prefix), separated)

  lazy val values: Resource = data

  protected lazy val sourceValues: Map[Property, List[RDFNode]] = data.listProperties.toList.toList.groupBy(_.getPredicate).mapValues(_.map(_.getObject))

  lazy val targetValues: Map[String, Any] = schema.attributes.map(_.uri).flatMap { k =>
    read(k) match {
      case Right(v) => Some(k -> v)
      case _ => None
    }
  }.toMap

  def keySet: Set[String] = sourceValues.keys.map(_.getURI).toSet

  // not that easy operation with rdf since we cant modify the source, maybe add a filter
  def --(keys: Iterable[String]) = this //new MapValueReader(data -- keys.map(separated.wrap(_, prefix)), prefix, separated)

  def isComplex(key: String) = {
    val pref = separated.wrap(key, prefix)
    if (pref != null && pref.trim.nonEmpty) {
      val prop = model.getProperty(key)
      if (data.hasProperty(prop)) {
        separated.stripPrefix(key, prefix).contains(separated.beginning) && key.startsWith(pref + separated.beginning)
      } else false
    } else false
  }

  def contains(key: String): Boolean = false //(data contains separated.wrap(key, prefix)) || isComplex(key)

  private[this] def stripPrefix(d: Map[String, Any]): Map[String, Any] = {
    if (prefix != null && prefix.trim.nonEmpty) {
      d collect {
        case (k, v) if k startsWith (prefix + separated.beginning) => separated.stripPrefix(k, prefix) -> v
      }
    } else d
  }

}

object Rdf extends App {

  def validate[T](v: ValueProvider[T], s: Frame) = {
    val reqAttrs = s.attributes.filter(_.isRequired).map(_.uri).toSet
    (reqAttrs -- v.keySet).isEmpty
  }

  def debug[T](reader: ValueProvider[T]) = reader.keySet map (k => k -> reader.get(k)) foreach println

  val source = io.Source.fromInputStream(getClass.getResourceAsStream("/recipes.fl"))
  val txt = source.mkString
  source.close

  val schemas = FLogicParser(txt).map(f => f.uri -> f).toMap
  val recipeSchema = schemas("http://food.42dots.com/Recipe")

  val recipes = ModelFactory.createDefaultModel()
  recipes.read(getClass.getResourceAsStream("/recipe.xml"), null)
  val recipeResource = recipes.getResource("http://food.42dots.com/dataset/taaable/Apple_twists")

  val reader = new SchemaBasedRdfReader(recipeResource, recipes, recipeSchema, schemas.get _)

//  debug(reader)
  // traverse to ingredientLines
//    reader.get("http://food.42dots.com/hasIngredientLine").get match {
//      case readers: List[SchemaBasedRdfReader] =>
//        readers foreach debug
//      case _ =>
//    }
//

  def print(values: Map[String, Any], prefix: String = "") {
    def printValue(a: Any) {
      a match {
        case v: SchemaBasedRdfReader => print(v.targetValues, prefix + "  ")
        case v => println(prefix + "  " + v)
      }
    }
    values foreach {
      case (key, Some(l: List[_])) =>
        println(prefix + key + ": ")
        l foreach (v => printValue(v))
      case (key, Some(v)) =>
        println(prefix + key + ": ")
        printValue(v)
      case _ =>
    }
  }

  print(reader.targetValues)

/*

http://www.w3.org/2000/01/rdf-schema#label: Apple twists
http://semantic-mediawiki.org/swivt/1.0#page: http://wikitaaable.loria.fr/index.php/Apple_twists
http://food.42dots.com/canBeEatenAs: http://food.42dots.com/Dessert_dish
http://food.42dots.com/hasIngredientLine:

  http://purl.org/dc/terms/description: 4 tb Ice water (up to 5)
  http://food.42dots.com/ingredient: http://food.42dots.com/Ice_cube
  http://data.nasa.gov/qudt/owl/qudt#unit: http://food.42dots.com/Tblsp
  http://data.nasa.gov/qudt/owl/qudt#numericalValue: 4.0

  http://purl.org/dc/terms/description: 3/4 c White sugar
  http://food.42dots.com/ingredient: http://food.42dots.com/Granulated_sugar
  http://data.nasa.gov/qudt/owl/qudt#unit: http://food.42dots.com/Cup
  http://data.nasa.gov/qudt/owl/qudt#numericalValue: 0.75

  http://purl.org/dc/terms/description: 1/2. shortening
  http://food.42dots.com/ingredient: http://food.42dots.com/Shortening
  http://data.nasa.gov/qudt/owl/qudt#numericalValue: 0.5

  http://purl.org/dc/terms/description: 1 ts Cinnamon
  http://food.42dots.com/ingredient: http://food.42dots.com/Cinnamon
  http://data.nasa.gov/qudt/owl/qudt#unit: http://food.42dots.com/Tsp
  http://data.nasa.gov/qudt/owl/qudt#numericalValue: 1.0

  http://purl.org/dc/terms/description: 2 Tart apples
  http://food.42dots.com/ingredient: http://food.42dots.com/Apple
  http://data.nasa.gov/qudt/owl/qudt#numericalValue: 2.0

  http://purl.org/dc/terms/description: 1/3 c Melted butter
  http://food.42dots.com/ingredient: http://food.42dots.com/Butter
  http://data.nasa.gov/qudt/owl/qudt#unit: http://food.42dots.com/Cup
  http://data.nasa.gov/qudt/owl/qudt#numericalValue: 0.33

  http://purl.org/dc/terms/description: 1 1/2 c Plain flour
  http://food.42dots.com/ingredient: http://food.42dots.com/All-2Dpurpose_flour
  http://data.nasa.gov/qudt/owl/qudt#unit: http://food.42dots.com/Cup
  http://data.nasa.gov/qudt/owl/qudt#numericalValue: 1.5

  http://purl.org/dc/terms/description: 1/2 c Warm water
  http://food.42dots.com/ingredient: http://food.42dots.com/Water
  http://data.nasa.gov/qudt/owl/qudt#unit: http://food.42dots.com/Cup
  http://data.nasa.gov/qudt/owl/qudt#numericalValue: 0.5

  http://purl.org/dc/terms/description: 1/2 ts Salt
  http://food.42dots.com/ingredient: http://food.42dots.com/Salt
  http://data.nasa.gov/qudt/owl/qudt#unit: http://food.42dots.com/Tsp
  http://data.nasa.gov/qudt/owl/qudt#numericalValue: 0.5


*/

}
