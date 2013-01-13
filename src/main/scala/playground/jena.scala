package playground

import scala.collection.JavaConversions._
import util.control.Exception._
import com.hp.hpl.jena.rdf.model._

class SchemaBasedRdfReader(protected val data: Resource, val model: Model, val schema: Frame, val schemaSource: String => Option[Frame], val prefix: String = "", val separated: Separator = by.Dots) extends ValueProvider[Resource] {

  // reads a property value from a Resource and does early local validation (cardinality, literal types)
  def read(key: String): Either[Throwable, Option[Any]] = allCatch either {

    // type-cast value, use typesig information
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
      case _ => throw new Error("Could not determine type.")
    }

    // check cardinality here
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

  // validation
  //
  // for all attributes:
  // - check if all required attributes are there
  // - check if cardinality is applicable (get returns None if not)
  // - check if literal type compatible (again get returns None if not)
  // - validate nested frames if required
  def validate: Boolean = {
    def attributeTypeIsOk(a: Attribute): Boolean = {
      get(a.uri) match {
        case Some(v: SchemaBasedRdfReader) =>
          schemaSource(a.typesig).map(s => v.validate).getOrElse(false)
        case Some(v: List[_]) if !v.isEmpty =>
          v(0) match {
            case x: SchemaBasedRdfReader =>
              schemaSource(a.typesig).map(s => v.asInstanceOf[List[SchemaBasedRdfReader]].forall(_.validate)).getOrElse(false)
            case _ => true
          }
        case Some(v) => true
        case None => !a.isRequired
      }
    }

    schema.attributes.forall(attributeTypeIsOk)
  }
}

object Uris {
  val dcDescription = "http://purl.org/dc/terms/description"
  val rdfType = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
  val rdfsLabel = "http://www.w3.org/2000/01/rdf-schema#label"
  val qudtUnit = "http://data.nasa.gov/qudt/owl/qudt#unit"
  val qudtNumericalValue = "http://data.nasa.gov/qudt/owl/qudt#numericalValue"
  val smwPage = "http://semantic-mediawiki.org/swivt/1.0#page"
  val recipeHasIngredientLine = "http://food.42dots.com/hasIngredientLine"
  val recipeIngredient = "http://food.42dots.com/ingredient"
  val recipeCanBeEatenAs = "http://food.42dots.com/canBeEatenAs"
  val recipe = "http://food.42dots.com/Recipe"
}

object Rdf extends App {

  def print(values: ValueProvider[_], prefix: String = "") {
    def printValue(a: Any) {
      a match {
        case v: ValueProvider[_] => print(v, prefix + "  ")
        case v => println(prefix + "  " + v)
      }
    }

    values.keySet foreach { key =>
      (key, values.get(key)) match {
        case (key, Some(l: List[_])) =>
          println(prefix + key + ": ")
          l foreach (v => printValue(v))
        case (key, Some(v)) =>
          println(prefix + key + ": ")
          printValue(v)
        case _ =>
      }
    }
  }

  val source = io.Source.fromInputStream(getClass.getResourceAsStream("/recipes.fl"))
  val txt = source.mkString
  source.close

  val schemas = FLogicParser(txt).map(f => f.uri -> f).toMap
  val schema = schemas(Uris.recipe)

  val rdfModel = ModelFactory.createDefaultModel()
  rdfModel.read(getClass.getResourceAsStream("/recipes.xml"), null)

  val recipes = rdfModel.listStatements(null, rdfModel.getProperty(Uris.rdfType), rdfModel.getResource(Uris.recipe)).map(_.getSubject)
  recipes.filterNot { res =>
    val reader = new SchemaBasedRdfReader(res, rdfModel, schema, schemas.get _)
    reader.validate
  } foreach println

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
