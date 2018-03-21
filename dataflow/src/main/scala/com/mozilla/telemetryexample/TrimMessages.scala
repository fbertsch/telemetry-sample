package com.mozilla.telemetryexample

import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.json4s.jackson.Serialization.read
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.{compact, render, parse}
import org.json4s.JValue
import com.fasterxml.jackson.databind.ObjectMapper
import com.google.api.services.bigquery.model.TableRow
import scala.collection.JavaConverters._

class JsonToJValue() extends DoFn[String, JValue] {
  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    c.output(parse(c.element()))
  }
}

class JValueToJson() extends DoFn[JValue, String] {
  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    if (c.element() != JNothing) {
      c.output(compact(render(c.element())))
    }
  }
}

class JsonToTableRow() extends DoFn[String, TableRow] {
  val objectMapper = new ObjectMapper()

  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    c.output(objectMapper.readValue(c.element(), classOf[TableRow]))
  }
}

case class PSMessage(data: String, attributes: Map[String,String])

class JsonToPubsubMessage() extends DoFn[String, PubsubMessage] {
  def convert(element: String): PubsubMessage = {
    implicit val formats = DefaultFormats
    val message = read[PSMessage](element)
    new PubsubMessage(message.data.getBytes, message.attributes.asJava)
  }

  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    c.output(convert(c.element()))
  }
}

case class BQField(`type`: String = "RECORD", name: String = "<root>", mode: String = "REQUIRED", fields: List[BQField] = List())

class TrimToSchema(schemaString: String) extends DoFn[JValue, JValue] {
  val rootSchema: BQField = {
    implicit val formats = DefaultFormats
    read[BQField](schemaString)
  }

  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    val trimmed: JValue = trim(c.element())
    if (trimmed != JNothing) c.output(trimmed)
  }

  def trim(message: JValue): JValue = {
    try {
      trim(message, rootSchema)
    } catch {
      // on failure print exception and return JNothing
      case e: IllegalArgumentException => {println(e.getMessage); JNothing}
    }
  }

  def trim(message: JValue, schema: BQField): JValue = {
    if (schema.mode == "REPEATED") {
      // handle array
      message match {
        case JArray(arr) => JArray(
          arr.map { element =>
            trim(element, BQField(schema.`type`, schema.name, "NULLABLE", fields = schema.fields))
          }
        )
        case JNothing => JArray(List())
        case JNull => JArray(List())
        case _ => {
          throw new IllegalArgumentException(s"Required JArray got '${message.getClass.getName}' for field '${schema.name}'")
        }
      }
    } else {
      // handle all non-array types
      {
        schema.`type` match {
          case "DATE" => "STRING"
          case "TIMESTAMP" => "STRING"
          case v => v
        }
      } match {
        case "RECORD" => {
          message match {
            case JObject(obj) => {
              val map = Map(obj.map { f => (f._1 -> f._2) }: _*)
              JObject(obj = schema.fields.map { field =>
                JField(field.name, trim(
                  if (map.contains(field.name)) map(field.name) else JNothing,
                  field
                ))
              })
            }
            case JNull => if (schema.mode == "NULLABLE") JNothing else {
              throw new IllegalArgumentException(s"Required RECORD got null for field '${schema.name}'")
            }
            case JNothing => if (schema.mode == "NULLABLE") JNothing else {
              throw new IllegalArgumentException(s"Required RECORD got nothing for field '${schema.name}'")
            }
            case _ => {
              throw new IllegalArgumentException(s"Required RECORD got '${message.getClass.getName}' for field '${schema.name}'")
            }
          }
        }
        // try to coerce values to primitive types
        case "STRING" => message match {
          case JArray(_) => JString(compact(render(message)))
          case JBool(v) => JString(s"$v")
          case JDecimal(v) => JString(s"$v")
          case JDouble(v) => JString(s"$v")
          case JInt(v) => JString(s"$v")
          case JLong(v) => JString(s"$v")
          case JObject(_) => JString(compact(render(message)))
          case JSet(_) => JString(compact(render(message)))
          case JString(_) => message
          case JNull => if (schema.mode == "NULLABLE") JNothing else {
            throw new IllegalArgumentException(s"Required STRING got null for field '${schema.name}'")
          }
          case JNothing => if (schema.mode == "NULLABLE") JNothing else {
            throw new IllegalArgumentException(s"Required STRING got nothing for field '${schema.name}'")
          }
          case _ => throw new IllegalArgumentException(s"Required STRING got '${message.getClass.getName}' for field '${schema.name}'")
        }
        case "INTEGER" => message match {
          case JArray(v) => JLong(v.length)
          case JBool(v) => JLong(if (v) 1 else 0)
          case JDecimal(v) => JLong(v.toLong)
          case JDouble(v) => JLong(v.toLong)
          case JInt(_) => message
          case JLong(_) => message
          case JObject(v) => JLong(v.length)
          case JSet(v) => JLong(v.size)
          case JString(v) => JLong(v.length)
          case JNull => if (schema.mode == "NULLABLE") JNothing else {
            throw new IllegalArgumentException(s"Required INTEGER got null for field '${schema.name}'")
          }
          case JNothing => if (schema.mode == "NULLABLE") JNothing else {
            throw new IllegalArgumentException(s"Required INTEGER got nothing for field '${schema.name}'")
          }
          case _ => throw new IllegalArgumentException(s"Required INTEGER got '${message.getClass.getName}' for field '${schema.name}'")
        }
        case "FLOAT" => message match {
          case JArray(v) => JDouble(v.length)
          case JBool(v) => JDouble(if (v) 1 else 0)
          case JDecimal(_) => message
          case JDouble(_) => message
          case JInt(v) => JDouble(v.toDouble)
          case JLong(v) => JDouble(v)
          case JObject(v) => JDouble(v.length)
          case JSet(v) => JDouble(v.size)
          case JString(v) => JDouble(v.length)
          case JNull => if (schema.mode == "NULLABLE") JNothing else {
            throw new IllegalArgumentException(s"Required FLOAT got null for field '${schema.name}'")
          }
          case JNothing => if (schema.mode == "NULLABLE") JNothing else {
            throw new IllegalArgumentException(s"Required FLOAT got nothing for field '${schema.name}'")
          }
          case _ => throw new IllegalArgumentException(s"Required FLOAT got '${message.getClass.getName}' for field '${schema.name}'")
        }
        case "BOOLEAN" => message match {
          case JArray(v) => JBool(v.length == 0)
          case JBool(_) => message
          case JDecimal(v) => JBool(v == 0)
          case JDouble(v) => JBool(v == 0)
          case JInt(v) => JBool(v == 0)
          case JLong(v) => JBool(v == 0)
          case JObject(v) => JBool(v.length == 0)
          case JSet(v) => JBool(v.size == 0)
          case JString(v) => JBool(v == "")
          case JNull => if (schema.mode == "NULLABLE") JNothing else {
            throw new IllegalArgumentException(s"Required BOOLEAN got null for field '${schema.name}'")
          }
          case JNothing => if (schema.mode == "NULLABLE") JNothing else {
            throw new IllegalArgumentException(s"Required BOOLEAN got nothing for field '${schema.name}'")
          }
          case _ => throw new IllegalArgumentException(s"Required BOOLEAN got '${message.getClass.getName}' for field '${schema.name}'")
        }
        // unsupported type
        case _ => throw new IllegalArgumentException(s"Unknown bigquery field type '${schema.`type`}'")
      }
    }
  }
}
