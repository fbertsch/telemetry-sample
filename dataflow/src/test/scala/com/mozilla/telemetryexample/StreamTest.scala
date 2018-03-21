package com.mozilla.telemetryexample

import scala.io.Source
import org.scalatest.{FlatSpec, Matchers}
import org.apache.beam.sdk.transforms.DoFnTester
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage
import org.json4s.jackson.Serialization.read
import org.json4s.DefaultFormats

case class PSMessage(data: String, attributes: Map[String,String])

class StreamTest extends FlatSpec with Matchers {
  "json" must "parse" in {
    val jsonToJValueTester = DoFnTester.of(new JsonToJValue())
    val trimTester = DoFnTester.of(new TrimToSchema(Stream.schemaString))
    val jvalueToJsonTester = DoFnTester.of(new JValueToJson())
    val jsonToTableRowTester = DoFnTester.of(new JsonToTableRow())

    val lines = Source.fromResource("lines.json").getLines.toList.asJava
    val jvalueLines = jsonToJValueTester.processBundle(lines)
    val trimmedLines = trimTester.processBundle(jvalueLines)
    val jsonLines = jvalueToJsonTester.processBundle(trimmedLines)
    val tablerowLines = jsonToTableRowTester.processBundle(jsonLines)

    val expects = jsonToTableRowTester.processBundle(
      Source.fromResource("expect.lines.json").getLines.toList.asJava)

    for ( (expect, tablerow) <- expects zip tablerowLines ) {
      tablerow should be (expect)
    }
  }

  "pubsubMessages" must "parse" in {
    val jsonToPubsubMessageTester = DoFnTester.of(new JsonToPubsubMessage())

    val values = jsonToPubsubMessageTester.processBundle(List(
      """{"data":"","attributes":{}}""").asJava)
  }
}
