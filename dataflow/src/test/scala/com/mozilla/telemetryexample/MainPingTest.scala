package com.mozilla.telemetryexample

import scala.io.Source
import org.scalatest.{FlatSpec, Matchers}
import org.apache.beam.sdk.transforms.DoFnTester
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class MainPingTest extends FlatSpec with Matchers {
  "json" must "parse" in {
    val stringToJValueTester = DoFnTester.of(new StringToJValue())
    val trimTester = DoFnTester.of(new TrimToSchema(MainPing.schemaString))
    val jvalueToStringTester = DoFnTester.of(new JValueToString())
    val stringToTableRowTester = DoFnTester.of(new StringToTableRow())

    val lines = Source.fromResource("lines.json").getLines.toList.asJava
    val jvalueLines = stringToJValueTester.processBundle(lines)
    val trimmedLines = trimTester.processBundle(jvalueLines)
    val stringLines = jvalueToStringTester.processBundle(trimmedLines)
    val tablerowLines = stringToTableRowTester.processBundle(stringLines)

    val expects = stringToTableRowTester.processBundle(
      Source.fromResource("expect.lines.json").getLines.toList.asJava)

    for ( (expect, tablerow) <- expects zip tablerowLines ) {
      tablerow should be (expect)
    }
  }
}
