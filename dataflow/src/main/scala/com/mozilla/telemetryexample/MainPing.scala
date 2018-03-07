package com.mozilla.telemetryexample

import scala.io.Source
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory, ValueProvider}
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.values.PCollection
import org.joda.time.{Duration, Instant}

object MainPing {
  val schemaFile = "schemas/main.4.bigquery.json"
  val schemaString = Source.fromResource(schemaFile).mkString

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[MainPingOptions])

    val pipeline = Pipeline.create(options)

    val raw = readInput(pipeline, options)
    val records = processRecords(raw, options)
    writeOutput(records, options)

    pipeline.run().waitUntilFinish()
  }

  def readInput(pipeline: Pipeline, options: MainPingOptions): PCollection[String] = {
    val input: String = options.getInput
    if (input.matches("pubsub://projects/.*/subscriptions/.*")) {
      pipeline.apply(PubsubIO.readStrings().fromSubscription(input.substring(9)))
    } else if (input.matches("pubsub://projects/.*/topics/.*")) {
      pipeline.apply(PubsubIO.readStrings().fromTopic(input.substring(9)))
    } else {
      pipeline.apply(TextIO.read.from(input))
    }
  }

  def processRecords(records: PCollection[String], options: MainPingOptions): PCollection[String] = {
    records
      .apply(ParDo.of(new StringToJValue()))
      .apply(ParDo.of(new TrimToSchema(schemaString)))
      .apply(ParDo.of(new JValueToString()))
      .apply(Window.into[String](FixedWindows.of(Duration.standardMinutes(options.getWindowSize))))
  }

  def writeOutput(pipeline: PCollection[String], options: MainPingOptions) = {
    val output: String = options.getOutput
    if (output.matches("pubsub://projects/.*/topics/.*")) {
      pipeline
        .apply(PubsubIO.writeStrings().to(output.substring(9)))
    } else if (output.matches("bigquery://(.*:)?.*\\..*")) {
      pipeline
        .apply(ParDo.of(new StringToTableRow()))
        .apply(BigQueryIO
          .writeTableRows()
          .to(output.substring(11))
          .withJsonSchema(schemaString)
          .withCustomGcsTempLocation(new StringProvider(options.getGcsTempLocation))
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
          .withJsonTimePartitioning(new StringProvider(options.getBigQueryTimePartitioning))
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND))
    } else {
      pipeline.apply(TextIO
        .write
        .to(output)
        .withWindowedWrites()
        .withNumShards(options.getOutputShards))
    }
  }
}

trait MainPingOptions extends PipelineOptions {
  @Description("Input to read from")
  @Default.String("src/test/resources/lines.json")
  def getInput: String
  def setInput(path: String)

  @Description("Output to write to")
  @Required
  def getOutput: String
  def setOutput(path: String)

  @Description("Fixed window duration, in minutes")
  @Default.Long(1)
  def getWindowSize: Long
  def setWindowSize(value: Long): Unit

  @Description("Number of output shards when writing to a files")
  @Default.Integer(1)
  def getOutputShards: Int
  def setOutputShards(num: Int)

  @Description("Spec for time partitioning if writing to bigquery")
  @Default.String("""{"field":"submission_date","type":"DAY"}""")
  def getBigQueryTimePartitioning: String
  def setBigQueryTimePartitioning(path: String)

  @Description("Gcs temp location if writing to bigquery")
  @Default.String("gs://mozilla-data-poc/telemetry_example/gcstmp")
  def getGcsTempLocation: String
  def setGcsTempLocation(path: String)
}

class StringProvider(value: String) extends ValueProvider[String] {
  def get: String = value
  def isAccessible: Boolean = true
}
