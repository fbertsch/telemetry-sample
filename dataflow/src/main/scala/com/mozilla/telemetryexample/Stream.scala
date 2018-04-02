package com.mozilla.telemetryexample

import scala.io.Source
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.options.{Default, Description, PipelineOptions, PipelineOptionsFactory, ValueProvider}
import org.apache.beam.sdk.transforms.{Combine, ParDo}
import org.apache.beam.sdk.transforms.windowing.{FixedWindows, Window}
import org.apache.beam.sdk.values.PCollection
import org.joda.time.{Duration, Instant}

object Stream {
  var schemaFile = "schemas/main.4.bigquery.json"
  def schemaString = Source.fromResource(schemaFile).mkString

  def main(args: Array[String]): Unit = {
    val options = PipelineOptionsFactory
      .fromArgs(args: _*)
      .withValidation()
      .as(classOf[MainPingOptions])

    schemaFile = options.getSchema

    if (schemaFile == "") {
      if (options.getOutput.matches("bigquery://(.*:)?.*\\..*")) {
        throw new IllegalArgumentException("can't write to bigquery without schema")
      }
    }

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
    val windowed = records
      .apply(Window.into[String](FixedWindows.of(Duration.standardMinutes(options.getWindowSize))))
    if (schemaFile != "" || options.getAggregate) {
      val jvalued = windowed.apply(ParDo.of(new JsonToJValue()))
      val transformed = if (schemaFile != "" && options.getAggregate) {
        jvalued
          .apply(ParDo.of(new TrimToSchema(schemaString)))
          .apply(Combine.globally(new Aggregate()).withoutDefaults())
      } else if (schemaFile != "") {
        jvalued.apply(ParDo.of(new TrimToSchema(schemaString)))
      } else {
        jvalued.apply(Combine.globally(new Aggregate()).withoutDefaults())
      }
      transformed.apply(ParDo.of(new JValueToJson()))
    } else {
      windowed
    }
  }

  def writeOutput(pipeline: PCollection[String], options: MainPingOptions) = {
    val output: String = options.getOutput
    if (output.matches("pubsub://projects/.*/topics/.*")) {
      pipeline
        .apply(PubsubIO.writeStrings().to(output.substring(9)))
    } else if (output.matches("pubsubMessages://projects/.*/topics/.*")) {
      pipeline
        .apply(ParDo.of(new JsonToPubsubMessage()))
        .apply(PubsubIO.writeMessages().to(output.substring(17)))
    } else if (output.matches("bigquery://(.*:)?.*\\..*")) {
      pipeline
        .apply(ParDo.of(new JsonToTableRow()))
        .apply(BigQueryIO
          .writeTableRows()
          .to(output.substring(11))
          .withJsonSchema(schemaString)
          .withCustomGcsTempLocation(new StringProvider(options.getGcsTempLocation))
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
          .withJsonTimePartitioning(new StringProvider(options.getBigQueryTimePartitioning))
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND))
    } else if (output.matches("-|stdout")) {
      pipeline.apply(ParDo.of(new PrintString()))
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
  @Required
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

  @Description("Number of output shards when writing to files")
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

  @Description("Name of schema resource file")
  @Default.String("schemas/main.4.bigquery.json")
  def getSchema: String
  def setSchema(path: String)

  @Description("Whether or not to compute aggregates over windows")
  @Default.Boolean(false)
  def getAggregate: Boolean
  def setAggregate(value: Boolean): Unit
}

class StringProvider(value: String) extends ValueProvider[String] {
  def get: String = value
  def isAccessible: Boolean = true
}
