package com.pyaanalytics

import com.mongodb.hadoop.{ MongoInputFormat, MongoOutputFormat }
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.bson.{ BasicBSONEncoder, BasicBSONObject, BSONObject }
import epic.preprocess
import rapture.core._
import modes.returnTry
import rapture.json._
import rapture.json.jsonBackends.jackson._
import scala.util.{ Try, Success, Failure }

object PMTokenize {

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Pubmed Word Count")

    val config = new Configuration()
    config.set("mongo.input.uri", "mongodb://10.250.1.31:27017/pubmedtest.pubmedtestcol")
    config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/pubmedtest.wordcount")

    val mongoRDD = sc.newAPIHadoopRDD(config, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])
    val encoder = new BasicBSONEncoder()

    // MongoRDD contrains (ObjectId, BSONObject) tuples
    val countsRDD = mongoRDD.flatMap(arg => {
      // ugly way to handle this, but get on a non-existent key throws NPE
      // also haven't quite figured out how to use try/catch as expression
      val json = Json.parse(arg._2.toString) getOrElse Json("No Abstract")
      json
        .MedlineCitation
        .Article
        .Abstract
        .AbstractText
        .as[String] match {
          case Success(textChunk) => Some(preprocess.preprocess(textChunk))
          case _ => None
        }
      // println(abstractText)
      // str.toLowerCase().replaceAll("[.,!?\n]", " ")
      // .map(word => (word, 1))
      // .reduceByKey((a, b) => a + b)

      // (null, BSONObject) tuples
      /* 
    val saveRDD = countsRDD.map((tuple) => {
      var bson = new BasicBSONObject()
      bson.put("word", tuple._1)
      bson.put("count", tuple._2)
      (null, bson)
     */
    }).count()
    // Only MongoOutputFormat and config are relevant
    // saveRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[MongoOutputFormat[Any, Any]], config)

    println("Processed " + countsRDD.toString + " abstracts.")
    sc.stop()
  }
}
