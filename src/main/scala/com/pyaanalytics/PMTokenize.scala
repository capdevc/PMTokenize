package com.pyaanalytics

import com.mongodb.hadoop.{ MongoInputFormat, MongoOutputFormat }
import com.mongodb.hadoop.io.MongoUpdateWritable
import com.novus.salat._
import com.novus.salat.annotations._
import com.novus.salat.global._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.bson.{ BasicBSONEncoder, BasicBSONObject, BSONObject }
import org.bson.types.ObjectId
import epic.preprocess
import rapture.core._
import modes.returnTry
import rapture.json._
import rapture.json.jsonBackends.json4s._
import scala.util.{ Try, Success, Failure }

object PMTokenize {

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Pubmed Tokenizer")

    val config = new Configuration()
    config.set("mongo.input.uri", "mongodb://10.250.1.31:27017/pubmedtest.pubmedtestcol")
    // output uri doesn't matter here since we're writing back to the same db.
    config.set("mongo.output.uri", "mongodb://10.250.1.31:27017/pubmedtest.pubmedtestcol")

    val mongoRDD = sc.newAPIHadoopRDD(
      config,
      classOf[MongoInputFormat],
      classOf[Object],
      classOf[BSONObject]
    )

    // mongoRDD contains (ObjectId, BSONObject) tuples
    val tokensRDD = mongoRDD.flatMap(arg => {
      val json = Json.parse(arg._2.toString) getOrElse Json("No Abstract")
      json
        .MedlineCitation
        .Article
        .Abstract
        .AbstractText
        .as[String] match {
          case Success(textChunk) => {
            val tokens = preprocess.preprocess(textChunk).toArray map (_.toArray)
            val query = new BasicBSONObject("_id", arg._1)
            val update = new BasicBSONObject("$set", (new BasicBSONObject("AbstractTokens", tokens)))
            val muw = new MongoUpdateWritable(query, update, false, true)
            Some(null, muw)
          }
          case _ => None
        }
    })

    val abstractCount = tokensRDD.count()

    tokensRDD.saveAsNewAPIHadoopFile(
      "file:///bogus",
      classOf[Any],
      classOf[Any],
      classOf[MongoOutputFormat[Any, Any]],
      config
    )

    println("Found and tokenized " + abstractCount + " abstracts.")
    sc.stop()
  }
}
