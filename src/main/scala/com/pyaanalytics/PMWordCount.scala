package com.pyaanalytics

import com.mongodb.hadoop.{MongoInputFormat, MongoOutputFormat}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.conf.Configuration
import org.bson.BSONObject
import org.bson.BasicBSONObject
import org.bson.BSONEncoder

object PMWordCount {

  def main(args: Array[String]) {

    val sc = new SparkContext("local", "Pubmed Word Count")
    
    val config = new Configuration()
    config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/pubmedtest.pubmedtestcol")
    config.set("mongo.output.uri", "mongodb://127.0.0.1:27017/pubmedtest.wordcount")

    val mongoRDD = sc.newAPIHadoopRDD(config, classOf[MongoInputFormat], classOf[Object], classOf[BSONObject])

    // path is the path in the json tree to get the item we want
    val path = List("MedlineCitation", "Article", "Abstract", "AbstractText")
    val (allButLast, last) = path splitAt -1
    val badEntries = sc.accumulator(0, "Entries skipped due to key error")

    // MongoRDD contrains (ObjectId, BSONObject) tuples
    val countsRDD = mongoRDD.flatMap(arg => {
      // ugly way to handle this, but get on a non-existent key throws NPE
      // also haven't quite figured out how to use try/catch as expression
      var abst = try {
        val end = allButLast.foldLeft(arg._2)((b, a) => b.get(a).asInstanceOf[BSONObject])
        val str = end.get(last(0)).toString
        str.toLowerCase().replaceAll("[.,!?\n]", " ")
      } catch {
        case e: Exception => {
          badEntries += 1
          ""
        }
      }
      abst.split(" ")
    })
    .map(word => (word, 1))
    .reduceByKey((a, b) => a + b)

    // (null, BSONObject) tuples
    val saveRDD = countsRDD.map((tuple) => {
      var bson = new BasicBSONObject()
      bson.put("word", tuple._1)
      bson.put("count", tuple._2)
      (null, bson)
    })
    
    // Only MongoOutputFormat and config are relevant
    saveRDD.saveAsNewAPIHadoopFile("file:///bogus", classOf[Any], classOf[Any], classOf[MongoOutputFormat[Any, Any]], config)

    sc.stop()
    
    // print how many we had to skip
    println("Task done, skipped " + badEntries.value.toString + " entries.")
  }
}
