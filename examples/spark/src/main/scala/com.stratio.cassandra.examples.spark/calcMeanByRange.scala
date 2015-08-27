package com.stratio.cassandra.examples.spark

/* SimpleApp.scala */


import com.datastax.spark.connector._
import com.stratio.cassandra.lucene.search.SearchBuilders._
import org.apache.spark.{SparkConf, SparkContext}


object calcMeanByRange {
  def main(args: Array[String]) {

    val KEYSPACE: String = "spark_example_keyspace"
    val TABLE: String = "sensors"
    val INDEX_COLUMN_CONSTANT: String = "lucene"
    var totalMean = 0.0f

    val luceneQuery: String = search.refresh(true).filter(range("temp_value").includeLower(true).lower(30.0f)).toJson

    val sc : SparkContext = new SparkContext(new SparkConf)

    val tempRdd=sc.cassandraTable(KEYSPACE, TABLE).select("temp_value").where(INDEX_COLUMN_CONSTANT+ "= ?",luceneQuery).map[Float]((row)=>row.getFloat("temp_value"))

    val totalNumElems: Long =tempRdd.count()

    if (totalNumElems>0) {
      val pairTempRdd = tempRdd.map(s => (1, s))
      val totalTempPairRdd = pairTempRdd.reduceByKey((a, b) => a + b)
      totalMean = totalTempPairRdd.first()._2 / totalNumElems.toFloat
    }

    println("Mean calculated on range type data, mean: "+totalMean.toString+" , numRows: "+ totalNumElems.toString)
  }
}