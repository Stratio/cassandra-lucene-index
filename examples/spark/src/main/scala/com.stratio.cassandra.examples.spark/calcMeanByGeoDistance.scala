package com.stratio.cassandra.examples.spark

/* SimpleApp.scala */


import com.datastax.spark.connector._
import com.stratio.cassandra.lucene.search.SearchBuilders._
import org.apache.spark.{SparkConf, SparkContext}


object calcMeanByGeoDistance {
  def main(args: Array[String]) {

    val KEYSPACE: String = "spark_example_keyspace"
    val TABLE: String = "sensors"
    val INDEX_COLUMN_CONSTANT: String = "lucene"
    var totalMean = 0.0f

    val luceneQuery = search.refresh(true).filter(geoDistance("place", 0.0f, 0.0f, "100000km")).toJson

    val sc : SparkContext = new SparkContext(new SparkConf)

    val tempRdd=sc.cassandraTable(KEYSPACE, TABLE).select("temp_value").where(INDEX_COLUMN_CONSTANT+ "= ?",luceneQuery).map[Float]((row)=>row.getFloat("temp_value"))

    val totalNumElems: Long =tempRdd.count()

    if (totalNumElems>0) {
      val pairTempRdd = tempRdd.map(s => (1, s))
      val totalTempPairRdd = pairTempRdd.reduceByKey((a, b) => a + b)
      totalMean = totalTempPairRdd.first()._2 / totalNumElems.toFloat
    }

    println("Mean calculated on GeoDistance data, mean: "+totalMean.toString+" , numRows: "+totalNumElems.toString)
  }
}