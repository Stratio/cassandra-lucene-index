/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.cassandra.examples.spark

import com.datastax.spark.connector._
import com.stratio.cassandra.lucene.search.SearchBuilders._
import org.apache.spark.{SparkConf, SparkContext}


object calcMeanByType {
  def main(args: Array[String]) {

    val KEYSPACE: String = "spark_example_keyspace"
    val TABLE: String = "sensors"
    val INDEX_COLUMN_CONSTANT: String = "lucene"
    var totalMean = 0.0f

    val luceneQuery: String = search.refresh(true).filter(`match`("sensor_type", "plane")).toJson

    val sc : SparkContext = new SparkContext(new SparkConf)

    val tempRdd=sc.cassandraTable(KEYSPACE, TABLE).select("temp_value").where(INDEX_COLUMN_CONSTANT+ "= ?",luceneQuery).map[Float]((row)=>row.getFloat("temp_value"))

    val totalNumElems: Long =tempRdd.count()

    if (totalNumElems>0) {
      val pairTempRdd = tempRdd.map(s => (1, s))
      val totalTempPairRdd = pairTempRdd.reduceByKey((a, b) => a + b)
      totalMean = totalTempPairRdd.first()._2 / totalNumElems.toFloat
    }

    println("Mean calculated on type query data, mean: "+totalMean.toString+", numRows: "+ totalNumElems.toString)
  }
}