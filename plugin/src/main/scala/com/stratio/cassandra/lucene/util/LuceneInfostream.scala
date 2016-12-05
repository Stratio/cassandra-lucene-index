package com.stratio.cassandra.lucene.util

import java.io.IOException

import org.apache.lucene.util.InfoStream
import org.slf4j.Logger

/**
  * @author Eduardo Alonso { @literal <eduardoalonso@stratio.com>}
  */
class LuceneInfoStream extends InfoStream with Logging {

  override def close(): Unit = {}

  override def isEnabled(component: String): Boolean = true

  override def message(component: String, message: String): Unit = {
    logger.debug("{}: {}",component,message)


  }
}
