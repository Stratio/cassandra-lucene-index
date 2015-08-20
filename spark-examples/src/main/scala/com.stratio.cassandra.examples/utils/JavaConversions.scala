package com.stratio.cassandra.examples.utils

import com.stratio.cassandra.lucene.search.condition.builder.ConditionBuilder

/**
 * Created by eduardoalonso on 20/08/15.
 */
object JavaConversions {

  import scala.language.implicitConversions

  implicit def conditionBuilder[K,V](cb: ConditionBuilder): ConditionBuilder[Nothing,Nothing]  =
    cb.asInstanceOf[ConditionBuilder[Nothing,Nothing]]

}
