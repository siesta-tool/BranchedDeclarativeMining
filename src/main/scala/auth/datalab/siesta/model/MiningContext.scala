package auth.datalab.siesta.model

import org.apache.spark.sql.Dataset
import org.apache.spark.broadcast.Broadcast

/**
 * Mining context that encapsulates common parameters for constraint extraction
 */
case class MiningContext(
  metaData: MetaData,
  affectedEvents: Dataset[Event],
  bEvolvedTracesBounds: Broadcast[scala.collection.Map[String, (Int, Int)]],
  bTraceIds: Broadcast[Set[String]],
  newEvents: Dataset[Event],
  allEventTypes: Set[String],
  totalTraces: Long
)
