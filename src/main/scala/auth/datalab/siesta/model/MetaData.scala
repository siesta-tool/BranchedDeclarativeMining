package auth.datalab.siesta.model

case class MetaData(var traces: Long, var events: Long, var pairs: Long,
                    lookback: Int, var has_previous_stored: Boolean,
                    filename: String, streaming: Boolean,log_name: String, mode: String, compression: String,
                    var start_ts:String, var last_ts:String,
                    var last_declare_mined:String) extends Serializable
