package models

// eurostat Bulk Download
case class ncclass(DECLARANT: String,
                   PARTNER: String,
                   PRODUCT_NC: String,
                   FLOW: String,
                   STAT_REGIME: String,
                   PERIOD: String,
                   // precision and scale of decimal type
                   // according to comext support
                   VALUE_1000ECU: String, // Double,
                   QUANTITY_TON: String, // Double,
                   SUP_QUANTITY: String) // Double

