package models

// Eurostat SWS
case class esclass(chapter: String,
                   declarant: String,
                   partner: String,
                   product_nc: String,
                   flow: String,
                   stat_regime: String,
                   period: String,
                   // precision and scale of decimal type
                   // according to comext support
                   value_1k_euro: String, // Double,
                   qty_ton: String, // Double,
                   sup_quantity: String) // Double
