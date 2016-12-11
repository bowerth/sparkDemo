package models

// case class tlclass(chapter: String,
//                    rep: String,
//                    tyear: String,
//                    curr: String,
//                    hsrep: String,
//                    flow: String,
//                    repcurr: String,
//                    comm: String,
//                    prt: String,
//                    weight: String,
//                    qty: String,
//                    qunit: Int,
//                    tvalue: String,
//                    est: String,
//                    ht: String)

case class tlclass(chapter: Int,
                   rep: Int,
                   tyear: Int,
                   curr: String,
                   hsrep: String,
                   flow: Int,
                   repcurr: String,
                   comm: Int,
                   prt: Int,
                   weight: String,
                   qty: String,
                   qunit: Int,
                   tvalue: Double,
                   est: Int,
                   ht: Int)

 // |-- chapter: integer (nullable = true)
 // |-- rep: integer (nullable = true)
 // |-- tyear: integer (nullable = true)
 // |-- curr: string (nullable = true)
 // |-- hsrep: string (nullable = true)
 // |-- flow: integer (nullable = true)
 // |-- repcurr: string (nullable = true)
 // |-- comm: integer (nullable = true)
 // |-- prt: integer (nullable = true)
 // |-- weight: string (nullable = true)
 // |-- qty: string (nullable = true)
 // |-- qunit: integer (nullable = true)
 // |-- tvalue: double (nullable = true)
 // |-- est: integer (nullable = true)
 // |-- ht: integer (nullable = true)
