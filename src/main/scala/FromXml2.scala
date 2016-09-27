package org.fao.trade.xml

class Uncs(
  // Group
  var rpt: Integer,
  var time: Integer,
  var cl: String,
  var unit_mult: Integer,
  var decimals: Integer,
  var currency: String,
  var freq: String,
  var time_format: String,
  var reported_classification: String,
  var flows_in_dataset: String,
  // Section
  var tf: Integer,
  var reported_currency: String,
  var conversion_factor: Double,
  var valuation: String,
  var trade_system: String,
  var partner: String,
  // Obs
  var cc: Integer,
  var prt: Integer,
  var netweight: Double,
  var qty: Double,
  var qu : Integer,
  var value: Double,
  var est: Integer,
  var ht: Integer
) { // tf: Double

  // (a) convert Uncs fields to XML
  def toXml = {
    <uncs>
      <rpt>{rpt}</rpt>
      <prt>{prt}</prt>
      <tf>{tf}</tf>
    </uncs>
  }

  override def toString =
    s"rpt: $rpt, time: $time, cl: $cl, unit_mult: $unit_mult, decimals: $decimals, currency: $currency, freq: $freq, time_format: $time_format, reported_classification: $reported_classification, flows_in_dataset: $flows_in_dataset, tf: $tf, reported_currency: $reported_currency, conversion_factor: $conversion_factor, valuation: $valuation, trade_system: $trade_system, partner: $partner, cc: $cc, prt: $prt, netweight: $netweight, qty: $qty, qu: $qu, value: $value, est: $est, ht: $ht"
  // s"rpt: $rpt, prt: $prt, tf: $tf"


}

object Uncs {

  // (b) convert XML to a Uncs
  // def fromXml(node: scala.xml.Node): Uncs = {
  //   val rpt = ((node \\ "Group")(0) \ "@RPT").text.toInt
  //   val prt = ((node \\ "Obs")(0) \ "@PRT").text.toInt
  //   // val tf = (node \ "tf").text.toDouble
  //   val tf = ((node \\ "Section")(0) \ "@TF").text.toInt
  //   new Uncs(rpt, prt, tf)
  // }

  def fromXml(node: scala.xml.Node): Uncs = {

    val rpt = ((node \\ "Group")(0) \ "@RPT").text.toInt
    val time = ((node \\ "Group")(0) \ "@time").text.toInt
    val cl = ((node \\ "Group")(0) \ "@CL").text
    val unit_mult = ((node \\ "Group")(0) \ "@UNIT_MULT").text.toInt
    val decimals = ((node \\ "Group")(0) \ "@DECIMALS").text.toInt
    val currency = ((node \\ "Group")(0) \ "@CURRENCY").text
    val freq = ((node \\ "Group")(0) \ "@FREQ").text
    val time_format = ((node \\ "Group")(0) \ "@TIME_FORMAT").text
    val reported_classification = ((node \\ "Group")(0) \ "@REPORTED_CLASSIFICATION").text
    val flows_in_dataset = ((node \\ "Group")(0) \ "@FLOWS_IN_DATASET").text
    // Section
    val tf = ((node \\ "Section")(0) \ "@TF").text.toInt
    val reported_currency = ((node \\ "Section")(0) \ "@REPORTED_CURRENCY").text
    val conversion_factor = ((node \\ "Section")(0) \ "@CONVERSION_FACTOR").text.toDouble
    val valuation = ((node \\ "Section")(0) \ "@VALUATION").text
    val trade_system = ((node \\ "Section")(0) \ "@TRADE_SYSTEM").text
    val partner = ((node \\ "Section")(0) \ "@PARTNER").text
    // Obs
    val cc = ((node \\ "Obs")(0) \ "@CC-H2").text.toInt
    val prt = ((node \\ "Obs")(0) \ "@PRT").text.toInt
    val netweight = ((node \\ "Obs")(0) \ "@netweight").text.toDouble
    val qty = ((node \\ "Obs")(0) \ "@qty").text.toDouble
    val qu  = ((node \\ "Obs")(0) \ "@QU").text.toInt
    val value = ((node \\ "Obs")(0) \ "@value").text.toDouble
    val est = ((node \\ "Obs")(0) \ "@EST").text.toInt
    val ht = ((node \\ "Obs")(0) \ "@HT").text.toInt

    new Uncs(
      rpt = rpt,
      time = time,
      cl = cl,
      unit_mult = unit_mult,
      decimals = decimals,
      currency = currency,
      freq = freq,
      time_format = time_format,
      reported_classification = reported_classification,
      flows_in_dataset = flows_in_dataset,
      // Section
      tf = tf,
      reported_currency = reported_currency,
      conversion_factor = conversion_factor,
      valuation = valuation,
      trade_system = trade_system,
      partner = partner,
      // Obs
      cc = cc,
      prt = prt,
      netweight = netweight,
      qty = qty,
      qu = qu,
      value = value,
      est = est,
      ht = ht
    )

  }
}
