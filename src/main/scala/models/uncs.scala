package models

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

  def fromXml(node: scala.xml.Node, group: Integer, section: Integer, obs: Integer): Uncs = {


    // val url = "http://comtrade.un.org/ws/getsdmxtarifflinev1.aspx?px=H2&y=2005,2006&r=400&rg=1&p=392&cc=442190900&comp=false"
    // val filename = "data/TariffLineSdmx.xml"
    // utils.sdmx.fileDownloader(url = url, filename = filename)

    // val xmlFile = new java.io.File(filename)
    // val node = scala.xml.XML.loadFile(xmlFile)

    // break up into sub-classes: group, section, obs
    val rpt = ((node \\ "Group")(group) \ "@RPT").text.toInt
    val time = ((node \\ "Group")(group) \ "@time").text.toInt
    val cl = ((node \\ "Group")(group) \ "@CL").text
    val unit_mult = ((node \\ "Group")(group) \ "@UNIT_MULT").text.toInt
    val decimals = ((node \\ "Group")(group) \ "@DECIMALS").text.toInt
    val currency = ((node \\ "Group")(group) \ "@CURRENCY").text
    val freq = ((node \\ "Group")(group) \ "@FREQ").text
    val time_format = ((node \\ "Group")(group) \ "@TIME_FORMAT").text
    val reported_classification = ((node \\ "Group")(group) \ "@REPORTED_CLASSIFICATION").text
    val flows_in_dataset = ((node \\ "Group")(group) \ "@FLOWS_IN_DATASET").text
    // Section
    val tf = ((node \\ "Section")(section) \ "@TF").text.toInt
    val reported_currency = ((node \\ "Section")(section) \ "@REPORTED_CURRENCY").text
    val conversion_factor = ((node \\ "Section")(section) \ "@CONVERSION_FACTOR").text.toDouble
    val valuation = ((node \\ "Section")(section) \ "@VALUATION").text
    val trade_system = ((node \\ "Section")(section) \ "@TRADE_SYSTEM").text
    val partner = ((node \\ "Section")(section) \ "@PARTNER").text
    // Obs
    val cc = ((node \\ "Obs")(obs) \ "@CC-H2").text.toInt
    val prt = ((node \\ "Obs")(obs) \ "@PRT").text.toInt
    val netweight = ((node \\ "Obs")(obs) \ "@netweight").text.toDouble
    val qty = ((node \\ "Obs")(obs) \ "@qty").text.toDouble
    val qu  = ((node \\ "Obs")(obs) \ "@QU").text.toInt
    val value = ((node \\ "Obs")(obs) \ "@value").text.toDouble
    val est = ((node \\ "Obs")(obs) \ "@EST").text.toInt
    val ht = ((node \\ "Obs")(obs) \ "@HT").text.toInt

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
