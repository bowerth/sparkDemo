package org.fao.trade.xml

class Uncs(var reporter: String, var time: String, var cl: String) {

  // (a) convert Uncs fields to XML
  def toXml = {
    <uncs>
      <reporter>{reporter}</reporter>
      <time>{time}</time>
      <cl>{cl}</cl>
    </uncs>
  }

  override def toString = 
    s"reporter: $reporter, time: $time, cl: $cl"

}

object Uncs {

  // (b) convert XML to a Uncs
  def fromXml(node: scala.xml.Node): Uncs = {
    val reporter = ((node \\ "Group")(0) \ "@RPT").text
    val time = ((node \\ "Group")(0) \ "@time").text
    val cl = ((node \\ "Group")(0) \ "@CL").text
    new Uncs(reporter, time, cl)
  }

}

// object TestToFromXml extends App {

//   // (a) convert a Stock to its XML representation  
//   // val aapl = new Stock("AAPL", "Apple", 600d)
//   // println(aapl.toXml)
  
//   // (b) convert an XML representation to a Stock
//   val googXml = <stock>
//       <symbol>GOOG</symbol>
//       <businessName>Google</businessName>
//       <price>620.00</price>
//     </stock>
//   //  val goog = Stock.fromXml(googXml)
//   //  println(goog)

//    val tariffUncs = <uncs:DataSet>
//       <uncs:Group RPT="400" time="2005" CL="H2" UNIT_MULT="1" DECIMALS="1" CURRENCY="USD" FREQ="A" TIME_FORMAT="P1Y" REPORTED_CLASSIFICATION="H2" FLOWS_IN_DATASET="MXR">
//       <uncs:Section TF="1" REPORTED_CURRENCY="JOD" CONVERSION_FACTOR="1.410440" VALUATION="CIF" TRADE_SYSTEM="Special" PARTNER="Origin">
//       <uncs:Obs CC-H2="442190900" PRT="392" netweight="438" qty="438" QU="8" value="2238.36828" EST="0" HT="0" />
//       <uncs:Obs CC-H2="442190900" PRT="422" netweight="88883" qty="88883" QU="8" value="385604.42292" EST="0" HT="0" />
//       </uncs:Section>
//       </uncs:Group>
//     </uncs:DataSet>
//     val comtr = Uncs.fromXml(tariffUncs)
//   print(comtr)
//   // print("hello")

//   // scala> (tariffUncs \\ "Obs")(0) \ "@PRT"
//   // res4: scala.xml.NodeSeq = 392

//   // scala> (tariffUncs \\ "Group")(0) \ "@RPT"
//   // res5: scala.xml.NodeSeq = 400

// }
