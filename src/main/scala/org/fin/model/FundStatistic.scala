package org.fin.model

case class FundStatistic(code:String, name: String,date:String, sum: Int,refFunds: List[String]) extends Ordered[FundStatistic]{
  override def compare(that: FundStatistic): Int = {
    this.sum-that.sum
  }

  override def toString: String = {
    this.code+","+this.name+","+this.sum+","+this.date+","+this.refFunds.mkString("|")
  }
}
