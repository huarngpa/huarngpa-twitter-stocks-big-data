package edu.uchicago.huarngpa
import scala.reflect.runtime.universe._


case class StockRecord(
  ticker: String,
  date: String, 
  day_open: Double, 
  day_high: Double, 
  day_low: Double, 
  day_close: Double, 
  day_volume: Long
)
