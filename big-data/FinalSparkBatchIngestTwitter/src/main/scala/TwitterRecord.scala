package edu.uchicago.huarngpa
import scala.reflect.runtime.universe._


case class TwitterRecord(
  created_at: String,
  created_at_day: String, 
  id_str: String, 
  text: String,
  retweeted_count: Long,
  favorite_count: Long,
  user_id: String
)
