package edu.uchicago.huarngpa


object Sentiment extends Enumeration {
  type Sentiment = Value
  val VNEGATIVE, NEGATIVE, NEUTRAL, POSITIVE, VPOSITIVE = Value

  def toSentiment(sentiment: Int): Sentiment = sentiment match {
    case 0 => Sentiment.VNEGATIVE
    case 1 => Sentiment.NEGATIVE
    case 2 => Sentiment.NEUTRAL
    case 3 => Sentiment.POSITIVE
    case 4 => Sentiment.VPOSITIVE
  }
}


