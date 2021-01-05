package com.ms.psdi.meta.common

import net.liftweb.json._
import net.liftweb.json.Serialization.write

object JsonHelper{
  def toJSON[T](obj: T):String = {
    implicit val formats = DefaultFormats
    val jsonString = write(obj)
    jsonString
  }

  def fromJSON[T](jsonString: String)(implicit m: Manifest[T]): T = {
    implicit val formats = DefaultFormats
    val obj = parse(jsonString)
    obj.extract[T]
  }
}