package com.ms.psdi.meta.common

case class BuildContainer(schemas:List[SqlTable], tables: List[SqlTable], values: Map[String, String])
