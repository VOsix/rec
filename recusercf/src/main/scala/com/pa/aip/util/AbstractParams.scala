package com.pa.aip.util

import scala.reflect.runtime.universe._

/**
  * com.pa.aip.util Created by bookforcode on 2019/6/8.
  */

abstract class AbstractParams[T: TypeTag] {
  private def tag: TypeTag[T] = typeTag[T]

  override def toString: String = {
    val tpe = tag.tpe
    val allAccessors = tpe.decls.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors.map { f =>
      val paramName = f.name.toString
      val fieldMirror = instanceMirror.reflectField(f)
      val paramValue = fieldMirror.get
      s"$paramName:\t$paramValue"
    }.mkString("{\n", ",\n", "\n}")
  }
}
