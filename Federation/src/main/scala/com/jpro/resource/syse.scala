package com.jpro.resource

object syse {

  lazy val props: Context.type = Context

  // table name
  lazy val extraTable    : String = props | "sys.part.table"
  lazy val processTable  : String = props | "sys.process.table"
  lazy val defectTable   : String = props | "sys.defect.table"

  // inner field name
  lazy val skPartID           : String = props | "sys.part.id.key"
  lazy val skPartProduct      : String = props | "sys.part.product.key"
  lazy val skPartProductCode  : String = props | "sys.part.product.code.key"
  lazy val skPartColor        : String = props | "sys.part.color.key"
  lazy val skPartMedia        : String = props | "sys.part.media.key"

  lazy val skProcessID        : String = props | "sys.process.id.key"
  lazy val skProcessName      : String = props | "sys.process.name.key"

  lazy val skDefectID         : String = props | "sys.defect.id.key"
  lazy val skDefectDescCn     : String = props | "sys.defect.desc.cn.key"
  lazy val skDefectDescEn     : String = props | "sys.defect.desc.en.key"
  lazy val skDefectType       : String = props | "sys.defect.type.key"
}
