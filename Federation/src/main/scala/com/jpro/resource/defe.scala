package com.jpro.resource

object defe {

  lazy val props: Context.type = Context

  // table name
  lazy val defectTable  : String = props | "defect.table"

  // input data
  lazy val ikUniqueID   : String = props | "defect.unique.id.key"
  lazy val ikFailID     : String = props | "defect.fail.id.key"
  lazy val ikDefectID   : String = props | "defect.id.key"
  lazy val ikStationID  : String = props | "defect.station.id.key"

  // output
  lazy val okDefectID   : String = props | "defect.gen.id.key"
  lazy val okDefectEn   : String = props | "defect.gen.desc.en.key"
  lazy val okDefectCn   : String = props | "defect.gen.desc.cn.key"
  lazy val okDefectType : String = props | "defect.gen.type.key"

}