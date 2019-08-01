package com.jpro.resource

object dote {

  lazy val props: Context.type = Context

  // table name
  lazy val dotTable       : String = props | "cnc.table"

  lazy val stationNameOfCnc8  : String = props | "cnc.process.name.cnc8"
  lazy val stationNameOfCnc10 : String = props | "cnc.process.name.cnc10"
  lazy val stationNameOfLaser : String = props | "cnc.process.name.laser"

  lazy val ikUniqueID     : String = props | "cnc.unique.id.key"
  lazy val ikStationName  : String = props | "cnc.station.name.key"
  lazy val ikMachineName  : String = props | "cnc.machine.name.key"
  lazy val ikCell         : String = props | "cnc.cell.key"

}