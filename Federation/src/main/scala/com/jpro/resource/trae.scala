package com.jpro.resource

object trae {

  lazy val props: Context.type = Context

  // table name
  lazy val stationTablePrefix   : String = props | "travel.station.table.prefix"

  // input field name of Travel Data
  lazy val ikUniqueID     : String = props | "travel.unique.id.key"
  lazy val ikCompose      : String = props | "travel.compose.key"
  lazy val ikExtraID      : String = props | "travel.extra.id.key"
  lazy val ikStationID    : String = props | "travel.station.id.key"
  lazy val ikStateOne     : String = props | "travel.state.one.key"
  lazy val ikStateTwo     : String = props | "travel.state.two.key"
  lazy val ikOutTime      : String = props | "travel.out.time.key"
  lazy val ikFailID       : String = props | "travel.fail.id.key"

  // output field name
  lazy val okGenDefectKey : String = props | "travel.gen.json.defect.key"
  lazy val okGenState     : String = props | "travel.gen.state.key"
  lazy val ovGenPass      : String = props | "travel.gen.state.pass.val"
  lazy val ovGenFail      : String = props | "travel.gen.state.fail.val"
  lazy val ovGenRework    : String = props | "travel.gen.state.rework.val"
  lazy val ovGenMiddle    : String = props | "travel.gen.state.middle.val"
  lazy val ovGenFinal     : String = props | "travel.gen.state.final.val"
  lazy val okGenrstTime   : String = props | "travel.gen.rst.time.key"
  lazy val okGenCompose   : String = props | "travel.gen.compose.key"
  lazy val okGenAuto      : String = props | "travel.gen.auto.key"
  lazy val ovGenAuto      : String = props | "travel.gen.auto.val"
  lazy val okGenStation   : String = props | "travel.gen.station.name.key"
  lazy val okGenColor     : String = props | "travel.gen.color.key"
  lazy val okGenMedia     : String = props | "travel.gen.media.key"
  lazy val okGenProduct   : String = props | "travel.gen.product.key"
  lazy val okGenProductID : String = props | "travel.gen.product.id.key"

  lazy val reworkStationIDOfCnc8  : String = props | "rework.id.cnc8"
  lazy val reworkStationIDOfAno   : String = props | "rework.id.Ano"
  lazy val reworkStationIDOfFqc   : String = props | "rework.id.fqc"
  lazy val stationNameOfCnc8      : String = props | "process.name.cnc8"
  lazy val stationNameOfAno       : String = props | "process.name.ano"
  lazy val stationNameOfFqc       : String = props | "process.name.fqc"

  lazy val ReworkIDMapper: Map[String, String] =
    Map(reworkStationIDOfCnc8 -> stationNameOfCnc8,
      reworkStationIDOfAno -> stationNameOfAno,
      reworkStationIDOfFqc -> stationNameOfFqc)

  lazy val stationIDOfCnc8        : String = props | "process.id.cnc8"
  lazy val stationIDOfCnc10       : String = props | "process.id.cnc10"
  lazy val stationIDOfLaser       : String = props | "process.id.laser"

  lazy val DotStationsIDSeq: List[String] =
    List(stationIDOfCnc8, stationIDOfCnc10, stationIDOfLaser)

}