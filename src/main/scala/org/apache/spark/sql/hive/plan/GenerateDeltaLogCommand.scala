package org.apache.spark.sql.hive.plan

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.delta.commands.DeltaCommand
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.hive.datashare.ConverterUtil

case class GenerateDeltaLogCommand(table: Option[CatalogTable],
                                   location: Option[String],
                                   format:String
                                  ) extends LeafRunnableCommand with DeltaCommand{

  override val output:Seq[Attribute] = Nil

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val path = if(table.isDefined){
      table.get.storage.locationUri.get.getPath
    }else{
      location.get
    }
    ConverterUtil(None, table,format).generateDeltaLog(sparkSession,path,format)
  }


}
