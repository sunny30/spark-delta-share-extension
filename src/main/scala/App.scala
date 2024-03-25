import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.hive.datashare.ConverterUtil
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

object App {

  def getConf: SparkConf = {
    new SparkConf()
      .setMaster("local[2]")
      .set("spark.sql.extensions", "org.apache.spark.sql.hive.CustomExtensionSuite")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  }

  def main(args:Array[String]):Unit={
    val spark = SparkSession.builder().appName("spark-3.5.1-lake").master("local").
      config(getConf).enableHiveSupport().getOrCreate()

    import spark.implicits._
    val df  = Seq(
      Person(Some("Shar"),34),
      Person(Some("Xiaoyu"),40)
    ).toDF()

    val df1 = Seq(
      ID(1, "Shar"),
      ID(2, "Xiaoyu")
    ).toDF("id", "name")

//    spark.sql("""create table if not exists tpar1(id string, name string) using parquet PARTITIONED BY (id) location '/tmp/par1/'""")
//    df1.write.insertInto("tpar1")
//    spark.sql("CONVERT TO DELTA default.tpar1")

    spark.sql("""create table if not exists tpar4(id string, name string) using orc PARTITIONED BY (id) location '/tmp/par4/'""")
    df1.write.insertInto("tpar4")
    spark.sql("generate deltalog for table default.tpar4 using orc")

  //  val dt = DeltaTable.forPath("/tmp/par3/")
    val dl = DeltaLog.forTable(spark,"/tmp/par4/")
    dl.snapshot.metadata

//   // df.write.format("delta").save("/tmp/spark-3.5.0")
//    //df.agg(())
//    val js = JsonMethods.parse("""{"name": "sharad", "age":20}""")
//    implicit val format = DefaultFormats
//    val p = js.extract[Person]
//    println("%s",p.toString)
//    val jsonStr = org.json4s.jackson.Serialization.write(p)
//   // println(jsonStr)
//
//    val js1 = JsonMethods.parse("""["ss","ss1"]""")
//    val s = js1.extract[Seq[String]]
//    //println(s.mkString(","))
//
//    var map: Map[String,Seq[Person]] = Map("sharad"->Seq(p,p))
//    val newJs = org.json4s.jackson.Serialization.write(map)
//    println(newJs)
//
//
//
//    val js3 = JsonMethods.parse("""[{"name": "sharad", "age":20},{"name": "xiaoyu", "age":22}]""")
//    val ss = js3.extract[List[Person]]
//    println(ss)


  }

  case class Person(name:Option[String]=None, age:Int)

  case class ID(id:Int, name: String)
}