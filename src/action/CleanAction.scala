package action

import com.alibaba.fastjson.JSON
import entity.JA.dt.dt_data
import entity.TZ.TZdata
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.native.Serialization.read

object CleanAction {

  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://ha")
    sc.hadoopConfiguration.set("dfs.nameservices", "ha")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.ha", "nn1,nn2")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn1", "yunhe.bigdata-master.yhwx:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn2", "yunhe.bigdata-slave.yhwx:8020")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.ha", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    val ssc = new StreamingContext(sc,Seconds(3))
    val sql = SparkSession.builder().config(conf).getOrCreate()

    /**
      *spark监听
      */
    val dt_stream = ssc.textFileStream("hdfs://ha/cusReport/inputdata/JA/dtsj")
    val tz_stream = ssc.textFileStream("hdfs://ha/cusReport/inputdata/TZ")
    val jd_stream = ssc.textFileStream("hdfs://ha/cusReport/inputdata/BR/jdyx")
    val ts_stream = ssc.textFileStream("hdfs://ha/cusReport/inputdata/BR/tsmd")

//    集奥多头数据处理
    dt_stream.map{
      line=>
        implicit val formats = org.json4s.DefaultFormats
        val dtsj:dt_data = read[dt_data](line)
        val city = dtsj.ISPNUM.city
        val isp = dtsj.ISPNUM.isp
        val province = dtsj.ISPNUM.province
        (city,isp,province)
    }.foreachRDD{
      line=>
        sql.createDataFrame(line).toDF().write.mode(SaveMode.Append).text("hdfs://ha/cusReport/resultdata/JA/dtsj")
    }
//    探知数据处理
    tz_stream.map{
      line=>
        implicit val formats = org.json4s.DefaultFormats
        val tzdata:TZdata = read[TZdata](line)
        val score_s = tzdata.score_s
        val score_l = tzdata.score_l
        val name = tzdata.basic_info.name
        val idcard_location = tzdata.basic_info.idcard_location
        val idcard = tzdata.basic_info.idcard
        val phone_number_location = tzdata.basic_info.phone_number_location
        val phone_number = tzdata.basic_info.phone_number
        val operator_type = tzdata.basic_info.operator_type
    }

//    百融借贷意向数据处理
    jd_stream.map{
      line=>
        val jdyx = JSON.parseObject(line)
        val als_m12_cell_nbank_night_allnum = jdyx.get("als_m12_cell_nbank_night_allnum")
        val als_lst_cell_nbank_consnum = jdyx.get("als_lst_cell_nbank_consnum")
        val als_m12_id_nbank_cf_orgnum = jdyx.get("als_m12_id_nbank_cf_orgnum")
        val als_m12_id_nbank_max_monnum = jdyx.get("als_m12_id_nbank_max_monnum")
        val als_m12_cell_bank_tra_orgnum = jdyx.get("als_m12_cell_bank_tra_orgnum")
        val als_lst_cell_nbank_inteday = jdyx.get("als_lst_cell_nbank_inteday")
        val rs_Rule_decision = jdyx.get("rs_Rule_decision")
        val rs_product_type = jdyx.get("rs_product_type")
        val rs_scene = jdyx.get("rs_scene")
        val rs_product_name = jdyx.get("rs_product_name")
        (als_m12_cell_nbank_night_allnum,als_lst_cell_nbank_consnum,als_m12_id_nbank_cf_orgnum,als_m12_id_nbank_max_monnum,als_m12_cell_bank_tra_orgnum,als_lst_cell_nbank_inteday,rs_Rule_decision,rs_product_type,rs_scene,rs_product_name)
    }.foreachRDD {
      line =>
        sql.createDataFrame(line).toDF().write.mode(SaveMode.Append).text("hdfs://ha/cusReport/resultdata/BR/jdyx")
    }
    //    百融特殊名单
    val tsdata = ts_stream.map{
      line=>
        val tsmd = JSON.parseObject(line)
        val rs_product_name = tsmd.get("rs_product_name")
        val swift_number = tsmd.get("swift_number")
        val code = tsmd.get("code")
        val flag_specialList_c = tsmd.get("flag_specialList_c")
        val rs_strategy_id = tsmd.get("rs_strategy_id")
        (rs_product_name,swift_number,code,flag_specialList_c,rs_strategy_id)
    }


//    jddata.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
