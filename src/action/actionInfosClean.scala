package action

import com.alibaba.fastjson.{JSON, JSONObject}
import entity.JA.dt.{dt_data, dt_desc, dt_res_data}
import entity.TZ.tz_res_data
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization.read
import shapeless.syntax.std.function.fnUnHListOps

object actionInfosClean {

  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
//    System.setProperty("HADOOP_USER_NAME", "yhwx");
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://ha")
    sc.hadoopConfiguration.set("dfs.nameservices", "ha")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.ha", "nn1,nn2")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn1", "yunhe.bigdata-master.yhwx:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn2", "yunhe.bigdata-slave.yhwx:8020")
//    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn2", "192.168.1.235:33003")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.ha", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    val ssc = new StreamingContext(sc,Seconds(3))
    val sql = SparkSession.builder().config(conf).getOrCreate()

    val br_rdd = sc.textFile("hdfs://ha/yh_bigdata/cusReport/inputdata/BR/jdyx");
    val tz_rdd = sc.textFile("hdfs://ha/yh_bigdata/cusReport/inputdata/TZ/tzsj");
    val ja_rdd = sc.textFile("hdfs://ha/yh_bigdata/cusReport/inputdata/JA/dtsj");

    var ja_result = ja_rdd.map{
      line=>
        implicit val formats = org.json4s.DefaultFormats
        val dtsj:dt_res_data = read[dt_res_data](line)
        val desc = dtsj.data.RSL(0).RS.desc
        val desc_res:dt_desc = read[dt_desc](desc)
        var regfirsttime = desc_res.TJXX_90d.regfirsttime
        (regfirsttime)
    }


    var br_result = br_rdd.map{
      line=>
        var rdd_obj=JSON.parseObject(line)
        var als_str = rdd_obj.get("ApplyLoanStr")
        var als_obj=JSON.parseObject(als_str.toString)
        var m3_str = als_obj.get("m3")
        var m6_str = als_obj.get("m6")
        var m12_str = als_obj.get("m12")

        var m3_obj=JSON.parseObject(m3_str.toString)
        var m6_obj=JSON.parseObject(m6_str.toString)
        var m12_obj=JSON.parseObject(m12_str.toString)

        var id_str = m3_obj.get("id")
        var id_obj=JSON.parseObject(id_str.toString)
        var nbank_str = id_obj.get("nbank")
        var nbank_obj=JSON.parseObject(nbank_str.toString)
        var tot_mons_str = nbank_obj.get("tot_mons")//tot_mons

        var cell_str = m6_obj.get("cell")
        var cell_obj=JSON.parseObject(cell_str.toString)
        var m6_nbank_str = cell_obj.get("nbank")
        var m6_nbank_obj=JSON.parseObject(m6_nbank_str.toString)
        var oth_orgnum_str = m6_nbank_obj.get("oth_orgnum")//oth_orgnum

        var m12_id_str = m12_obj.get("id")
        var m12_id_obj=JSON.parseObject(m12_id_str.toString)
        var m12_id_nbank_str = m12_id_obj.get("nbank")
        var m12_id_nbank_obj=JSON.parseObject(m12_id_nbank_str.toString)
        var m12_nsloan_orgnum_str = m12_id_nbank_obj.get("nsloan_orgnum")//nsloan_orgnum

        var m12_cell_str = m12_obj.get("cell")
        var m12_cell_obj=JSON.parseObject(m12_cell_str.toString)
        var m12_cell_nbank_str = m12_cell_obj.get("nbank")
        var m12_cell_nbank_obj=JSON.parseObject(m12_cell_nbank_str.toString)
        var m12_night_allnum_str = m12_cell_nbank_obj.get("night_allnum")//night_allnum

        (tot_mons_str,oth_orgnum_str,m12_nsloan_orgnum_str,m12_night_allnum_str)

    }

    var tz_result = tz_rdd.map{
      line=>
        implicit val formats = org.json4s.DefaultFormats
        val tzsj:tz_res_data = read[tz_res_data](line)
        val eveSums = tzsj.data.mb_infos(0).credit_info.eveSums.iterator
        val platform_Infos = tzsj.data.mb_infos(0).credit_info.platform_Infos.iterator
        val refInfos = tzsj.data.mb_infos(0).credit_info.refInfos.iterator

        var loan_offer_sum="";
        var verif_count="";
        var ave_repay_amount_level="";
        while(eveSums.hasNext){
          var eveSum =eveSums.next()
          val slice_name = eveSum.slice_name
          if ("m1".equals(slice_name)){//ave_repay_amount_leve
            loan_offer_sum=eveSum.loan_offer_sum.toString
          }
        }

        while(platform_Infos.hasNext){
          val platform_Info = platform_Infos.next()
          val slice_name = platform_Info.slice_name
          if("m6".equals(slice_name)){
            verif_count = platform_Info.verif_count.toString
          }
        }

        while(refInfos.hasNext){
          val refInfo = refInfos.next()
          val slice_name = refInfo.slice_name
          if ("m1".equals(slice_name)){
            ave_repay_amount_level = refInfo.ave_repay_amount_level
          }
        }

        (loan_offer_sum,verif_count,ave_repay_amount_level)

    }

  }

}
