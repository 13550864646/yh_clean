package action

import com.alibaba.fastjson.JSON
import entity.JA.dt.{dt_desc, dt_res_data}
import entity.TZ.tz_res_data
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization.read

object actionInfosClean {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "d")
    val conf = new SparkConf().setAppName("test").setMaster("local")
    val sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.defaultFS", "hdfs://ha")
    sc.hadoopConfiguration.set("dfs.nameservices", "ha")
    sc.hadoopConfiguration.set("dfs.ha.namenodes.ha", "nn1,nn2")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn1", "yunhe.bigdata-master.yhwx:8020")
    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn2", "yunhe.bigdata-slave.yhwx:8020")
    //    sc.hadoopConfiguration.set("dfs.namenode.rpc-address.ha.nn2", "192.168.1.235:33003")
    sc.hadoopConfiguration.set("dfs.client.failover.proxy.provider.ha", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    val ssc = new StreamingContext(sc, Seconds(3))
    val sql = SparkSession.builder().config(conf).getOrCreate()
    import sql.implicits._

    val br_rdd = sc.textFile("hdfs://ha/yh_bigdata/cusReport/inputdata/BR/jdyx");
    val tz_rdd = sc.textFile("hdfs://ha/yh_bigdata/cusReport/inputdata/TZ/tzsj");
    val ja_rdd = sc.textFile("hdfs://ha/yh_bigdata/cusReport/inputdata/JA/dtsj");

//    val br_rdd = sc.textFile("file:///F:/test/STR0026066-13037212061.json");
//    val tz_rdd = sc.textFile("file:///F:/test/13037212061.json");
//    val ja_rdd = sc.textFile("file:///F:/test/T40301-13037212061.json");

    var ja_result = ja_rdd.map {
      line =>
        implicit val formats = org.json4s.DefaultFormats
        val dtsj: dt_res_data = read[dt_res_data](line)
        val desc = dtsj.data.RSL(0).RS.desc
        val desc_res: dt_desc = read[dt_desc](desc)
        val regfirsttime = desc_res.TJXX_90d.regfirsttime
        val idCard = dtsj.idCard
        val realName = dtsj.realName
        (realName, idCard, regfirsttime)
    }

    val jaDF = ja_result.toDF("realName", "idCard", "regfirsttime")
    jaDF.createOrReplaceTempView("ja")


    var br_result = br_rdd.map {
      line =>
        val rdd_obj = JSON.parseObject(line)
        val idCard = rdd_obj.get("idCard")
        val als_str = rdd_obj.get("ApplyLoanStr")

        var tot_mons_str = ""
        var oth_orgnum_str = ""
        var m12_nsloan_orgnum_str = ""
        var m12_night_allnum_str = ""

        if(als_str != null ){
          val als_obj = JSON.parseObject(als_str.toString)
          val m3_str = als_obj.get("m3")
          val m6_str = als_obj.get("m6")
          val m12_str = als_obj.get("m12")
          if(m3_str != null){
            val m3_obj = JSON.parseObject(m3_str.toString)
            val id_str = m3_obj.get("id")
            if(id_str != null){
              val id_obj = JSON.parseObject(id_str.toString)
              val nbank_str = id_obj.get("nbank")
              if(nbank_str != null ){
                val nbank_obj = JSON.parseObject(nbank_str.toString)
                if(nbank_obj.get("tot_mons") != null){
                  tot_mons_str = nbank_obj.get("tot_mons").toString //tot_mons
                }

              }
            }
          }

          if(m6_str != null){
            val m6_obj = JSON.parseObject(m6_str.toString)

            val cell_str = m6_obj.get("cell")
            if(cell_str != null){
              val cell_obj = JSON.parseObject(cell_str.toString)
              val m6_nbank_str = cell_obj.get("nbank")
              if(m6_nbank_str != null){
                  val m6_nbank_obj = JSON.parseObject(m6_nbank_str.toString)
                if(m6_nbank_obj.get("oth_orgnum") != null){
                  oth_orgnum_str = m6_nbank_obj.get("oth_orgnum").toString //oth_orgnum
                }

              }
            }

          }

          if(m12_str != null){
            val m12_obj = JSON.parseObject(m12_str.toString)

            val m12_id_str = m12_obj.get("id")
            if(m12_id_str != null){
              val m12_id_obj = JSON.parseObject(m12_id_str.toString)
              val m12_id_nbank_str = m12_id_obj.get("nbank")
              if(m12_id_nbank_str != null){
                val m12_id_nbank_obj = JSON.parseObject(m12_id_nbank_str.toString)
                if(m12_id_nbank_obj.get("nsloan_orgnum") != null){
                  m12_nsloan_orgnum_str = m12_id_nbank_obj.get("nsloan_orgnum").toString //nsloan_orgnum
                }
              }
            }


            val m12_cell_str = m12_obj.get("cell")
            if(m12_cell_str != null){
              val m12_cell_obj = JSON.parseObject(m12_cell_str.toString)
              val m12_cell_nbank_str = m12_cell_obj.get("nbank")
              if (m12_cell_nbank_str != null){
                val m12_cell_nbank_obj = JSON.parseObject(m12_cell_nbank_str.toString)
                if(m12_cell_nbank_obj.get("night_allnum") != null){
                  m12_night_allnum_str = m12_cell_nbank_obj.get("night_allnum").toString //night_allnum
                }
              }
            }
          }
        }


        (idCard.toString, tot_mons_str, oth_orgnum_str, m12_nsloan_orgnum_str, m12_night_allnum_str)
    }

    val brDF = br_result.toDF("idCard", "tot_mons_str", "oth_orgnum_str", "m12_nsloan_orgnum_str", "m12_night_allnum_str")
    brDF.createOrReplaceTempView("br")

    var tz_result = tz_rdd.map {
      line =>
        implicit val formats = org.json4s.DefaultFormats
        val tzsj: tz_res_data = read[tz_res_data](line)
        val idCard = tzsj.idCard
        val eveSums = tzsj.data.mb_infos(0).credit_info.eveSums.iterator
        val platform_Infos = tzsj.data.mb_infos(0).credit_info.platform_Infos.iterator
        val refInfos = tzsj.data.mb_infos(0).credit_info.refInfos.iterator

        var loan_offer_sum = "";
        var verif_count = "";
        var ave_repay_amount_level = "";
        while (eveSums.hasNext) {
          var eveSum = eveSums.next()
          val slice_name = eveSum.slice_name
          if ("m1".equals(slice_name)) {
            //ave_repay_amount_leve
            loan_offer_sum = eveSum.loan_offer_sum.toString
          }
        }

        while (platform_Infos.hasNext) {
          val platform_Info = platform_Infos.next()
          val slice_name = platform_Info.slice_name
          if ("m6".equals(slice_name)) {
            verif_count = platform_Info.verif_count.toString
          }
        }

        while (refInfos.hasNext) {
          val refInfo = refInfos.next()
          val slice_name = refInfo.slice_name
          if ("m1".equals(slice_name)) {
            ave_repay_amount_level = refInfo.ave_repay_amount_level
          }
        }

        (idCard, loan_offer_sum, verif_count, ave_repay_amount_level)
    }
    val tzDF = tz_result.toDF("idCard", "loan_offer_sum", "verif_count", "ave_repay_amount_level")
    tzDF.createOrReplaceTempView("tz")

    val res_data = sql.sql("select j.realName, j.idCard, j.regfirsttime," +
      "t.loan_offer_sum, t.verif_count, t.ave_repay_amount_level, " +
      "b.tot_mons_str, b.oth_orgnum_str, b.m12_nsloan_orgnum_str, b.m12_night_allnum_str " +
      "from ja j " +
      "join tz t on j.idCard = t.idCard " +
      "join br b on j.idCard = b.idCard")
    res_data.toDF().write.mode(SaveMode.Append).csv("hdfs://ha/sparkStreaminput/mydata")

  }

}
