// reset avangrid database

spark.sql("DROP DATABASE IF EXISTS avangrid CASCADE")
spark.sql("DROP DATABASE IF EXISTS avangrid_datastore_testing CASCADE")
spark.sql("CREATE DATABASE avangrid LOCATION '/opt/spark-apps/spark-warehouse/avangrid.db'")
spark.sql("CREATE DATABASE avangrid_datastore_testing LOCATION '/opt/spark-apps/spark-warehouse/avangrid_testing.db'")

/** Config file
  * This is a temporary config in a string variable
  * while the Blob Storage Gen2 is un available
  */
  
val configString = """
database = "avangrid_datastore"

avangrid_database_tables_list = [
                              "customer_count",
                              "customer_owned",
                              "dta_web_bushing",
                              "dta_web_excitation_current",
                              "dtaweb_overalltests",
                              "load_serving_regulatory_compliance",
                              "ltc_issue_list",
                              "number_operations",
                              "powertransformer_barrier_board_issues",
                              "powertransformer_peakloads",
                              "sap_characteristics",
                              "sap_notifications",
                              "toa4_equipment_list",
                              "toa4_test_data"
]

queries {
  query_customer_count                          = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `substation` varchar(255), `equip` char(12), `asset_dx` varchar(255), `primary_voltage` float, `secondary_voltage` float, `sum_of_customers` int) PARTITIONED BY ( `file_year` string, `file_month` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",  
  query_customer_owned                          = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `equip` varchar(255), `func_loc` varchar(255), `serial_num` varchar(255), `division` varchar(255), `substation` varchar(255), `bank_desc` varchar(255), `manufacturer` varchar(255), `voltage` float, `voltage_level` varchar(255), `size` int, `constr_year` int, `age` int, `oltc_found` varchar(255), `oltc_removable` varchar(255)) PARTITIONED BY ( `file_year` int, `file_month` int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_dta_web_bushing                         = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `apparatus_id` string, `apparatus_type` string, `serial_num` string, `year_manufacture` int, `special_id` string, `model_number` string, `manufacturer` string, `cooling_class` string, `configuration` string, `coolant` string, `tank_type` string, `kv_rating_0` double, `kv_rating_1` double, `kv_rating_2` double, `phases` int, `manufacturer_location` string, `basic_insulation_level` double, `weight` double, `weight_unit` string, `oil_volume` double, `oil_volume_units` string, `va_rating_0` double, `va_rating_1` double, `va_rating_2` double, `va_rating_3` double, `va_rating_units` string, `leakage_reactance_winding_hl` string, `winding_temperature_rise` double, `winding_material` string, `air_temperature` double, `cct_designation` string, `company` string, `division` string, `first_revision_date` string, `humidity` double, `id` string, `internal_temperature` double, `last_revision_date` string, `location` string, `test_date` string, `test_note` string, `test_number` int, `upload_date` string, `weather` string, `manual_rating` string, `frank_rating` string) PARTITIONED BY ( `file_year` string, `file_month` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_dta_web_excitation_current              = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `apparatus_id` string, `apparatus_type` string, `serial_num` string, `year_manufacture` int, `special_id` string, `model_number` string, `manufacturer` string, `cooling_class` string, `configuration` string, `coolant` string, `tank_type` string, `kv_rating_0` double, `kv_rating_1` double, `kv_rating_2` double, `phases` int, `manufacturer_location` string, `basic_insulation_level` double, `weight` double, `weight_unit` string, `oil_volume` double, `oil_volume_units` string, `va_rating_0` double, `va_rating_1` double, `va_rating_2` double, `va_rating_3` double, `va_rating_units` string, `leakage_reactance_winding_hl` string, `winding_temperature_rise` double, `winding_material` string, `air_temperature` double, `cct_designation` string, `company` string, `division` string, `first_revision_date` string, `humidity` double, `id` string, `internal_temperature` double, `last_revision_date` string, `location` string, `test_date` string, `test_note` string, `test_number` int, `upload_date` string, `weather` string, `manual_rating` string, `frank_rating` string) PARTITIONED BY ( `file_year` string, `file_month` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_dtaweb_overalltests                     = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `apparatus_id` string, `apparatus_type` string, `serial_num` string, `year_manufacture` int, `special_id` string, `model_number` string, `manufacturer` string, `cooling_class` string, `configuration` string, `coolant` string, `tank_type` string, `kv_rating_0` double, `kv_rating_1` double, `kv_rating_2` double, `phases` int, `manufacturer_location` string, `basic_insulation_level` double, `weight` double, `weight_unit` string, `oil_volume` double, `oil_volume_units` string, `va_rating_0` double, `va_rating_1` double, `va_rating_2` double, `va_rating_3` double, `va_rating_units` string, `leakage_reactance_winding_hl` string, `winding_temperature_rise` double, `winding_material` string, `air_temperature` double, `cct_designation` string, `company` string, `division` string, `first_revision_date` string, `humidity` double, `id` string, `internal_temperature` double, `last_revision_date` string, `location` string, `test_date` string, `test_note` string, `test_number` int, `upload_date` string, `weather` string, `pf_corrected` double, `insulation` string) PARTITIONED BY ( `file_year` string, `file_month` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_load_serving_regulatory_compliance      = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `equip` varchar(20), `serial_num` varchar(50), `subst` varchar(255), `bank_dx` varchar(255), `high_side_voltage` float, `base_nameplate_kva` int, `const_yr` int, `low_load_serving` int, `medium_load_serving` int, `high_load_serving` int, `very_high_load_serving` int, `low_regulatory_compliance` int, `medium_regulatory_compliance` int, `high_regulatory_compliance` int, `very_high_regulatory_compliance` int) PARTITIONED BY ( `file_year` string, `file_month` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_ltc_issue_list                          = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `ltc_model` varchar(255), `adj_ltc_model` varchar(255), `manufacturer` varchar(255), `vacuum` string, `removable` varchar(255), `hi_de_rated` varchar(255), `sme_issue` varchar(255), `kinectrics_comments` varchar(255), `additional_info` varchar(255)) PARTITIONED BY ( `file_year` int, `file_month` int) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_number_operations                       = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `equip` varchar(255), `meas_document` varchar(255), `meas_point` varchar(255), `date_measurement` varchar(255), `meas_tot_ctr_rdg` float, `charac_unit` varchar(255), `text` varchar(255), `meas_position` varchar(255), `counter_reading` float, `description` varchar(255), `cntr_over_readg` float) PARTITIONED BY ( `file_year` string, `file_month` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_powertransformer_barrier_board_issues   = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `opco` varchar(20), `equip` varchar(20), `serial_num` varchar(20), `subst` varchar(20)) PARTITIONED BY ( `file_year` varchar(4), `file_month` varchar(2)) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_powertransformer_peakloads              = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `equip` varchar(20), `opco` varchar(10), `subst` varchar(50), `transformer_bank` varchar(20), `lowside_voltage` decimal(4,2), `cnr_mva` decimal(4,2), `calc_plbn_rating` decimal(4,2), `thermal_study_rating` decimal(4,2), `limiting_element` varchar(20), `rating_mva` decimal(4,2), `year` int, `summer_peak_load_mva` decimal(4,2)) PARTITIONED BY ( `file_year` varchar(4), `file_month` varchar(2)) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_sap_characteristics                     = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `equip` char(18), `serial_num` varchar(50), `system_status` varchar(40), `user_status` varchar(250), `constr_yr` int, `func_loc` varchar(100), `short_asset_dx` varchar(250), `main_work_ctr` varchar(100), `company_code` varchar(20), `long_asset_dx` varchar(250), `object_type` varchar(20), `oper_num` varchar(100), `base_kva_nameplate` varchar(100), `bil_high_side` varchar(100), `bil_low_side` varchar(100), `cooling_ratings` varchar(100), `core_type` varchar(100), `depth_wo_radiators` int, `dielectric_type` varchar(100), `height_wo_busings` int, `height_wo_high_busings` int, `high_side_connection` varchar(100), `u_high_side_nameplate` float, `hs_ls_volt_string` varchar(100), `impendance_hl` int, `low_side_connection` varchar(20), `low_side_nameplate_v` varchar(100), `low_side_volt_str` varchar(100), `ltc_balance_voltage` varchar(100), `ltc_bandwith` varchar(100), `ltc_location` varchar(100), `equi_d_can_ten_tipo_rayo` varchar(100), `equi_herst` varchar(100), `equi_inbdt` varchar(100), `equi_d_can_pot_trafo` varchar(100), `equi_u_pri_voltage` varchar(100)) PARTITIONED BY ( `file_year` varchar(4), `file_month` varchar(2)) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_sap_notifications                       = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `equip` varchar(12), `notif_type` varchar(20), `func_loc` varchar(20), `company_code` varchar(5), `notification` varchar(20), `notif_date_created_on` varchar(20), `description` varchar(100), `long_text_1` varchar(250), `long_text_2` varchar(250), `long_text_3` varchar(250), `long_text_4` varchar(250)) PARTITIONED BY ( `file_year` varchar(4), `file_month` varchar(2)) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='|', 'serialization.format'='|') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_toa4_equipment_list                     = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `equipnum` string, `serial_num` string, `appr_type` string, `owner_name` string, `region_name` string, `substn_name` string, `designation` string, `norm_name` string, `fluid_type` string, `eqp_desc` string, `mfr` string, `year_mfg` string, `model` string, `kv_ratings` string, `mva_ratings` string, `rated_kv` string, `primarykv` string, `secondarykv` string, `ratedamps` string, `oil_pres` string, `cooling` string, `fluidvol` string, `has_infiltration` string, `pcb_label` string, `temp_rise` string, `triphase` string, `pct_impedance_hx` string, `external_id` string, `in_service` string, `in_service_date` string, `spare` string, `surveillance` string, `eqp_remarks` string) PARTITIONED BY ( `file_year` string, `file_month` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='~', 'serialization.format'='~') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'",
  query_toa4_test_data                          = "CREATE EXTERNAL TABLE ${database}.${table_name} ( `equipnum` string, `serial_num` string, `apprtype` string, `tank` string, `labreportnum` string, `sample_date` string, `labrecvdate` string, `labtestdate` string, `paid` string, `jobnum` string, `container_id` string, `fluidtempc` string, `opscount` string, `lab_name` string, `is_base` string, `exclude` string, `reason` string, `weather` string, `humidity` string, `airtemp` string, `shipdate` string, `sampler` string, `samplept` string, `h2` string, `o2` string, `n2` string, `ch4` string, `co` string, `co2` string, `c2h4` string, `c2h6` string, `c2h2` string, `total_gas` string, `tcg` string, `fluid_temp_top` string, `d1816_1` string, `d1816_2` string, `d877` string, `water` string, `rel_saturation` string, `acid_num` string, `ift` string, `pf25` string, `pf100` string, `inhibitor` string, `visual` string, `fq_water` string, `color` string, `d1275b` string, `sg` string, `viscosity` string, `firept` string, `flashpt` string, `pourpt` string, `login_remarks` string, `labremarks` string, `otstatus` string, `aluminum` string, `calcium` string, `copper` string, `iron` string, `lead` string, `lithium` string, `nickel` string, `silicon` string, `silver` string, `sodium` string, `tin` string, `zinc` string, `totalmetal` string, `pcb_kit_result` string, `p21c` string, `p70c` string, `a1016` string, `a1221` string, `a1232` string, `a1242` string, `a1248` string, `a1254` string, `a1260` string, `a1262` string, `a1268` string, `totalpcb` string, `furfural` string, `totalfuran` string, `norm_used` string, `context` string, `dga_result` string, `dga_diagnosis` string, `dga_summary` string, `dga_remarks` string, `dga_retestdays` string, `dga_retestdate` string, `dga_refdays` string, `fq_result` string, `fq_diagnosis` string, `fq_summary` string, `fq_retestdays` string, `fq_retestdate` string, `fq_remarks` string, `pcb_result` string, `pcb_remarks` string, `pcb_flag` string, `moisture_result` string, `moisture_diagnosis` string, `moisture_summary` string, `moisture_remarks` string, `inhibitor_result` string, `furan_result` string, `furan_remarks` string, `furan_summary` string, `c2h2_h2` string, `c2h6_ch4` string, `c2h4_c2h2` string, `c2h2_c2h4` string, `co2_co` string, `co_co2` string, `o2_n2` string, `n2_o2` string, `thg` string, `tdcg` string, `etcg` string, `totpartpress` string, `eshl` string, `monitorcalc` string, `nei_hc` string, `nei_t` string, `nei_co` string, `fqindex` string, `pfratio` string, `dewptc` string, `estdp` string, `ptotal` string, `pq1` string, `pmedian` string, `pq3` string, `piqr` string, `pmean` string, `pstddev` string, `pskewness` string, `dga_first_date` string, `dga_last_date` string, `dga_num_samples` string, `dga_num_gas_events` string, `rdga_severity_liquid` string, `rdga_severity_paper` string, `rdga_status` string, `gas_event_type` string, `gas_event_start_date` string, `gas_event_end_date` string, `gas_event_start_value` string, `gas_event_end_value` string, `gas_event_num_samples` string, `gas_event_summary` string, `gas_event_severity` string, `gas_event_hf` string, `gas_event_fault_type` string, `gas_event_co_co2_rel_inc` string, `airtempc` string, `equipcond` string, `fluid_review` string, `fluidcond` string, `fluidstd` string, `freegas` string, `gas_review` string, `gasstd` string, `pcbstatus` string, `posted` string, `remarks` string, `reviewer` string, `sampledby` string, `sulfur` string, `substation` string, `watersol_a` string, `watersol_b` string) PARTITIONED BY ( `file_year` string, `file_month` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='|', 'serialization.format'='|') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/${db_path}/${table_name}/'"
}

options {
  options_customer_count {
    header      = "false"
    sep         = ";"
  }
  options_customer_owned {
    header      = "false"
    sep         = ";"
  }
  options_dta_web_bushing {   
    header      = "false"
    sep         = ";"
    nullValue   = "\\N"
  }
  options_dta_web_excitation_current {
    header      = "false"
    sep         = ";"
    nullValue   = "\\N"
  }
  options_dtaweb_overalltests {
    header      = "false"
    sep         = ";"
    nullValue   = "\\N"
  }
  options_load_serving_regulatory_compliance {
    header      = "false"
    sep         = ";"
  }
  options_ltc_issue_list {
    header      = "false"
    sep         = ";"
  }
  options_number_operations { 
    header      = "false"
    sep         = ";"
  }
  options_powertransformer_barrier_board_issues {
    header      = "false"
    sep         = ";"
  }
  options_powertransformer_peakloads {
    header      = "false"
    sep         = ";"
  }
  options_sap_characteristics {
    header      = "false"
    sep         = ";"
  }
  options_sap_notifications { 
    header      = "false"
    sep         = "|"
    nullValue   = "\\N"
  }
  options_toa4_equipment_list {
    header      = "false"
    sep         = "~"
    nullValue   = "\\N"
  }
  options_toa4_test_data {
    header      = "false"
    sep         = "|"
    nullValue   = "\\N"
  }
}""".stripMargin


import collection.JavaConversions._                                                                                                                                                                                
import scala.collection.JavaConverters._                                                                                                                                                                           
import com.typesafe.config.{ConfigFactory,ConfigRenderOptions}                                                                                                                                                     

// Load and parse the config file

//val configFilePath = "aam_load.conf"
//val config = ConfigFactory.load(configFilePath)
//val config = ConfigFactory.parseString(configString).getConfig("queries")
val config = ConfigFactory.parseString(configString)
val avangrid_database_tables_list = config.getStringList("avangrid_database_tables_list").toList

val queries = config.getObject("queries").toMap.mapValues(_.unwrapped())

val op = config.getConfig("options")
val v = op.root.keySet.map( x=> { op.getObject(x).map( y => { (y._1 -> y._2.unwrapped.toString) } ) } )
val k = op.root.keySet
val options = k zip v toMap
val options2 = op.root.unwrapped()

import com.microsoft.azure.synapse._
import mssparkutils._
import org.apache.spark.sql.DataFrame
import java.io.File
import scala.util.matching.Regex

/** Factory for creating external tables */
object CreateExternalTable {
  /** Creates a external table based in its <name> and the query_<name> in the config file 
   * @param database The database where the table will be created
   * @param tableName The name of the table 
   */
  def createExternalTableStruct(database: String, tableName: String): Unit = {
    val queryName = s"query_${tableName}"
    val query = queries(queryName).toString().replace("${table_name}", tableName).replace("${database}",database).replace("${db_path}","opt/spark-apps/spark-warehouse/avangrid.db")
    println(query)
    if (!spark.catalog.tableExists(database + "." + tableName)) {
      spark.sql(query)
    }

  }


  /** Creates and loads data to a external table from a file in the
   * local files when the table has two partition values 
   * file_year & file_month
   *
   * @param database The database where the table will be created
   * @param tableName The name of the table
   * @param inputFilePath The path to the data to be inserted in the table
   * @param partitionColumn0 The name of the first column to partition by
   * @param partitionValue0 The value of the first column to partition by
   * @param partitionColumn1 The name of the second column to partition by
   * @param partitionValue1 The value of the second column to partition by
   */
  def loadDataToExternalTable1(database: String, tableName: String, inputFilePath: String,
                              partitionColumn0: String, partitionValue0: String,
                              partitionColumn1: String, partitionValue1: String): Unit = {
    val o = "options_" + tableName
    val optionsMap = op.getObject(o).map(y=> { ( y._1 -> y._2.unwrapped.toString) } )
    val df1 = spark.table(s"${database}.${tableName}").drop(col(s"${partitionColumn0}")).drop(col(s"${partitionColumn1}"))
    val df2 = spark.read.options(optionsMap).schema(df1.schema).csv(inputFilePath)
    df2.createOrReplaceTempView("myPreOutputTable")

    //Write the data in the table
    spark.sql(
      s"""INSERT INTO TABLE $database.$tableName
      PARTITION (
      ${partitionColumn0} = ${partitionValue0},
      ${partitionColumn1} = ${partitionValue1}
      )
      SELECT * FROM myPreOutputTable
         |""".stripMargin
    )
    //Drop temp view
    spark.catalog.dropTempView("myPreOutputtable")
  }

  // Regular expressions to find the partition field and value
  // in the path
  val yearExp = new Regex("(file_year)=([0-9]{4})")
  val monthExp = new Regex("(file_month)=([0-9]{2})")
  //TODO Regular expression to find the partition when is execution_date
  // val exDateExp = new Regex("")

  /** Creates and loads all the files in a local path in Azure Data Lake Storage Gen2
   *
   * The paths of every file have to keep the structure from the hive data lake
   * from which the data was extracted
   *
   * @param path Azure Data Lake Storage Gen2 path where the files are
   */

  // This method works for partition = file_month + file_year

  def loadDataTest (path: String): Unit = {
    val directory = new File(path)
    directory.listFiles.foreach {
        f => {
            f.isDirectory match {
                case true => {
                    println("DIR  --  " + f)
                    loadDataTest(f.toString)
                }
                case false =>{
                    println("FILE --  " + f)

                    val table_name = avangrid_database_tables_list.find(f.toString.contains(_)).getOrElse("Error")
                    println(table_name)
                    //TODO This code will fail when the partition is execution_date
                    val part0 = yearExp findAllIn f.toString.toLowerCase
                    val part1 = monthExp findAllIn f.toString.toLowerCase
                    val query = "query_" + table_name
 
                    CreateExternalTable.createExternalTableStruct(database = "avangrid_datastore_testing", tableName = table_name)
                  
                    CreateExternalTable.loadDataToExternalTable1(
                        database = "avangrid_datastore_testing",
                        tableName = table_name,
                        inputFilePath = f.toString,
                        partitionColumn0 = part0.group(1),
                        partitionValue0 = part0.group(2),
                        partitionColumn1 = part1.group(1),
                        partitionValue1 = part1.group(2)
                    )
                    spark.sql("SELECT count(*) FROM avangrid_datastore_testing.customer_count").show()
                } 
            }
        }
    }
  }

}

// spark-shell --jars /opt/spark-libraries/microsoft/azure/synapse/synapseutils_2.12/1.4/synapseutils_2.12-1.4.jar,/opt/spark-libraries/typesafe/config/1.4.1/config-1.4.1.jar -I /opt/spark-apps/playground.scala



/*
val tableName = "customer_count"
val database = "avangrid"
val queryName = s"query_${tableName}"

val yearExp = new Regex("(file_year)=([0-9]{4})")
val monthExp = new Regex("(file_month)=([0-9]{2})")

val inputFilePath = path
val part0 = yearExp findAllIn path.toLowerCase
val part1 = monthExp findAllIn path.toLowerCase
val partitionColumn0 = part0.group(1)
val partitionValue0 = part0.group(2)
val partitionColumn1 = part1.group(1)
val partitionValue1 = part1.group(2)
 */



/* 
val path = "/opt/spark-apps/avangrid_datastore.db/customer_count/FILE_YEAR=2021/FILE_MONTH=09"
CreateExternalTable.loadDataTest(path)
spark.sql("SHOW TABLES FROM avangrid_datastore_testing").show(false)
spark.sql("SELECT * FROM avangrid_datastore_testing.customer_count").show()
spark.sql("SELECT count(*) FROM avangrid_datastore_testing.customer_count").show()
 */


/* 
val querytt = "CREATE EXTERNAL TABLE avangrid_datastore_testing.customer_test2 ( `substation` varchar(255), `equip` char(12), `asset_dx` varchar(255), `primary_voltage` float, `secondary_voltage` float, `sum_of_customers` int) PARTITIONED BY ( `file_year` string, `file_month` string) ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' LOCATION '/avangrid_testing/customer_test/'"
val query = queries(querytt).toString().replace("${table_name}", tableName).replace("${database}",database)
spark.sql(querytt) 
*/

/* 
spark.sql("DROP DATABASE IF EXISTS avangrid CASCADE")
spark.sql("DROP DATABASE IF EXISTS avangrid_datastore_testing CASCADE")
spark.sql("CREATE DATABASE avangrid LOCATION '/opt/spark-apps/spark-warehouse/avangrid.db'")
spark.sql("CREATE DATABASE avangrid_datastore_testing LOCATION '/opt/spark-apps/spark-warehouse/avangrid_testing.db'")

spark.sql("CREATE EXTERNAL TABLE avangrid_datastore_testing.test_table (column1 varchar(255),column2 int ) LOCATION '/opt/spark-apps/spark-warehouse/avangrid_testing.db/test_table/'")
spark.sql("INSERT INTO avangrid_datastore_testing.test_table VALUES ('A',10)") 
spark.sql("SELECT * FROM avangrid_datastore_testing.test_table").show()
  */