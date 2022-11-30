// webs de referencia
// https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/ch04.html
// https://www.learningjournal.guru/courses/spark/spark-foundation-training/spark-sql-database-and-table/

//CREAMOS DATABASES Y TABLES PARA TESTEAR FUNC.

//DROPEAMOS DE OTRA PASADA
spark.sql("DROP DATABASE IF EXISTS test_db_locat CASCADE")
spark.sql("DROP DATABASE IF EXISTS test_db_noloc CASCADE")


//EXTERNAL
spark.sql("CREATE DATABASE test_db_locat LOCATION '/opt/spark-apps/spark-warehouse/test_db_locat.db'")
spark.sql("USE  test_db_locat")
spark.sql("SHOW DATABASES").show()
spark.sql("DESCRIBE DATABASE test_db_locat").show(false)
spark.sql("CREATE EXTERNAL TABLE test_db_locat.test_table (column1 varchar(255),column2 int ) LOCATION '/opt/spark-apps/spark-warehouse/test_db_locat.db/test_table/'")
spark.sql("SHOW TABLES FROM test_db_locat").show()
spark.sql("SELECT * FROM test_db_locat.test_table").show()
spark.sql("DESCRIBE FORMATTED test_db_locat.test_table").show(false)
spark.sql("DESCRIBE TABLE test_db_locat.test_table").show(false)
spark.sql("INSERT INTO test_db_locat.test_table VALUES ('A',10)") 
spark.sql("SELECT * FROM test_db_locat.test_table").show()




//INTERNAL
spark.sql("CREATE DATABASE test_db_noloc")
spark.sql("USE  test_db_noloc")
spark.sql("SHOW DATABASES").show()
spark.sql("DESCRIBE DATABASE test_db_noloc").show(false)
spark.sql("CREATE TABLE test_db_noloc.test_table (column1 varchar(255),column2 int )")
spark.sql("SHOW TABLES FROM test_db_noloc").show()
spark.sql("SELECT * FROM test_db_noloc.test_table").show()
spark.sql("DESCRIBE FORMATTED test_db_noloc.test_table").show(false)
spark.sql("DESCRIBE TABLE test_db_noloc.test_table").show(false)
spark.sql("INSERT INTO test_db_noloc.test_table VALUES ('A',10)") 
spark.sql("SELECT * FROM test_db_noloc.test_table").show()



//Partes del script loadHdfsFiles:

/* 1. Crea la estructura de la tabla:

                def createExternalTableStruct(database: String, tableName: String): Unit = {
                    val queryName = s"query_${tableName}"
                    val query = queries(queryName).toString().replace("${tableName}", tableName).replace("${database}",database)
                    if (!spark.catalog.tableExists(database + "." + tableName)) {
                    spark.sql(query)
                    }
                }


2. Carga los datos de cierto path a la tabla creada en paso previo:

                def loadDataToExternalTable(database: String, tableName: String, inputFilePath: String,
                                            partitionColumn0: String, partitionValue0: String,
                                            partitionColumn1: String, partitionValue1: String): Unit = {
                    val o = "options_" + tableName
                    val df1 = spark.table(s"${database}.${tableName}").drop(col(s"${partitionColumn0}")).drop(col(s"${partitionColumn1}"))
                    val df2 = spark.read.options(options(o)).schema(df1.schema).csv(inputFilePath)
                    df2.createOrReplaceTempView("myPreOutputTable")

                    //Write the data in the table
                    spark.sql(
                    s"""INSERT OVERWRITE TABLE $database.$tableName
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
 */


// Hacemos el proceso para una tabla puntual (customer_count)
val database = "avangrid_datastore"
val table_name = "customer_count"

spark.sql("CREATE DATABASE avangrid_datastore LOCATION '/opt/spark-apps/spark-warehouse/'")
spark.sql("DESCRIBE DATABASE avangrid_datastore").show(false)

// Query para la tabla en cuestion
val query_customer_count  = """CREATE EXTERNAL TABLE `${database}`.`${table_name}`(
    |  `substation` varchar(255),
    |  `equip` char(12),
    |  `asset_dx` varchar(255),
    |  `primary_voltage` float,
    |  `secondary_voltage` float,
    |  `sum_of_customers` int
    |  ) 
    |  
    |  PARTITIONED BY ( `file_year` string, `file_month` string) 
    |  ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
    |  WITH SERDEPROPERTIES ( 'field.delim'='\\;', 'serialization.format'='\\;') 
    |  STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
    |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
    |  LOCATION '/opt/spark-apps/spark-warehouse/'""".stripMargin


val query_create_schema = query_customer_count.replace("${table_name}", table_name).replace("${database}",database)
spark.sql(query_create_schema)
spark.sql("SELECT * FROM avangrid_datastore.customer_count").show()
spark.sql("DESCRIBE TABLE avangrid_datastore.customer_count").show(false)
spark.sql("DESCRIBE FORMATTED avangrid_datastore.customer_count").show(false)


//Buscamos regex en el path puntual para UN SOLO FILE de customer_count

import scala.util.matching.Regex
val path = "/opt/spark-apps/avangrid_datastore.db/customer_count/FILE_YEAR=2021/FILE_MONTH=09/AM_CUSTOMER_COUNTS_20210913_CMP.csv"            

val yearExp = new Regex("(file_year)=([0-9]{4})")
val monthExp = new Regex("(file_month)=([0-9]{2})")


val part0 = yearExp findAllIn path.toLowerCase
val part1 = monthExp findAllIn path.toLowerCase


val  partition_column_0 = part0.group(1)
val  partition_value_0 = part0.group(2)
val  partition_column_1 = part1.group(1)
val  partition_value_1 = part1.group(2)

// creamos manualmente las options de parseo en un map, y vía dataframes cargamos nuestra data en una vista temporal 

val options = Map("header" -> "false", "sep" -> ";")

val df1 = spark.table(s"${database}.${table_name}").drop(col(s"${partition_column_0}")).drop(col(s"${partition_column_1}"))
val df2 = spark.read.options(options).schema(df1.schema).csv(path)
df2.createOrReplaceTempView("myPreOutputTable")
spark.sql("SELECT * FROM myPreOutputTable").show()
spark.sql("DESCRIBE FORMATTED myPreOutputTable").show(false)

//Nos resta insertar esta data de la vista, en nuestro schema creado

val query_write = s"""INSERT OVERWRITE TABLE $database.$table_name
      | PARTITION (
      | ${partition_column_0} = ${partition_value_0},
      | ${partition_column_1} = ${partition_value_1}
      | )
      | SELECT * FROM myPreOutputTable""".stripMargin

spark.sql(query_write)
spark.sql("SELECT * FROM avangrid_datastore.customer_count").show()




val query_test1  = """CREATE EXTERNAL TABLE avangrid_datastore.test_table (
    |  SELECT * FROM myPreOutputTable)
    |  LOCATION '/opt/spark-apps/spark-warehouse/' """.stripMargin
spark.sql(query_test1).show()


val query_test2  = """CREATE TABLE avangrid_datastore.test_table (
    |  SELECT * FROM myPreOutputTable ) """.stripMargin
spark.sql(query_test2).show()


spark.sql("SHOW TABLES FROM avangrid_datastore").show()


//cat /etc/*-release
//cat /etc/sudoers
//chmod u+x /opt/spark-apps/spark-warehouse/

// spark-shell --jars /opt/spark-libraries/microsoft/azure/synapse/synapseutils_2.12/1.4/synapseutils_2.12-1.4.jar,/opt/spark-libraries/typesafe/config/1.4.1/config-1.4.1.jar -I loadHdfsFiles.scala


//1. No puedo correr spark-shell con root user error: 
//Exception in thread "main" java.lang.IllegalArgumentException: basedir must be absolute: ?/.ivy2/local

// 2. Corriendo sin root user, al querer llenar una tabla con contenido

// 3. Al crear tablas el siguiente warn: A Hive serde table will be created as there is no table provider specified. 
// You can set spark.sql.legacy.createHiveTableByDefault to false so that native data source table will be created instead.