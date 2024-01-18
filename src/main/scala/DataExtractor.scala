import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
import org.apache.spark.sql.types.{StructType, StringType, IntegerType, DateType}


class DataExtractor(spark: SparkSession) {
  // Chargement des données à partir d'un fichier CSV
  def CSVData(path: String, schema: StructType, options: Map[String, String] = Map()): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true") // Habituellement, les fichiers CSV ont une ligne d'en-tête
      .schema(schema)
      .options(options)
      .load(path)
  }

  // Chargement des données à partir d'un fichier JSON
  def JSONData(path: String): DataFrame = {
    spark.read
      .format("json")
      .load(path)
  }

  // Chargement des données à partir d'une base de données PostgreSQL
  def PostgresData(query: String): DataFrame = {
    val connectionProperties = new Properties()
	  connectionProperties.setProperty("driver", "org.postgresql.Driver")
	  connectionProperties.setProperty("user", "tpid")
	  connectionProperties.setProperty("password", "tpid")
    val url = connectionProperties.getProperty("url", "jdbc:postgresql://stendhal:5432/tpid2020")
    Class.forName(connectionProperties.getProperty("driver", "org.postgresql.Driver"))
    spark.read.jdbc(url, query, connectionProperties)
  }

}


