import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
class DataLoader(spark: SparkSession) {

  // Chargement des données à partir d'un fichier CSV
  def loadCSVData(path: String, options: Map[String, String] = Map()): DataFrame = {
    spark.read
      .format("csv")
      .options(options)
      .load(path)
  }

  // Chargement des données à partir d'un fichier JSON
  def loadJSONData(path: String): DataFrame = {
    spark.read
      .format("json")
      .load(path)
  }

  // Chargement des données à partir d'une base de données PostgreSQL
    def loadPostgresData(query: String): DataFrame = {
    val connectionProperties = new Properties()
	connectionProperties.setProperty("driver", "org.postgresql.Driver")
	connectionProperties.setProperty("user", "tpid")
	connectionProperties.setProperty("password", "tpid")
    val url = connectionProperties.getProperty("url", "jdbc:postgresql://stendhal:5432/tpid2020")
    Class.forName(connectionProperties.getProperty("driver", "org.postgresql.Driver"))
    spark.read.jdbc(url, query, connectionProperties)
  }

}
