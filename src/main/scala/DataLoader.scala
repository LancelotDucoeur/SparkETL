import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.jdbc.JdbcDialects
import java.util.Properties


class DataLoader {

    val oracleUrl = "jdbc:oracle:thin:@stendhal.iem:1521:enss2023"
    val oracleUser = "ld754555"
    val oraclePassword = "ld754555"

    val dialect = new OracleDialect
	JdbcDialects.registerDialect(dialect)


    def loadToOracle(dataframe: DataFrame, tableName: String): Unit = {
        val connectionProperties = new Properties()
        connectionProperties.put("user", oracleUser)
        connectionProperties.put("password", oraclePassword)
        connectionProperties.put("driver", "oracle.jdbc.driver.OracleDriver")

        // Enregistrement du DataFrame dans la base de données Oracle
        dataframe.write
        .mode(SaveMode.Overwrite)
        .jdbc(oracleUrl, tableName, connectionProperties)
    }
}



