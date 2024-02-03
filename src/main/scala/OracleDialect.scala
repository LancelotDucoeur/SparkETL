import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

class OracleDialect extends JdbcDialect {
	override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
		case BooleanType => Some(JdbcType("NUMBER(1)", java.sql.Types.INTEGER))
		case IntegerType => Some(JdbcType("NUMBER(10)", java.sql.Types.INTEGER))
		case LongType => Some(JdbcType("NUMBER(19)", java.sql.Types.BIGINT))
		case FloatType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.FLOAT))
		case DoubleType => Some(JdbcType("NUMBER(19, 4)", java.sql.Types.DOUBLE))
		case ByteType => Some(JdbcType("NUMBER(3)", java.sql.Types.SMALLINT))
		case ShortType => Some(JdbcType("NUMBER(5)", java.sql.Types.SMALLINT))
		//case StringType => Some(JdbcType("VARCHAR2(4000)", java.sql.Types.VARCHAR))
		case DateType => Some(JdbcType("DATE", java.sql.Types.DATE))
		case StringType => Some(JdbcType("CLOB", java.sql.Types.CLOB))
		case _ => None
	}

	override def canHandle(url: String): Boolean = url.startsWith("jdbc:oracle")
}
    