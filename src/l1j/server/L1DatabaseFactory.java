/*
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA
 * 02111-1307, USA.
 *
 * http://www.gnu.org/copyleft/gpl.html
 */
package l1j.server;

import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class L1DatabaseFactory {
	private HikariDataSource _source;
	private static L1DatabaseFactory _instance;
	private static Logger _log = LoggerFactory
			.getLogger(L1DatabaseFactory.class);
	private static String _driver;
	private static String _url;
	private static String _user;
	private static String _password;

	public static void setDatabaseSettings(final String driver,
			final String url, final String user, final String password) {
		_driver = driver;
		_url = url;
		_user = user;
		_password = password;
	}

	public L1DatabaseFactory() throws SQLException {
		try {
			HikariConfig config = new HikariConfig();

			// Configurações básicas
			config.setDriverClassName(_driver);
			config.setJdbcUrl(_url);
			config.setUsername(_user);
			config.setPassword(_password);

			// Pool settings - otimizado para performance
			config.setMaximumPoolSize(30); // Equivalente a 3 partitions x 10
											// connections
			config.setMinimumIdle(10);

			// Connection timeout e idle timeout
			config.setConnectionTimeout(30000); // 30 segundos
			config.setIdleTimeout(600000); // 10 minutos
			config.setMaxLifetime(1800000); // 30 minutos

			// Test query para validar conexões
			config.setConnectionTestQuery("/* ping */ SELECT 1");

			// Performance optimizations
			config.setAutoCommit(true);
			config.setTransactionIsolation("TRANSACTION_READ_COMMITTED");

			// Pool name para debugging
			config.setPoolName("L1JDatabasePool");

			// Leak detection (útil para desenvolvimento)
			config.setLeakDetectionThreshold(60000); // 60 segundos

			// Propriedades específicas do MariaDB para melhor performance
			config.addDataSourceProperty("cachePrepStmts", "true");
			config.addDataSourceProperty("prepStmtCacheSize", "250");
			config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
			config.addDataSourceProperty("useServerPrepStmts", "true");
			config.addDataSourceProperty("useLocalSessionState", "true");
			config.addDataSourceProperty("rewriteBatchedStatements", "true");
			config.addDataSourceProperty("cacheResultSetMetadata", "true");
			config.addDataSourceProperty("cacheServerConfiguration", "true");
			config.addDataSourceProperty("elideSetAutoCommits", "true");
			config.addDataSourceProperty("maintainTimeStats", "false");

			_source = new HikariDataSource(config);

			_log.info("HikariCP Database Connection Pool initialized successfully");

		} catch (Exception e) {
			_log.error("Database Connection FAILED", e);
			throw new SQLException("could not init DB connection:" + e);
		}
	}

	public void shutdown() {
		try {
			if (_source != null && !_source.isClosed()) {
				_source.close();
				_log.info("Database connection pool closed successfully");
			}
		} catch (Exception e) {
			_log.error("Error closing database connection pool", e);
		}
		try {
			_source = null;
		} catch (Exception e) {
			_log.error("Error nullifying datasource", e);
		}
	}

	public static L1DatabaseFactory getInstance() throws SQLException {
		if (_instance == null) {
			_instance = new L1DatabaseFactory();
		}
		return _instance;
	}

	public Connection getConnection() {
		Connection con = null;
		while (con == null) {
			try {
				con = _source.getConnection();
			} catch (SQLException e) {
				_log.warn(
						"L1DatabaseFactory: getConnection() failed, trying again",
						e);
			}
		}
		return con;
	}
}