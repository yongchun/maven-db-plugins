
package com.nesting.maven2.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.settings.Server;
import org.apache.maven.settings.Settings;

/**
 * Abstract mojo that all DB related mojos
 * inherit from.
 */
public abstract class AbstractDBMojo
    extends AbstractMojo {    
    
    /**
     * The database connection settings for
     * the application.
     * @parameter
     * @required
     */
    private DatabaseConnectionSettings appDbConnectionSettings;

    /**
     * The database connection settings for
     * the database administrator.
     * @parameter
     * @required
     */
    private DatabaseConnectionSettings adminDbConnectionSettings;

    /**
     * The batch size when executing batches.
     * @parameter default-value="20"
     * @required
     */
    private int batchSize;

    /**
     * Whether or not to use SQL batches.
     * @parameter default-value="true"
     * @required
     */
    private boolean useBatch;
    
    /**
     * The {@link Settings} object.
     * @parameter default-value="${settings}"
     * @required
     * @readonly
     */
    private Settings settings;
    
    /**
     * The String that delimits sql statements in a file.
     * @parameter default-value=";"
     * @required
     */
    private String sqlDelimiter;

    /**
	 * Charset for sql script files.
	 * @parameter
	 */
	private String scriptEncoding;
    
    /**
     * Child mojos need to implement this.
     * @throws MojoExecutionException on error
     * @throws MojoFailureException on error
     */
    public abstract void executeInternal() 
        throws MojoExecutionException, 
        MojoFailureException;
    
    /**
     * {@inheritDoc}
     */
    public final void execute() 
        throws MojoExecutionException, 
        MojoFailureException {
        checkDbSettings(adminDbConnectionSettings, "admin");
        checkDbSettings(appDbConnectionSettings, "application");
        executeInternal();
    }
    
    /**
     * Checks the given database connection settings.
     * @param dbSettings the settings to check
     * @param name the name of the settings
     * @throws MojoExecutionException on error
     * @throws MojoFailureException on error
     */
    private void checkDbSettings(
        DatabaseConnectionSettings dbSettings, String name) 
        throws MojoExecutionException, 
        MojoFailureException {
        
        // check server
        if (!StringUtils.isEmpty(dbSettings.getServerId())) {
            Server server = settings.getServer(dbSettings.getServerId());
            if (server==null) {
                throw new MojoFailureException(
                    "["+name+"] Server ID: "
                    +dbSettings.getServerId()+" not found!");
            } else if (StringUtils.isEmpty(server.getUsername())) {
                throw new MojoFailureException(
                    "["+name+"] Server ID: "+dbSettings.getServerId()+" found, "
                    +"but username is empty!");
            }
            
        // check non server settings
        } else if (StringUtils.isEmpty(dbSettings.getUserName())) {
            throw new MojoFailureException("["+name+"] No username defined!");
        }
        
        // check url and jdbc driver
        if (StringUtils.isEmpty(dbSettings.getJdbcUrl())) {
            throw new MojoFailureException("["+name+"] No jdbc url defined!");
        } else if (StringUtils.isEmpty(dbSettings.getJdbcDriver())) {
            throw new MojoFailureException(
                "["+name+"] No jdbc driver defined!");
        }
        
    }
    
    /**
     * Executes all of the sql scripts in a given directory
     * using the given database connection.
     * @param directory the directory where the scripts reside
     * @param con the database connection
     * @throws SQLException on error
     * @throws MojoFailureException on error
     * @throws MojoExecutionException on error
     * @throws IOException on error
     */
    protected void executeScriptsInDirectory(File directory, Connection con) 
        throws SQLException,
        MojoFailureException,
        MojoExecutionException,
        IOException {
        
        // talk a bit :)
        getLog().info("Executing scripts in: "+directory.getName());
        
        // make sure we can read it, and that it's 
        // a file and not a directory
        if (!directory.isDirectory()) {
            throw new MojoFailureException(
                directory.getName()+" is not a directory");
        }
        
        // get all files in directory
        File[] files = directory.listFiles();
        
        // sort
        Arrays.sort(files, new Comparator() {
            public int compare(Object arg0, Object arg1) {
                return ((File)arg0).getName().compareTo(((File)arg1).getName());
            } }
        );
        
        // loop through all the files and execute them
        for (int i = 0; i<files.length; i++) {
            if (!files[i].isDirectory() && files[i].isFile()) {
                double startTime = System.currentTimeMillis();
                if (useBatch) {
                    batchExecuteSqlScript(files[i], con);
                } else {
                    executeSqlScript(files[i], con);
                }
                double endTime = System.currentTimeMillis();
                double elapsed = ((endTime-startTime)/1000.0);
                getLog().info(" script completed execution in "+elapsed+" second(s)");
            }
        }
        
    }
    
    /**
     * Batch executes a script file.
     * @param file the file
     * @param con the connection
     * @throws SQLException on error
     * @throws MojoFailureException on error
     * @throws MojoExecutionException on error
     * @throws IOException on error
     */
    protected void batchExecuteSqlScript(File file, Connection con) 
        throws SQLException,
        MojoFailureException,
        MojoExecutionException,
        IOException {
            
        // talk a bit :)
        getLog().info("batch executing script: "+file.getName());
        
        // make sure we can read it, and that it's 
        // a file and not a directory
        if (!file.exists() || !file.canRead() 
            || file.isDirectory() || !file.isFile()) {
            throw new MojoFailureException(file.getName()+" is not a file");
        }
        
        // open input stream to file
        InputStream ips = new FileInputStream(file);
        
        // if it's a compressed file (gzip) then unzip as
        // we read it in
        if (file.getName().toUpperCase().endsWith("GZ")) {
            ips = new GZIPInputStream(ips);
        }

        // check encoding
        checkEncoding();
        
        // our file reader
        Reader reader;
        reader = new InputStreamReader(ips, scriptEncoding);
        
        // create SQL Statement
        Statement st = con.createStatement();
        
        StringBuffer sql = new StringBuffer();
        String line;
        BufferedReader in = new BufferedReader(reader);

        // loop through the statements
        int execCount = 0;
        List sqlLines = new ArrayList();
        while ((line = in.readLine()) != null) {
            
            // append the line
            line.trim();
            sql.append("\n").append(line);
            
            // if the line ends with the delimiter, then
            // lets execute it
            if (sql.toString().endsWith(sqlDelimiter)) {
                String sqlLine = sql.substring(
                    0, sql.length() - sqlDelimiter.length());
                sqlLines.add(sqlLine);
                sql.replace(0, sql.length(), "");
                execCount++;
                if (sqlLines.size()>=batchSize) {
                    executeBatch(st, sqlLines);
                    sqlLines.clear();
                }
            }
        }
        
        // execute last statement
        if (sql.toString().trim().length()>0) {
            sqlLines.add(sql.toString());
            sql.replace(0, sql.length(), "");
            execCount++;
            executeBatch(st, sqlLines);
            sqlLines.clear();
        } else if (sqlLines.size()>0) {
            executeBatch(st, sqlLines);
            sqlLines.clear();
        }
        
        st.close();
        reader.close();
        in.close();
        
        getLog().info(" "+execCount+" statements batch executed from "+file.getName());
    }
    
    /**
     * Executes the given sql script, using the given
     * connection.
     * @param file the file to execute
     * @param con the connection
     * @throws SQLException on error
     * @throws MojoFailureException on error
     * @throws MojoExecutionException on error
     * @throws IOException on error
     */
    protected void executeSqlScript(File file, Connection con) 
        throws SQLException,
        MojoFailureException,
        MojoExecutionException,
        IOException {
        
        // talk a bit :)
        getLog().info("executing script: "+file.getName());
        
        // make sure we can read it, and that it's 
        // a file and not a directory
        if (!file.exists() || !file.canRead() 
            || file.isDirectory() || !file.isFile()) {
            throw new MojoFailureException(file.getName()+" is not a file");
        }
        
        // open input stream to file
        InputStream ips = new FileInputStream(file);
        
        // if it's a compressed file (gzip) then unzip as
        // we read it in
        if (file.getName().toUpperCase().endsWith("GZ")) {
            ips = new GZIPInputStream(ips);
            getLog().info(" file is gz compressed, using gzip stream");
        }
 
        // check encoding
        checkEncoding();
        
        // our file reader
        Reader reader;
        reader = new InputStreamReader(ips, scriptEncoding);
        
        // create SQL Statement
        Statement st = con.createStatement();
        
        StringBuffer sql = new StringBuffer();
        String line;
        BufferedReader in = new BufferedReader(reader);

        // loop through the statements
        int execCount = 0;
        while ((line = in.readLine()) != null) {
            
            // append the line
            line.trim();
            sql.append("\n").append(line);
            
            // if the line ends with the delimiter, then
            // lets execute it
            if (sql.toString().endsWith(sqlDelimiter)) {
                String sqlLine = sql.substring(
                    0, sql.length() - sqlDelimiter.length());
                executeStatement(st, sqlLine);
                sql.replace(0, sql.length(), "");
                execCount++;
            }
        }
        
        // execute last statement
        if (sql.toString().trim().length()>0) {
            executeStatement(st, sql.toString());
            sql.replace(0, sql.length(), "");
            execCount++;
        }
        
        st.close();
        reader.close();
        in.close();
        
        getLog().info(" "+execCount+" statements executed from "+file.getName());
    }
    
    /**
     * Executes a batch update.
     * @param st the statement
     * @param sqlLines the sql lines
     * @throws SQLException on error
     */
    protected void executeBatch(Statement st, List sqlLines)
        throws SQLException {
        
        if (getLog().isDebugEnabled()) {
            getLog().debug("Executing batch");
        }
        
        // add to batch
        for (int i=0; i<sqlLines.size(); i++) {
            st.addBatch((String)sqlLines.get(i));
        }
        
        int[] ret  = st.executeBatch();
        if (getLog().isDebugEnabled()) {
            getLog().debug("    "+ret.length+" statement(s) executed");
        }
        
        for (int i=0; i<ret.length; i++) {
            if (ret[i]==Statement.SUCCESS_NO_INFO
                && getLog().isDebugEnabled()) {
                getLog().debug("    statement "+i+" processed successfully "
                    + "without return results");
                
            } else if (ret[i]==Statement.EXECUTE_FAILED) {
                getLog().error("    error durring batch execution of statement: "+sqlLines.get(i));
                throw new SQLException("Error executing: "+sqlLines.get(i));
                
            } else if (ret[i]>=0 && getLog().isDebugEnabled()) {
                getLog().debug("    statement "+i+" processed successfully "
                    + " with "+ret[i]+" records effected");
            }
        }
        
    }
    
    /**
     * Runs the given SQL statement.
     * 
     * @param st
     *            the statement to run it on
     * @param sqlLine
     *            the sql statement
     * @throws SQLException
     *             on error
     */
    protected void executeStatement(Statement st, String sqlLine) 
        throws SQLException {
        if (getLog().isDebugEnabled()) {
            getLog().debug("    executing:\n"+sqlLine);
        }
        boolean execRet = false;
        try {
            execRet = st.execute(sqlLine);
        } catch(SQLException sqle) {
            SQLException se = new SQLException(
                sqle.getMessage()+"\n\nSQL:\n"+sqlLine, 
                sqle.getSQLState(), 
                sqle.getErrorCode());
            se.setNextException(sqle);
            throw se;
        }
        boolean loop = true;
        while (loop) {
            if (execRet) {
                getLog().warn(" statement returned a resultset");
            } else {
                // Got an update count
                int count = st.getUpdateCount();

                if (count == -1) {
                    // Nothing left
                    loop = false;
                } else if (getLog().isDebugEnabled()){
                    // An update count was returned
                    getLog().debug("    "+count+" row(s) updated");
                }
            }
            if (loop) {
                execRet = st.getMoreResults();
            }
        }
    }
    
    /**
     * Returns a {@link Connection} to the application
     * database.
     * @return the connection
     * @throws SQLException on error
     * @throws MojoFailureException on error
     */
    protected Connection openApplicationDbConnection() 
        throws SQLException,
        MojoFailureException {
        return openConnection(appDbConnectionSettings);
    }
    
    /**
     * Returns a {@link Connection} to the application
     * database.
     * @return the connection
     * @throws SQLException on error
     * @throws MojoFailureException on error
     */
    protected Connection openAdminDbConnection() 
        throws SQLException,
        MojoFailureException {
        return openConnection(adminDbConnectionSettings);
    }
    
    /**
     * Opens a connection using the given settings.
     * @param dbSettings the connection settings
     * @return the Connection
     * @throws SQLException on error
     * @throws MojoFailureException on error
     */
    private Connection openConnection(DatabaseConnectionSettings dbSettings) 
        throws SQLException,
        MojoFailureException {
        
        String username = null;
        String password = null;
        
        // use settings to get authentication info for the given server
        if (!StringUtils.isEmpty(dbSettings.getServerId())) {
            Server server = settings.getServer(dbSettings.getServerId());
            username = server.getUsername();
            password = server.getPassword();
            
        // use settings in pom.xml
        } else {
            username = dbSettings.getUserName();
            password = dbSettings.getPassword();
            
        }
        
        
        // make sure the driver is good
        try {
            Class.forName(dbSettings.getJdbcDriver());
        } catch(Exception e) {
            throw new MojoFailureException(e.getMessage());
        }
        
        // consult the driver manager for the connection
        Connection con = DriverManager.getConnection(
            dbSettings.getJdbcUrl(), 
            username,
            password);
        
        // we're good :)
        return con;
    }

	private void checkEncoding() {
		if (scriptEncoding == null) {
			scriptEncoding = Charset.defaultCharset().name();
			getLog().warn("Using platform encoding (" + scriptEncoding + ") for executing script, i.e. build is platform dependent!");
		} else {
			getLog().info(" setting encoding for executing script: " + scriptEncoding);
		}
	}
}










