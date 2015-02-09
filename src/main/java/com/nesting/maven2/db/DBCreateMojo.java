package com.nesting.maven2.db;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * Mojo for creating databases.
 * @goal create
 */
public class DBCreateMojo 
    extends AbstractDBMojo {

    /**
     * The sql statement that creates the database.
     * @parameter 
     * @required
     */
    private String dbCreateStatements;
    
    /**
     * {@inheritDoc}
     */
    public void executeInternal() 
        throws MojoExecutionException, 
        MojoFailureException {
        
        try {
            // get connection and create statement
            Connection con  = openAdminDbConnection();
            
            // create file
            File out = File.createTempFile("dbCreate-", ".sql");
            out.deleteOnExit();
            
            // write statements to it
            FileOutputStream fops = new FileOutputStream(out);
            fops.write(dbCreateStatements.getBytes());
            fops.flush();
            fops.close();
            
            // execute statement
            executeSqlScript(out, con);
            
            
        } catch(SQLException se) {
            throw new MojoExecutionException(
                "Error executing database query", se);
            
        } catch(IOException ioe) {
            throw new MojoExecutionException(
                "Error executing database query", ioe);
        }
        
        
    }

}
