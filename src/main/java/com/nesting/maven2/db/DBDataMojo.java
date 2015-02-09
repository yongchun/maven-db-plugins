package com.nesting.maven2.db;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * Mojo for filling databases with data.
 * @goal data
 */
public class DBDataMojo 
    extends AbstractDBMojo {

    /**
     * The directory that contains data
     * scripts.
     * @parameter
     * @required
     */
    private File[] dbDataScriptsDirectory;
    
    /**
     * {@inheritDoc}
     */
    public void executeInternal() 
        throws MojoExecutionException, 
        MojoFailureException {
        
        try {
            Connection con = openApplicationDbConnection();
            for (int i=0; i<dbDataScriptsDirectory.length; i++) {
                executeScriptsInDirectory(
                    dbDataScriptsDirectory[i], con);
            }
            
        } catch(SQLException se) {
            throw new MojoExecutionException(
                "Error executing database scripts", se);
        } catch(IOException ioe) {
            throw new MojoExecutionException(
                "Error executing database scripts", ioe);
        }
        
        
    }

}
