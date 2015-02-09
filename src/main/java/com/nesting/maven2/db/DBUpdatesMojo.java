package com.nesting.maven2.db;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;

/**
 * Mojo for executing database update scripts.
 * @goal update
 */
public class DBUpdatesMojo 
    extends AbstractDBMojo {

    /**
     * The directory that contains update
     * scripts.
     * @parameter
     * @required
     */
    private File[] dbUpdateScriptsDirectory;
    
    /**
     * {@inheritDoc}
     */
    public void executeInternal() 
        throws MojoExecutionException, 
        MojoFailureException {
        
        try {
            Connection con = openApplicationDbConnection();
            for (int i=0; i<dbUpdateScriptsDirectory.length; i++) {
                executeScriptsInDirectory(
                    dbUpdateScriptsDirectory[i], con);
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
