package com.oracle.tutorial.jdbc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

public class MyQueriess {

    Connection con;
    JDBCUtilities settings;

    public MyQueriess(Connection connArg, JDBCUtilities settingsArg) {
        this.con = connArg;
        this.settings = settingsArg;
    }

    public static void populatetable(Connection con) throws IOException, SQLException {
        Statement stmt = null;
        String query = "";
        BufferedReader inputStream = null;

        try {
            stmt = con.createStatement();
            Scanner scanned_line = null;
            String line;
            String[] value;
            value = new String[7];
            int countv;
            inputStream = new BufferedReader(new FileReader("src/com/oracle/tutorial/jdbc/Debito-populate-table.txt"));
            while ((line = inputStream.readLine()) != null) {
                countv = 0;
                scanned_line = new Scanner(line);
                scanned_line.useDelimiter("\t");
                while (scanned_line.hasNext()) {
                    System.out.println(value[countv++] = scanned_line.next());
                } 
                if (scanned_line != null) {
                    scanned_line.close();
                }
                query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) "
                        + "values (" +
                        value[0] + ", " + value[1] + ", " + value[2] + ", '" + value[3] + "', " + value[4] + ", '"
                        + value[5]
                        + "', '" + value[6] + "');";
                stmt.executeUpdate(query);
            }
        } catch (SQLException e) {
            JDBCUtilities.printSQLException(e);

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    public static void main(String[] args) {
        JDBCUtilities myJDBCUtilities;
        Connection myConnection = null;
        if (args[0] == null) {
            System.err.println("Properties file not specified at command line");
            return;
        } else {
            try {
                myJDBCUtilities = new JDBCUtilities(args[0]);
            } catch (Exception e) {
                System.err.println("Problem reading properties file " + args[0]);
                e.printStackTrace();
                return;
            }
        }

        try {
            myConnection = myJDBCUtilities.getConnection();

            MyQueriess.populatetable(myConnection);

        } catch (SQLException e) {
            JDBCUtilities.printSQLException(e);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            JDBCUtilities.closeConnection(myConnection);
        }

    }
}
