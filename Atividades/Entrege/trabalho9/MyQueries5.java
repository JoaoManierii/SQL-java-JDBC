package com.oracle.tutorial.jdbc;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyQueries5 {

  private Connection con;
  private JDBCUtilities settings;

  public MyQueries5(Connection connArg, JDBCUtilities settingsArg) {
    this.con = connArg;
    this.settings = settingsArg;
  }

  public void insertMyData1() throws SQLException {
    Statement stmt = null;
    String query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) "
        + "values (3000, 3000, 5, '2014-02-06', 36593, 'UFU', 'Pedro Alvares Sousa')";
    try {
      stmt = con.createStatement();
      long startTime = System.currentTimeMillis();
      stmt.executeUpdate(query);
      long endTime = System.currentTimeMillis();
      System.out.println("Um debito em IB2 inserido em " + (endTime - startTime) + " milisegundos");
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  public void insertMyData2() throws SQLException {
    PreparedStatement stmt = null;
    String query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) "
        + "values (?, ?, ?, ?, ?, ?, ?)";
    try {
      stmt = con.prepareStatement(query);
      stmt.setInt(1, 3001);
      stmt.setDouble(2, 3001);
      stmt.setInt(3, 4);
      stmt.setDate(4, Date.valueOf("2014-02-06"));
      stmt.setInt(5, 36593);
      stmt.setString(6, "UFU");
      stmt.setString(7, "Pedro Alvares Sousa");
      long startTime = System.currentTimeMillis();
      stmt.executeUpdate();
      long endTime = System.currentTimeMillis();
      System.out.println("Um debito em IB2 inserido em " + (endTime - startTime) + " milisegundos");
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  public void insertMyData1000() throws SQLException {
    Statement stmt = null;
    try {
      long startTime = System.currentTimeMillis();
      for (int numdeb = 3002; numdeb < 4002; numdeb++) {
        String query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) "
            + "values (" + numdeb + ", " + numdeb + ", 5, '2014-02-06', 36593, 'UFU', 'Pedro Alvares Sousa')";
        stmt = con.createStatement();
        stmt.executeUpdate(query);
        if ((numdeb % 50) == 0) {
          long endTime = System.currentTimeMillis();
          System.out.println((numdeb - 3000) + "\t" + (endTime - startTime));
        }
      }
      long endTime = System.currentTimeMillis();
      System.out.println("Um debito da IB2 inserido em " + (endTime - startTime) + " milisegundos");
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  public void insertMyData2000() throws SQLException {
    PreparedStatement stmt = null;
    try {
      long startTime = System.currentTimeMillis();
      for (int numdeb = 4002; numdeb < 5002; numdeb++) {
        String query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) "
            + "values (?, ?, ?, ?, ?, ?, ?)";
        stmt = con.prepareStatement(query);
        stmt.setInt(1, numdeb);
        stmt.setInt(2, numdeb);
        stmt.setInt(3, 5);
        stmt.setDate(4, Date.valueOf("2014-02-06"));
        stmt.setInt(5, 36593);
        stmt.setString(6, "UFU");
        stmt.setString(7, "Pedro Alvares Sousa");
        stmt.executeUpdate();
        if ((numdeb % 50) == 0) {
          long endTime = System.currentTimeMillis();
          System.out.println((numdeb - 4000) + "\t" + (endTime - startTime));
        }
      }
      long endTime = System.currentTimeMillis();
      System.out.println("Um debito da IB2 inserido em " + (endTime - startTime) + " milisegundos");
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  public void insertMyData3000() throws SQLException {
    PreparedStatement stmt = null;
    try {
      con.setAutoCommit(false);
  
      String query = "insert into debito (numero_debito, valor_debito, motivo_debito, data_debito, numero_conta, nome_agencia, nome_cliente) "
          + "values (?, ?, ?, ?, ?, ?, ?)";
      stmt = con.prepareStatement(query);
  
      long startTime = System.currentTimeMillis();
      for (int numdeb = 5002; numdeb < 6002; numdeb++) {
        stmt.setInt(1, numdeb);
        stmt.setInt(2, numdeb);
        stmt.setInt(3, 5);
        stmt.setDate(4, Date.valueOf("2014-02-06"));
        stmt.setInt(5, 36593);
        stmt.setString(6, "UFU");
        stmt.setString(7, "Pedro Alvares Sousa");
        stmt.addBatch();
  
        if ((numdeb % 50) == 0) {
          long endTime = System.currentTimeMillis();
          System.out.println((numdeb - 5000) + "\t" + (endTime - startTime));
        }
      }
  
      stmt.executeBatch();
      con.commit();
  
      long endTime = System.currentTimeMillis();
      System.out.println("Um debito da IB2 inserido em " + (endTime - startTime) + " milisegundos");
  
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
      con.setAutoCommit(true);
    }
  }
  

  public static void main(String[] args) {
    if (args.length < 1) {
      System.err.println("Properties file not specified at command line");
      return;
    }

    JDBCUtilities myJDBCUtilities;
    Connection myConnection = null;
    try {
      myJDBCUtilities = new JDBCUtilities(args[0]);
    } catch (Exception e) {
      System.err.println("Problem reading properties file " + args[0]);
      e.printStackTrace();
      return;
    }

    try {
      myConnection = myJDBCUtilities.getConnection();
      MyQueries5 myQueries = new MyQueries5(myConnection, myJDBCUtilities);

      myQueries.insertMyData1();
      myQueries.insertMyData2();
      myQueries.insertMyData1000();
      myQueries.insertMyData2000();
      myQueries.insertMyData3000();

    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      JDBCUtilities.closeConnection(myConnection);
    }
  }
}
