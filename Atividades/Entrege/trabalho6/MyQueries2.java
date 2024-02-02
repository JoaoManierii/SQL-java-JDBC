package com.oracle.tutorial.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyQueries2 {
  
  Connection con;
  JDBCUtilities settings;  
  
  public MyQueries2(Connection connArg, JDBCUtilities settingsArg) {
    this.con = connArg;
    this.settings = settingsArg;
  }

  public static void getMyData(Connection con) throws SQLException {
  Statement stmt = null;
  String query =
            "SELECT d.nome_cliente, SUM(d.saldo_deposito) AS total_depositos " +
            "FROM deposito d " +
            "LEFT JOIN emprestimo e ON d.numero_conta = e.numero_conta AND d.nome_cliente = e.nome_cliente AND d.nome_agencia = e.nome_agencia " +
            " e.numero_emprestimo IS NULL " +
            "GROUP BY d.nome_cliente " +
            "HAVING COUNT(*) = 1";

        try {
            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            System.out.println("Clientes com apenas depósitos, a soma de depósitos e sem empréstimos:");
            while (rs.next()) {
                String cliente = rs.getString("nome_cliente");
                double totalDepositos = rs.getDouble("total_depositos");
                System.out.println(cliente + ": " + totalDepositos);
            }
        } catch (SQLException e) {
            JDBCUtilities.printSQLException(e);
        } finally {
            if (stmt != null) { stmt.close(); }
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

 	MyQueries2.getMyData(myConnection);

    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      JDBCUtilities.closeConnection(myConnection);
    }

  }
}
