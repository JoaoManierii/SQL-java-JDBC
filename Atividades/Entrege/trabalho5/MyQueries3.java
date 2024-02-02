package com.oracle.tutorial.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyQueries3 {
  
  Connection con;
  JDBCUtilities settings;  
  
  public MyQueries3(Connection connArg, JDBCUtilities settingsArg) {
    this.con = connArg;
    this.settings = settingsArg;
  }

  public static void getMyData(Connection con) throws SQLException {
  Statement stmt = null;
     String query =
            "SELECT d.nome_cliente, SUM(d.saldo_deposito) AS total_depositos, SUM(e.valor_emprestimo) AS total_emprestimos " +
            "FROM deposito d " +
            "JOIN emprestimo e ON d.numero_conta = e.numero_conta AND d.nome_cliente = e.nome_cliente AND d.nome_agencia = e.nome_agencia " +
            "GROUP BY d.nome_cliente";

        try {
            stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            System.out.println("Clientes com depósitos e empréstimos e suas respectivas somas:");
            while (rs.next()) {
                String cliente = rs.getString("nome_cliente");
                double totalDepositos = rs.getDouble("total_depositos");
                double totalEmprestimos = rs.getDouble("total_emprestimos");
                System.out.println(cliente + ": Depósitos: " + totalDepositos + ", Empréstimos: " + totalEmprestimos);
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

 	MyQueries3.getMyData(myConnection);

    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      JDBCUtilities.closeConnection(myConnection);
    }

  }
}
