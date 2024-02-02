package com.oracle.tutorial.jdbc;

import java.util.Scanner;
import java.sql.DatabaseMetaData;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale; 
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Date;
import java.io.IOException;

public class MyQueries4 {

  Connection con;
  JDBCUtilities settings;

  public MyQueries4(Connection connArg, JDBCUtilities settingsArg) {
    this.con = connArg;
    this.settings = settingsArg;
  }

  public static void insertRow(Connection con, int numero_debito, double valor_debito, String motivo_debito, String data_debito, int numero_conta, String nome_agencia, String nome_cliente)
        throws SQLException {
    Statement stmt = null;
    try {
      stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

      ResultSet uprs = stmt.executeQuery("SELECT * FROM debito");
      uprs.moveToInsertRow(); 
      uprs.updateInt("numero_debito", numero_debito);
      uprs.updateDouble("valor_debito", valor_debito);
      uprs.updateString("motivo_debito", motivo_debito);
      uprs.updateDate("data_debito", Date.valueOf(data_debito));
      uprs.updateInt("numero_conta", numero_conta);
      uprs.updateString("nome_agencia", nome_agencia);
      uprs.updateString("nome_cliente", nome_cliente);
      uprs.insertRow(); 
      uprs.beforeFirst(); 
    } catch (SQLException e) {
      JDBCTutorialUtilities.printSQLException(e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  public static void modifyDeposits(Connection con) throws SQLException {
    Statement stmt = null;
    try {
      stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

      System.out.println("Digite a porcentagem de juros como um número real (Ex.: 5% = 1.05):");
      Scanner in = new Scanner(System.in);
      in.useLocale(Locale.US); 
      double percentage = in.nextDouble();

      ResultSet uprs = stmt.executeQuery("SELECT * FROM DEPOSITOS");
      while (uprs.next()) {
        double saldo = uprs.getDouble("SALDO");
        double juros = saldo * percentage;
        double novoSaldo = saldo + juros;

        uprs.updateDouble("SALDO", novoSaldo);
        uprs.updateRow();
      }
    } catch (SQLException e) {
      JDBCTutorialUtilities.printSQLException(e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  public static void cursorHoldabilitySupport(Connection conn) throws SQLException {
    DatabaseMetaData dbMetaData = conn.getMetaData();
    System.out.println("ResultSet.HOLD_CURSORS_OVER_COMMIT = " +
        ResultSet.HOLD_CURSORS_OVER_COMMIT);
    System.out.println("ResultSet.CLOSE_CURSORS_AT_COMMIT = " +
        ResultSet.CLOSE_CURSORS_AT_COMMIT);
    System.out.println("Default cursor holdability: " +
        dbMetaData.getResultSetHoldability());
    System.out.println("Supports HOLD_CURSORS_OVER_COMMIT? " +
        dbMetaData.supportsResultSetHoldability(
            ResultSet.HOLD_CURSORS_OVER_COMMIT));
    System.out.println("Supports CLOSE_CURSORS_AT_COMMIT? " +
        dbMetaData.supportsResultSetHoldability(
            ResultSet.CLOSE_CURSORS_AT_COMMIT));
  }

  public static void getMyData3(Connection con) throws SQLException {
    Statement stmt = null;
    String query = "SELECT c.nome_cliente, COALESCE(d.soma_depositos, 0) AS soma_depositos, COALESCE(e.soma_emprestimos, 0) AS soma_emprestimos "
        +
        "FROM public.cliente c " +
        "LEFT JOIN ( " +
        "  SELECT ct.nome_cliente, ct.nome_agencia, ct.numero_conta, SUM(d.saldo_deposito) AS soma_depositos " +
        "  FROM public.conta ct " +
        "  LEFT JOIN public.deposito d ON ct.numero_conta = d.numero_conta AND ct.nome_agencia = d.nome_agencia " +
        "  GROUP BY ct.nome_cliente, ct.nome_agencia, ct.numero_conta " +
        ") d ON c.nome_cliente = d.nome_cliente " +
        "LEFT JOIN ( " +
        "  SELECT ct.nome_cliente, ct.nome_agencia, ct.numero_conta, SUM(e.valor_emprestimo) AS soma_emprestimos " +
        "  FROM public.conta ct " +
        "  LEFT JOIN public.emprestimo e ON ct.numero_conta = e.numero_conta AND ct.nome_agencia = e.nome_agencia " +
        "  GROUP BY ct.nome_cliente, ct.nome_agencia, ct.numero_conta " +
        ") e ON c.nome_cliente = e.nome_cliente";

    try {
      stmt = con.createStatement();
      ResultSet rs = stmt.executeQuery(query);
      System.out.println("Dados da consulta: ");
      while (rs.next()) {
        String nomeClienteAlias = rs.getString("nome_cliente");
        double somaDepositosAlias = rs.getDouble("soma_depositos");
        double somaEmprestimosAlias = rs.getDouble("soma_emprestimos");
        System.out.println(nomeClienteAlias + ", " + somaDepositosAlias + ", " + somaEmprestimosAlias);
      }
    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } finally {
      if (stmt != null) {
        stmt.close();
      }
    }
  }

  public static void supportsResultSetConcurrency(Connection conn) throws SQLException {
    DatabaseMetaData dbMetaData = conn.getMetaData();

    int[] resultSetTypes = {
        ResultSet.TYPE_FORWARD_ONLY,
        ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.TYPE_SCROLL_SENSITIVE
    };

    int[] resultSetConcurrency = {
        ResultSet.CONCUR_READ_ONLY,
        ResultSet.CONCUR_UPDATABLE
    };

    for (int resultSetType : resultSetTypes) {
        for (int resultSetConcurrencyType : resultSetConcurrency) {
            boolean supportsConcurrency = dbMetaData.supportsResultSetConcurrency(
                resultSetType,
                resultSetConcurrencyType
            );

            System.out.println("ResultSet Type: " + resultSetType);
            System.out.println("ResultSet Concurrency Type: " + resultSetConcurrencyType);
            System.out.println("Supports Concurrency? " + supportsConcurrency);
            System.out.println();
        }
    }
  }
  
  public static void populatetable(Connection con) throws IOException, SQLException {
    Statement stmt = null;
    String query = "";
    BufferedReader inputStream = null;

    try {
        con.setAutoCommit(false); 

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
                    value[0] + ", " + value[1] + ", '" + value[2] + "', '" + value[3] + "', " + value[4] + ", '"
                    + value[5]
                    + "', '" + value[6] + "')";
            stmt.executeUpdate(query);
        }

        con.commit(); 
    } catch (SQLException e) {
        JDBCUtilities.printSQLException(e);
    } catch (FileNotFoundException e) {
        e.printStackTrace();
    } catch (IOException e) {
        e.printStackTrace();
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

      modifyDeposits(myConnection);
      getMyData3(myConnection);
      supportsResultSetConcurrency(myConnection);
      populatetable(myConnection);

    } catch (SQLException e) {
      JDBCUtilities.printSQLException(e);
    } catch (IOException e) {
        e.printStackTrace();
    } finally {
      JDBCUtilities.closeConnection(myConnection);
    }
  }
}
