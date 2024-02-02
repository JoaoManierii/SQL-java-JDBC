--AULA1

update conta set saldo_conta=0;
--PRIMEIRO CONFERIMOS COMO ESTá A SITUAçãO DAS CONTAS
select * from conta;

--EXECUTAMOS OUTRA CONFERENCIA PARA VER COMO SERãO OS RETORNOS DA PESQUISA
select getliquido(numero_conta, nome_agencia, nome_cliente) from conta

--DEPOIS ATUALIZAMOS ...
update conta set saldo_conta=getliquido(numero_conta, nome_agencia, nome_cliente);

--... E CONFERINDO DE NOVO:
select * from conta where nome_agencia = 'PUC';


--AULA2


--PRIMEIRO CONFERIMOS COMO ESTá A SITUAçãO DAS CONTAS
select * from emprestimo where nome_agencia = 'PUC';

--EXECUTAMOS OUTRA CONFERENCIA PARA VER COMO SERãO OS RETORNOS DA PESQUISA
select conta.*, update_valor_emprestimo1(numero_conta, nome_agencia, nome_cliente) from conta where nome_agencia = 'PUC';

--Lista as quantidades de emprestimos realizados por cada cliente
select nome_cliente, count(1) from emprestimo group by nome_cliente order by count(1) desc

--Lista apenas um deles
select * from emprestimo where nome_cliente = 'Reinaldo Pereira da Silva';

--Agora atualiza apenas os valores de empréstimos desta pessoa
select update_valor_emprestimo2('Reinaldo Pereira da Silva');

--AULA 3

update agencia set ativo_agencia=0;
--PRIMEIRO CONFERIMOS COMO ESTá A SITUAçãO DAS AGENCIAS
select * from agencia;

--EXECUTAMOS OUTRA CONFERENCIA PARA VER COMO SERãO OS RETORNOS DA PESQUISA
select getagencia(nome_agencia) from agencia;

--DEPOIS ATUALIZAMOS ...
update agencia set ativo_agencia=getagencia(nome_agencia);

--... E CONFERINDO DE NOVO:
select * from agencia;

--AULA 4

--INICIALMENTE OS ATIVOS DA AGENCIA ESTÃO COM VALOR ZERO
UPDATE AGENCIA SET ATIVO_AGENCIA = 0;

SELECT * FROM AGENCIA

SELECT * FROM CONTA

--AO ATUALIZARMOS OS SALDOS DAS CONTAS, DISPARAMOS A ATUALIZAÇÃO DOS ATIVOS DAS AGÊNCIAS
UPDATE CONTA SET SALDO_CONTA=GETLIQUIDO(NUMERO_CONTA, NOME_AGENCIA, NOME_CLIENTE);

--INICIALMENTE OS ATIVOS DA AGENCIA ESTÃO COM VALOR ZERO
UPDATE AGENCIA SET ATIVO_AGENCIA = 0;

SELECT * FROM AGENCIA

SELECT * FROM CONTA

--AO ATUALIZARMOS OS SALDOS DAS CONTAS, DISPARAMOS A ATUALIZAÇÃO DOS ATIVOS DAS AGÊNCIAS
UPDATE CONTA SET SALDO_CONTA=GETLIQUIDO(NUMERO_CONTA, NOME_AGENCIA, NOME_CLIENTE);

