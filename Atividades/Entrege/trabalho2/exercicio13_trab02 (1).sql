--13.1
CREATE OR REPLACE FUNCTION getDadosCliente(p_nome_cliente character varying)
RETURNS character varying AS
$BODY$
DECLARE
    dados_agencia_str character varying;
    dados_conta_num integer;
    dados_cliente_str character varying;
    conta_str character varying;
    cursor_relatorio CURSOR FOR 
        SELECT nome_cliente, nome_agencia, numero_conta 
        FROM conta 
        WHERE nome_cliente = p_nome_cliente;
BEGIN
    OPEN cursor_relatorio;
    conta_str = '';
    LOOP
        FETCH cursor_relatorio INTO dados_cliente_str, dados_agencia_str, dados_conta_num;
        IF FOUND THEN
            conta_str = conta_str || dados_agencia_str || ' - ' || dados_conta_num || ' , ';
        END IF;
        IF NOT FOUND THEN
            CLOSE cursor_relatorio;
            RETURN conta_str;
        END IF;
    END LOOP;
END
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

ALTER FUNCTION getDadosCliente(character varying)
OWNER TO postgres;

select nome_cliente, getDadosCliente(nome_cliente) from conta;


--13.2
CREATE OR REPLACE FUNCTION getClassificacao(
    p_numero_conta INTEGER,
    p_nome_agencia CHARACTER VARYING,
    p_nome_cliente CHARACTER VARYING
)
RETURNS CHARACTER VARYING AS
$BODY$
DECLARE
    soma_deposito FLOAT;
    classificacao CHARACTER VARYING;
    cursor_relatorio CURSOR FOR 
        SELECT SUM(d.saldo_deposito) AS total_dep
        FROM conta c 
        LEFT JOIN deposito d ON 
            c.numero_conta = d.numero_conta 
            AND c.nome_agencia = d.nome_agencia 
            AND c.nome_cliente = d.nome_cliente 
        WHERE c.nome_cliente = p_nome_cliente 
        AND c.nome_agencia = p_nome_agencia 
        AND c.numero_conta = p_numero_conta
        GROUP BY c.nome_cliente, c.nome_agencia, c.numero_conta;
BEGIN
    OPEN cursor_relatorio;
    FETCH cursor_relatorio INTO soma_deposito;
    IF FOUND THEN
        IF soma_deposito IS NULL THEN 
            soma_deposito = 0;
        END IF;
        IF soma_deposito > 6000 THEN 
            classificacao = 'A';
        ELSIF soma_deposito BETWEEN 4000 AND 6000 THEN 
            classificacao = 'B'; 
        ELSE 
            classificacao = 'C';
        END IF;
    END IF;
    CLOSE cursor_relatorio;
    RETURN classificacao;
END
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;

ALTER FUNCTION getClassificacao(
    INTEGER, 
    CHARACTER VARYING, 
    CHARACTER VARYING
) OWNER TO postgres;

select numero_conta , nome_agencia , nome_cliente , getClassificacao (
numero_conta , nome_agencia , nome_cliente ) from conta c