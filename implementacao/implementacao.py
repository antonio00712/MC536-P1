import psycopg2
import os

def criar_esquema(conn):
    """
    Cria o esquema do banco de dados lendo apenas as definições de tabela
    do arquivo SQL, parando antes das consultas de análise.
    """
    print("Criando esquema no banco de dados...")
    with conn.cursor() as cur:
        # ATENÇÃO: Verifique se este caminho para o arquivo SQL está correto no seu ambiente.
        caminho_sql = '/path/to/modelo_fisico.sql'
        
        with open(caminho_sql, 'r') as f:
            sql_commands = []
            for linha in f:
                # Para de ler o arquivo quando chegar na seção de consultas
                if linha.strip().startswith('-- Consultar os dados importados'):
                    break
                # Ignora os comandos \copy e linhas em branco
                if not linha.strip().startswith('\\copy') and linha.strip():
                    sql_commands.append(linha)
            
            sql = ''.join(sql_commands)
            cur.execute(sql)
            conn.commit()
            print("✅ Esquema criado com sucesso.")


def popular_banco(conn):
    """Popula as tabelas com dados CSV usando o método copy_expert."""
    print("\nPopulando o banco de dados...")
    # ATENÇÃO: Verifique se este caminho para os dados CSV está correto.
    base_path = '/tmp/data/'
    
    tabelas_arquivos = {
        "regiao_escolar": "regiao_escolar_parsed.csv",
        "escolas": "escolas_parsed.csv",
        "agua": "agua_parsed.csv",
        "energia": "energia_parsed.csv",
        "esgoto": "esgoto_parsed.csv",
        "infraestrutura": "dependencias_parsed.csv",
        "internet": "internet_parsed.csv",
        "funcionarios": "corpo_docente_parsed.csv",
        "rendimento_enem": "rendimento_enem.csv",
        "rendimento_escolar": "serie_parsed.csv",
    }
    
    copy_statements = {
        "regiao_escolar": "COPY regiao_escolar (NO_MUNICIPIO, SG_UF) FROM STDIN WITH CSV HEADER DELIMITER ','",
        "escolas": "COPY escolas (CO_ENTIDADE, NO_ENTIDADE, TP_DEPENDENCIA, TP_LOCALIZACAO, regiao_id) FROM STDIN WITH CSV HEADER DELIMITER ','",
        "agua": "COPY agua (NU_ANO_CENSO, CO_ENTIDADE, IN_AGUA_REDE_PUBLICA, IN_AGUA_POCO_ARTESIANO, IN_AGUA_CACIMBA, IN_AGUA_FONTE_RIO, IN_AGUA_INEXISTENTE) FROM STDIN WITH CSV HEADER DELIMITER ','",
        "energia": "COPY energia(NU_ANO_CENSO,CO_ENTIDADE,IN_ENERGIA_REDE_PUBLICA, IN_ENERGIA_GERADOR_FOSSIL, IN_ENERGIA_RENOVAVEL, IN_ENERGIA_INEXISTENTE) FROM STDIN WITH CSV HEADER DELIMITER ','",
        "esgoto": "COPY esgoto(NU_ANO_CENSO, CO_ENTIDADE, IN_ESGOTO_REDE_PUBLICA, IN_ESGOTO_FOSSA_SEPTICA, IN_ESGOTO_FOSSA_COMUM, IN_ESGOTO_FOSSA, IN_ESGOTO_INEXISTENTE) FROM STDIN WITH CSV HEADER DELIMITER ','",
        "infraestrutura": "COPY infraestrutura(NU_ANO_CENSO, CO_ENTIDADE, IN_AREA_VERDE, IN_BANHEIRO, IN_BIBLIOTECA, IN_LABORATORIO_INFORMATICA) FROM STDIN WITH CSV HEADER DELIMITER ','",
        "internet": "COPY internet(NU_ANO_CENSO, CO_ENTIDADE, IN_INTERNET, IN_INTERNET_ALUNOS, IN_INTERNET_ADMINISTRATIVO, IN_INTERNET_APRENDIZAGEM, TP_REDE_LOCAL) FROM STDIN WITH CSV HEADER DELIMITER ','",
        "funcionarios": "COPY funcionarios(NU_ANO_CENSO, CO_ENTIDADE, QT_PROF_SAUDE, QT_PROF_PSICOLOGO, QT_PROF_ASSIST_SOCIAL) FROM STDIN WITH CSV HEADER DELIMITER ','",
        "rendimento_escolar": "COPY rendimento_escolar(NU_ANO_CENSO, CO_ENTIDADE, FUNDAMENTAL, ENSINO_MEDIO) FROM STDIN WITH CSV HEADER DELIMITER ','",
        "rendimento_enem": "COPY rendimento_enem(NU_ANO, CO_ESCOLA_EDUCACENSO, NU_MATRICULAS, NU_PARTICIPANTES, NU_TAXA_PARTICIPACAO, NU_MEDIA_TOT, PORTE_ESCOLA) FROM STDIN WITH CSV HEADER DELIMITER ','"
    }

    with conn.cursor() as cur:
        for tabela, arquivo in tabelas_arquivos.items():
            try:
                with open(os.path.join(base_path, arquivo), 'r') as f:
                    cur.copy_expert(copy_statements[tabela], f)
                    print(f"✅ Tabela '{tabela}' populada com sucesso.")
            except FileNotFoundError:
                print(f"❌ ERRO: Arquivo não encontrado para a tabela '{tabela}': {os.path.join(base_path, arquivo)}")
                # Decide se quer parar ou continuar se um arquivo não for encontrado
                # raise 
        conn.commit()
        print("✅ Banco de dados populado com sucesso.")


def executar_consultas(conn):
    print("\nExecutando consultas...")
    consultas = [
        {
            "descricao": "1. Consultar escolas com mais de uma fonte de água",
            "sql": """
                SELECT *
                FROM (
                    SELECT 
                        e.no_entidade,
                        (a.in_agua_rede_publica::int +
                         a.in_agua_poco_artesiano::int +
                         a.in_agua_cacimba::int +
                         a.in_agua_fonte_rio::int) AS fontes_agua
                    FROM escolas e
                    JOIN agua a ON e.co_entidade = a.co_entidade
                ) sub
                WHERE fontes_agua > 1
                ORDER BY fontes_agua DESC;
            """
        },
        {
            "descricao": "2. Consultar escolas com internet apenas para administrativo",
            "sql": """
                SELECT 
                    e.no_entidade,
                    r.sg_uf
                FROM internet i
                JOIN escolas e ON e.co_entidade = i.co_entidade
                JOIN regiao_escolar r ON r.id_regiao = e.regiao_id
                WHERE 
                    i.in_internet_administrativo = TRUE AND
                    (COALESCE(i.in_internet_alunos, FALSE) = FALSE AND 
                    COALESCE(i.in_internet_aprendizagem, FALSE) = FALSE);
            """
        },
        {
            "descricao": "3. Melhores rendimentos no enem por escola",
            "sql": """
                SELECT 
                    re.co_escola_educacenso,
                    e.no_entidade,
                    re.nu_ano,
                    re.nu_taxa_participacao,
                    re.nu_media_tot
                FROM rendimento_enem re
                JOIN escolas e ON e.co_entidade = re.co_escola_educacenso
                WHERE re.nu_media_tot IS NOT NULL
                ORDER BY re.nu_media_tot DESC, re.nu_ano;
            """

        },
        {
            "descricao": "4. Escolas com infraestrutura completa (rede pública)",
            "sql": """
                SELECT 
                    e.no_entidade,
                    r.sg_uf
                FROM escolas e
                JOIN regiao_escolar r ON r.id_regiao = e.regiao_id
                JOIN agua a ON a.co_entidade = e.co_entidade
                JOIN energia en ON en.co_entidade = e.co_entidade
                JOIN esgoto es ON es.co_entidade = e.co_entidade
                JOIN internet i ON i.co_entidade = e.co_entidade
                JOIN infraestrutura inf ON inf.co_entidade = e.co_entidade
                WHERE 
                    a.in_agua_rede_publica = TRUE AND
                    en.in_energia_rede_publica = TRUE AND
                    es.in_esgoto_rede_publica = TRUE AND
                    i.in_internet = TRUE AND
                    inf.in_biblioteca = TRUE AND
                    inf.in_banheiro = TRUE AND
                    inf.in_laboratorio_informatica = TRUE;
            """
        },
        {
            "descricao": "5. Relação rendimento e dependencia da escola",
            "sql": """
                SELECT 
                    e.tp_dependencia,
                    COUNT(*) AS qtd_escolas,
                    ROUND(AVG(r.ensino_medio), 2) AS media_ensino_medio,
                    ROUND(AVG(r.fundamental), 2) AS media_ensino_fundamental
                FROM rendimento_escolar r
                JOIN escolas e ON e.co_entidade = r.co_entidade
                WHERE 
                    r.ensino_medio IS NOT NULL OR r.fundamental IS NOT NULL
                GROUP BY e.tp_dependencia
                ORDER BY media_ensino_medio DESC NULLS LAST;
            """
        }
    ]

    with conn.cursor() as cur, open("resultados_consultas.txt", "w", encoding="utf-8") as f:
        for c in consultas:
            try:
                f.write(f"\nConsulta: {c['descricao']}\n")
                f.write("-" * 40 + "\n")
                print(f"\nExecutando Consulta: {c['descricao']}")
                cur.execute(c['sql'])
                
                # Escrever cabeçalhos das colunas
                col_names = [desc[0] for desc in cur.description]
                f.write(f"{col_names}\n")
                
                rows = cur.fetchall()
                if not rows:
                    print(" -> Nenhum resultado encontrado.")
                    f.write("Nenhum resultado encontrado.\n")
                
                for row in rows:
                    f.write(f"{row}\n")
                    print(row)
            except psycopg2.Error as e:
                print(f"❌ ERRO ao executar a consulta '{c['descricao']}': {e}")
                f.write(f"ERRO: {e}\n")
    print("\n✅ Resultados das consultas salvos em 'resultados_consultas.txt'.")

def main():
    try:
        conn = psycopg2.connect(
            dbname="nome_do_banco",
            user="postgres",
            password="**********", # Lembre-se de não deixar senhas fixas em código de produção
            host="localhost",
            port="5432"
        )

        criar_esquema(conn)
        popular_banco(conn)
        executar_consultas(conn)

    except psycopg2.OperationalError as e:
        print(f"❌ ERRO DE CONEXÃO: Não foi possível conectar ao banco de dados.")
        print(f"Detalhes: {e}")
    except Exception as e:
        print(f"❌ Ocorreu um erro inesperado: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("\nConexão com o banco de dados fechada.")

if __name__ == '__main__':
    main()