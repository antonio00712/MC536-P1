import psycopg2
import os

def criar_esquema(conn):
    print("Criando esquema no banco de dados...")
    with conn.cursor() as cur:
        with open('/home/parcv/Documentos/comput/faculdade/mc536/banco_de_dados_Escolas/data_sets/code/import.sql', 'r') as f:
            linhas = f.readlines()

        # Remove linhas com \copy (não são SQL válidas para psycopg2)
        comandos_validos = [linha for linha in linhas if not linha.strip().startswith('\\copy')]
        sql = ''.join(comandos_validos)

        cur.execute(sql)
        conn.commit()
        print("✅ Esquema criado com sucesso (comandos \\copy ignorados).")

def popular_banco(conn):
    """Popula as tabelas com dados CSV e insere anos diretamente na tabela 'ano'."""
    tabelas_arquivos = {
        "regiao_escolar": "/tmp/data/regiao_escolar_parsed.csv",
        "escolas": "/tmp/data/escolas_parsed.csv",
        "agua": "/tmp/data/agua_parsed.csv",
        "energia": "/tmp/data/energia_parsed.csv",
        "esgoto": "/tmp/data/esgoto_parsed.csv",
        "infraestrutura": "/tmp/data/dependencias_parsed.csv",
        "internet": "/tmp/data/internet_parsed.csv",
        "funcionarios": "/tmp/data/corpo_docente_parsed.csv",
        "rendimento_enem": "/tmp/data/rendimento_enem.csv",
        "rendimento_escolar": "/tmp/data/serie_parsed.csv",
    }

    with conn.cursor() as cur:
        # Inserir dados de cada tabela diretamente do CSV
        cur.copy_expert(
            "COPY regiao_escolar (NO_MUNICIPIO, SG_UF) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["regiao_escolar"], 'r')
        )
        print("✅ Tabela 'regiao_escolar' populada com sucesso.")
        cur.copy_expert(
            "COPY escolas (CO_ENTIDADE, NO_ENTIDADE, TP_DEPENDENCIA, TP_LOCALIZACAO, regiao_id) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["escolas"], 'r')
        )
        print("✅ Tabela 'escolas' populada com sucesso.")
        cur.copy_expert(
            "COPY agua (NU_ANO_CENSO, CO_ENTIDADE,IN_AGUA_REDE_PUBLICA, IN_AGUA_POCO_ARTESIANO, IN_AGUA_CACIMBA, IN_AGUA_FONTE_RIO, IN_AGUA_INEXISTENTE) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["agua"], 'r')
        )
        print("✅ Tabela 'agua' populada com sucesso.")
        cur.copy_expert(
            "COPY energia(NU_ANO_CENSO,CO_ENTIDADE,IN_ENERGIA_REDE_PUBLICA, IN_ENERGIA_GERADOR_FOSSIL, IN_ENERGIA_RENOVAVEL, IN_ENERGIA_INEXISTENTE) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["energia"], 'r')
        )
        print("✅ Tabela 'energia' populada com sucesso.")
        cur.copy_expert(
            "COPY esgoto(NU_ANO_CENSO, CO_ENTIDADE, IN_ESGOTO_REDE_PUBLICA, IN_ESGOTO_FOSSA_SEPTICA, IN_ESGOTO_FOSSA_COMUM, IN_ESGOTO_FOSSA, IN_ESGOTO_INEXISTENTE) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["esgoto"], 'r')
        )
        print("✅ Tabela 'esgoto' populada com sucesso.")
        cur.copy_expert(
            "COPY infraestrutura(NU_ANO_CENSO, CO_ENTIDADE, IN_AREA_VERDE, IN_BANHEIRO, IN_BIBLIOTECA, IN_LABORATORIO_INFORMATICA) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["infraestrutura"], 'r')
        )
        print("✅ Tabela 'infraestrutura' populada com sucesso.")
        cur.copy_expert(
            "COPY internet(NU_ANO_CENSO, CO_ENTIDADE, IN_INTERNET, IN_INTERNET_ALUNOS, IN_INTERNET_ADMINISTRATIVO, IN_INTERNET_APRENDIZAGEM, TP_REDE_LOCAL) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["internet"], 'r')
        )
        print("✅ Tabela 'internet' populada com sucesso.")
        cur.copy_expert(
            "COPY funcionarios(NU_ANO_CENSO, CO_ENTIDADE, QT_PROF_SAUDE, QT_PROF_PSICOLOGO, QT_PROF_ASSIST_SOCIAL) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["funcionarios"], 'r')
        )
        print("✅ Tabela 'funcionarios' populada com sucesso.")
        cur.copy_expert(
            "COPY rendimento_escolar(NU_ANO_CENSO, CO_ENTIDADE, FUNDAMENTAL, ENSINO_MEDIO) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["rendimento_escolar"], 'r')
        )
        print("✅ Tabela 'rendimento_escolar' populada com sucesso.")
        cur.copy_expert(
            "COPY rendimento_enem(NU_ANO, CO_ESCOLA_EDUCACENSO, NU_MATRICULAS, NU_PARTICIPANTES, NU_TAXA_PARTICIPACAO, NU_MEDIA_TOT, PORTE_ESCOLA) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["rendimento_enem"], 'r')
        )
        print("✅ Tabela 'rendimento_enem' populada com sucesso.")
        conn.commit()
        print("✅ Banco de dados populado com sucesso.")

def executar_consultas(conn):
    print("Executando consultas...")
    consultas = [
        {
            "descricao": "1. Consultar escolas com mais de uma fonte de água",
            "sql": """
                SELECT *
                FROM (
                SELECT 
                    e.no_entidade,
                    COUNT(*) FILTER (WHERE a.in_agua_rede_publica) +
                    COUNT(*) FILTER (WHERE a.in_agua_poco_artesiano) +
                    COUNT(*) FILTER (WHERE a.in_agua_cacimba) +
                    COUNT(*) FILTER (WHERE a.in_agua_fonte_rio) AS fontes_agua
                FROM escolas e
                JOIN agua a ON e.co_entidade = a.co_entidade
                WHERE 
                    a.in_agua_rede_publica OR 
                    a.in_agua_poco_artesiano OR 
                    a.in_agua_cacimba OR 
                    a.in_agua_fonte_rio
                GROUP BY e.no_entidade
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
                    COALESCE(i.internet_aprendizagem, FALSE) = FALSE);
            """
        },
        {
            "descricao": "3. melhores rendimentos no enem por escola",
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
            "descricao": " 4. Escolas com agua, energia, internet, biblioteca, banheiro e laboratorio de informatica presente ao mesmo tempo",
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
                inf.in_laboratorio_informatica = TRUE
            """
        },
        {
            "descricao": "5. Relação rendimento e dependencia e localização da escola",
            "sql": """
                SELECT 
                e.tp_dependencia,
                COUNT(*) AS qtd_escolas,
                ROUND(AVG(r.ensino_medio), 2) AS media_ensino_medio,
                ROUND(AVG(r.ensino_fundamental), 2) AS media_ensino_fundamental
                FROM rendimento r
                JOIN escolas e ON e.co_entidade = r.co_entidade
                WHERE 
                r.ensino_medio IS NOT NULL OR r.ensino_fundamental IS NOT NULL
                GROUP BY e.tp_dependencia
                ORDER BY media_ensino_medio DESC NULLS LAST;
            """
        }
    ]

    with conn.cursor() as cur, open("resultados_consultas.txt", "w") as f:
        for c in consultas:
            f.write(f"\nConsulta: {c['descricao']}\n")
            print(f"\nConsulta: {c['descricao']}")
            cur.execute(c['sql'])
            rows = cur.fetchall()
            for row in rows:
                f.write(f"{row}\n")
                print(row)
    print("✅ Resultados das consultas salvos em 'resultados_consultas.txt'.")

def main():
    conn = psycopg2.connect(
        dbname="projeto_mc536",
        user="postgres",
        password="unicamp-2324",
        host="localhost",
        port="5432"
    )

    try:
        criar_esquema(conn)
        popular_banco(conn)
        executar_consultas(conn)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
