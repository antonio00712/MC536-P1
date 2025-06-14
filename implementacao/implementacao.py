import psycopg2
import os

def criar_esquema(conn):
    print("Criando esquema no banco de dados...")
    with conn.cursor() as cur:
        with open('path_to/import.sql', 'r') as f:
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
        "dependencias": "/tmp/data/dependencias_parsed.csv",
        "internet": "/tmp/data/internet_parsed.csv",
        "corpo_docente": "/tmp/data/corpo_docente_parsed.csv",
        "rendimento_enem": "/tmp/data/rendimento_enem.csv",
        "rendimento": "/tmp/data/serie_parsed.csv",
    }

    with conn.cursor() as cur:
        # Inserir anos de 2005 a 2023 com proteção contra duplicatas
        print("🗓️ Inserindo anos diretamente na tabela 'ano'...")
        for ano in range(2005, 2024):
            cur.execute(
                "INSERT INTO ano (NU_ANO_CENSO) VALUES (%s) ON CONFLICT DO NOTHING;",
                (ano,)
            )
        print("✅ Anos inseridos com sucesso.")
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
            "COPY dependencias(NU_ANO_CENSO, CO_ENTIDADE, IN_AREA_VERDE, IN_BANHEIRO, IN_BIBLIOTECA, IN_LABORATORIO_INFORMATICA) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["dependencias"], 'r')
        )
        print("✅ Tabela 'dependencias' populada com sucesso.")
        cur.copy_expert(
            "COPY internet(NU_ANO_CENSO, CO_ENTIDADE, IN_INTERNET, IN_INTERNET_ALUNOS, IN_INTERNET_ADMINISTRATIVO, IN_INTERNET_APRENDIZAGEM, TP_REDE_LOCAL) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["internet"], 'r')
        )
        print("✅ Tabela 'internet' populada com sucesso.")
        cur.copy_expert(
            "COPY corpo_docente(NU_ANO_CENSO, CO_ENTIDADE, QT_PROF_SAUDE, QT_PROF_PSICOLOGO, QT_PROF_ASSIST_SOCIAL) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["corpo_docente"], 'r')
        )
        print("✅ Tabela 'corpo_docente' populada com sucesso.")
        cur.copy_expert(
            "COPY rendimento(NU_ANO_CENSO, CO_ENTIDADE, FUNDAMENTAL, ENSINO_MEDIO) FROM STDIN WITH CSV HEADER DELIMITER ','",
            open(tabelas_arquivos["rendimento"], 'r')
        )
        print("✅ Tabela 'rendimento' populada com sucesso.")
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
                HAVING 
                    COUNT(*) FILTER (WHERE a.in_agua_rede_publica) +
                    COUNT(*) FILTER (WHERE a.in_agua_poco_artesiano) +
                    COUNT(*) FILTER (WHERE a.in_agua_cacimba) +
                    COUNT(*) FILTER (WHERE a.in_agua_fonte_rio) > 1
                ORDER BY fontes_agua DESC
                LIMIT 50;
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
                (i.in_internet_alunos = FALSE OR i.in_internet_alunos IS NULL)
            LIMIT 50;
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
            ORDER BY re.nu_media_tot DESC, re.nu_ano
            LIMIT 20;
            """

        },
        {
            "descricao": " 4. escolas com todas as dependências presentes",
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
            JOIN dependencias d ON d.co_entidade = e.co_entidade
            WHERE 
            a.in_agua_rede_publica = TRUE AND
            en.in_energia_rede_publica = TRUE AND
            es.in_esgoto_rede_publica = TRUE AND
            i.in_internet = TRUE AND
            d.in_biblioteca = TRUE AND
            d.in_banheiro = TRUE AND
            d.in_laboratorio_informatica = TRUE
            LIMIT 50;
            """
        },
        {
            "descricao": "5. Relação rendimento e dependencia e localização da escola",
            "sql": """
            SELECT 
                e.tp_dependencia,
            COUNT(*) AS qtd_escolas,
            ROUND(AVG(r.ensino_medio), 2) AS media_ensino_medio
            FROM rendimento r
            JOIN escolas e ON e.co_entidade = r.co_entidade
            WHERE r.ensino_medio IS NOT NULL
            GROUP BY e.tp_dependencia
            ORDER BY media_ensino_medio DESC
            LIMIT 50;
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
        dbname="nome",
        user="postgres",
        password="*********",
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
