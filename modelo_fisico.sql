--fazendo um código para importar os dados do arquivo CSV para o banco de dados
-- código para ser rodado no psql
-- 1. Conectar ao banco de dados
--\c banco_de_dados_Escolas;
-- 2. Criar a tabela para armazenar os dados

DROP TABLE IF EXISTS ano CASCADE;
CREATE TABLE IF NOT EXISTS ano (
    NU_ANO_CENSO INTEGER PRIMARY KEY
);

DROP TABLE IF EXISTS regiao_escolar CASCADE;
CREATE TABLE IF NOT EXISTS regiao_escolar (
    id_regiao SERIAL PRIMARY KEY,
    NO_MUNICIPIO VARCHAR(255),
    SG_UF VARCHAR(2)
);

DROP TABLE IF EXISTS escolas CASCADE;
CREATE TABLE escolas (
    co_entidade INTEGER PRIMARY KEY,
    no_entidade VARCHAR(255) NOT NULL,
    tp_dependencia VARCHAR(20),     -- Ex: "Federal", "Estadual", etc.
    tp_localizacao VARCHAR(20),     -- Ex: "Urbana", "Rural"
    regiao_id INTEGER NOT NULL REFERENCES regiao_escolar(id_regiao)
);

DROP TABLE IF EXISTS agua CASCADE;
CREATE TABLE IF NOT EXISTS agua (
    agua_id SERIAL PRIMARY KEY,
    NU_ANO_CENSO INTEGER,
    CO_ENTIDADE INTEGER,
    FOREIGN KEY(CO_ENTIDADE) REFERENCES escolas(CO_ENTIDADE),
    FOREIGN KEY(NU_ANO_CENSO) REFERENCES ano(NU_ANO_CENSO),
    IN_AGUA_REDE_PUBLICA BOOLEAN,
    IN_AGUA_POCO_ARTESIANO BOOLEAN,
    IN_AGUA_CACIMBA BOOLEAN,
    IN_AGUA_FONTE_RIO BOOLEAN,
    IN_AGUA_INEXISTENTE BOOLEAN
);

DROP TABLE IF EXISTS energia CASCADE;
CREATE TABLE IF NOT EXISTS energia (
    energia_id SERIAL PRIMARY KEY,
    NU_ANO_CENSO INTEGER,
    CO_ENTIDADE INTEGER,
    FOREIGN KEY(NU_ANO_CENSO) REFERENCES ano(NU_ANO_CENSO),
    FOREIGN KEY(CO_ENTIDADE) REFERENCES escolas(CO_ENTIDADE),
    IN_ENERGIA_REDE_PUBLICA BOOLEAN,
    IN_ENERGIA_GERADOR_FOSSIL BOOLEAN,
    IN_ENERGIA_RENOVAVEL BOOLEAN,
    IN_ENERGIA_INEXISTENTE BOOLEAN
);

DROP TABLE IF EXISTS esgoto CASCADE;
CREATE TABLE IF NOT EXISTS esgoto (
    esgoto_id SERIAL PRIMARY KEY,
    NU_ANO_CENSO INTEGER,
    CO_ENTIDADE INTEGER,
    FOREIGN KEY(NU_ANO_CENSO) REFERENCES ano(NU_ANO_CENSO),
    FOREIGN KEY(CO_ENTIDADE) REFERENCES escolas(CO_ENTIDADE),
    IN_ESGOTO_REDE_PUBLICA BOOLEAN,
    IN_ESGOTO_FOSSA_SEPTICA BOOLEAN,
    IN_ESGOTO_FOSSA_COMUM BOOLEAN,
    IN_ESGOTO_FOSSA BOOLEAN,
    IN_ESGOTO_INEXISTENTE BOOLEAN
);

DROP TABLE IF EXISTS dependencias CASCADE;
CREATE TABLE IF NOT EXISTS dependencias (
    dependencias_id SERIAL PRIMARY KEY,
    NU_ANO_CENSO INTEGER,
    CO_ENTIDADE INTEGER,
    FOREIGN KEY(NU_ANO_CENSO) REFERENCES ano(NU_ANO_CENSO),
    FOREIGN KEY(CO_ENTIDADE) REFERENCES escolas(CO_ENTIDADE),
    IN_AREA_VERDE BOOLEAN,
    IN_BANHEIRO BOOLEAN,
    IN_BIBLIOTECA BOOLEAN,
    IN_LABORATORIO_INFORMATICA BOOLEAN
);

DROP TABLE IF EXISTS internet CASCADE;
CREATE TABLE IF NOT EXISTS internet (
    id SERIAL PRIMARY KEY,
    NU_ANO_CENSO INTEGER,
    CO_ENTIDADE INTEGER,
    FOREIGN KEY(NU_ANO_CENSO) REFERENCES ano(NU_ANO_CENSO),
    FOREIGN KEY(CO_ENTIDADE) REFERENCES escolas(CO_ENTIDADE),
    IN_INTERNET BOOLEAN,
    IN_INTERNET_ALUNOS BOOLEAN,
    IN_INTERNET_ADMINISTRATIVO BOOLEAN,
    IN_INTERNET_APRENDIZAGEM BOOLEAN,
    TP_REDE_LOCAL NUMERIC(2,1)
);

DROP TABLE IF EXISTS corpo_docente CASCADE;
CREATE TABLE IF NOT EXISTS corpo_docente (
    id SERIAL PRIMARY KEY,
    NU_ANO_CENSO INTEGER,
    CO_ENTIDADE INTEGER,
    FOREIGN KEY(NU_ANO_CENSO) REFERENCES ano(NU_ANO_CENSO),
    FOREIGN KEY(CO_ENTIDADE) REFERENCES escolas(CO_ENTIDADE),
    QT_PROF_SAUDE NUMERIC(7,1),
    QT_PROF_PSICOLOGO NUMERIC(7,1),
    QT_PROF_ASSIST_SOCIAL NUMERIC(7,1)
);

DROP TABLE IF EXISTS rendimento CASCADE;
CREATE TABLE IF NOT EXISTS rendimento (
    id_rendimento SERIAL PRIMARY KEY,
    NU_ANO_CENSO INTEGER,
    CO_ENTIDADE INTEGER,
    FOREIGN KEY(NU_ANO_CENSO) REFERENCES ano(NU_ANO_CENSO),
    FOREIGN KEY(CO_ENTIDADE) REFERENCES escolas(CO_ENTIDADE),
    FUNDAMENTAL NUMERIC(4,1),
    ENSINO_MEDIO NUMERIC(4,1)
);

DROP TABLE IF EXISTS rendimento_enem CASCADE;
CREATE TABLE IF NOT EXISTS rendimento_enem (
    rendimento_enem_id SERIAL PRIMARY KEY,
    NU_ANO INTEGER,
    CO_ESCOLA_EDUCACENSO INTEGER,
    FOREIGN KEY(CO_ESCOLA_EDUCACENSO) REFERENCES escolas(CO_ENTIDADE),
    NU_MATRICULAS INTEGER,
    NU_PARTICIPANTES INTEGER,
    NU_TAXA_PARTICIPACAO NUMERIC(5,2),
    NU_MEDIA_TOT NUMERIC(5,2),
    PORTE_ESCOLA VARCHAR(255)
);

-- 3. Importar os dados do arquivo CSV para a tabela
-- usando comando \copy para importar os dados do arquivo CSV
-- nome regiao = NO_REGIAO
-- sigla = SG_UF
INSERT INTO ano(NU_ANO_CENSO) VALUES (2023);
\copy regiao_escolar(NO_MUNICIPIO, SG_UF) from '/tmp/data/regiao_escolar_parsed.csv' DELIMITER ',' CSV HEADER;
\copy escolas(CO_ENTIDADE, NO_ENTIDADE, TP_DEPENDENCIA, TP_LOCALIZACAO, regiao_id) from '/tmp/data/escolas_parsed.csv' DELIMITER ',' CSV HEADER;
\copy agua(NU_ANO_CENSO, CO_ENTIDADE,IN_AGUA_REDE_PUBLICA, IN_AGUA_POCO_ARTESIANO, IN_AGUA_CACIMBA, IN_AGUA_FONTE_RIO, IN_AGUA_INEXISTENTE) from '/tmp/data/agua_parsed.csv' DELIMITER ',' CSV HEADER;
\copy energia(NU_ANO_CENSO,CO_ENTIDADE,IN_ENERGIA_REDE_PUBLICA, IN_ENERGIA_GERADOR_FOSSIL, IN_ENERGIA_RENOVAVEL, IN_ENERGIA_INEXISTENTE) from '/tmp/data/energia_parsed.csv' DELIMITER ',' CSV HEADER;
\copy esgoto(NU_ANO_CENSO, CO_ENTIDADE, IN_ESGOTO_REDE_PUBLICA, IN_ESGOTO_FOSSA_SEPTICA, IN_ESGOTO_FOSSA_COMUM, IN_ESGOTO_FOSSA, IN_ESGOTO_INEXISTENTE) from '/tmp/data/esgoto_parsed.csv' DELIMITER ',' CSV HEADER;
\copy dependencias(NU_ANO_CENSO, CO_ENTIDADE, IN_AREA_VERDE, IN_BANHEIRO, IN_BIBLIOTECA, IN_LABORATORIO_INFORMATICA) from '/tmp/data/dependencias_parsed.csv' DELIMITER ',' CSV HEADER;
\copy internet(NU_ANO_CENSO, CO_ENTIDADE, IN_INTERNET, IN_INTERNET_ALUNOS, IN_INTERNET_ADMINISTRATIVO, IN_INTERNET_APRENDIZAGEM, TP_REDE_LOCAL) from '/tmp/data/internet_parsed.csv' DELIMITER ',' CSV HEADER;
\copy corpo_docente(NU_ANO_CENSO, CO_ENTIDADE, QT_PROF_SAUDE, QT_PROF_PSICOLOGO, QT_PROF_ASSIST_SOCIAL) from '/tmp/data/corpo_docente_parsed.csv' DELIMITER ',' CSV HEADER;
\copy rendimento(NU_ANO_CENSO, CO_ENTIDADE, FUNDAMENTAL, ENSINO_MEDIO) from '/tmp/data/serie_parsed.csv' DELIMITER ',' CSV HEADER;
\copy rendimento_enem(NU_ANO, CO_ESCOLA_EDUCACENSO, NU_MATRICULAS, NU_PARTICIPANTES, NU_TAXA_PARTICIPACAO, NU_MEDIA_TOT, PORTE_ESCOLA) from '/tmp/data/rendimento_enem.csv' DELIMITER ',' CSV HEADER;


-- Consultar os dados importados

-- 1. Consultar escolas com mais de uma fonte de água
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
ORDER BY fontes_agua DESC;

-- 2. Consultar escolas com internet apenas para administrativo    
SELECT 
  e.no_entidade,
  r.sg_uf
FROM internet i
JOIN escolas e ON e.co_entidade = i.co_entidade
JOIN regiao_escolar r ON r.id_regiao = e.regiao_id
WHERE 
  i.in_internet_administrativo = TRUE AND
  (i.in_internet_alunos = FALSE OR i.in_internet_alunos IS NULL);

-- 3. melhores rendimentos no enem por escola
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

-- 4. escolas com todas as dependências presentes
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
  d.in_laboratorio_informatica = TRUE;

-- 5. Relação rendimento e dependencia e localização da escola
SELECT 
  e.tp_dependencia,
  COUNT(*) AS qtd_escolas,
  ROUND(AVG(r.ensino_medio), 2) AS media_ensino_medio
FROM rendimento r
JOIN escolas e ON e.co_entidade = r.co_entidade
WHERE r.ensino_medio IS NOT NULL
GROUP BY e.tp_dependencia
ORDER BY media_ensino_medio DESC;
