# Projeto MC536
> Projeto 1 da disciplina de Banco de Dados MC536 da Unicamp

## 📕 Modelo Conceitual

<img src="modelo_conceitual_p1.jpg" alt="Modelo Conceitual">

## 💻 Tutorial para uso 

1. Crie uma pasta vazia no diretório /tmp chamada data/;
2. É nessa pasta que os arquivos derivados do tratamento de dados vão parar;
3. O arquivo `implementacao.py` modifica um banco de dados chamado implementacao_python, recomendo criar mas pode colocar em qualquer um, é so mudar o nome do banco de dados no arquivo `implementacao.py` e sua senha;
``` python
conn = psycopg2.connect(
dbname="nome",
user="postgres",
password="********",
host="localhost",
port="5432")
```
4. Na linha 7 do código de `implementacao.py` é necessário ajustar o caminho para o arquivo `modelo_fisico.sql`, para o caminho até essa pasta em seu computador. Não esqueça de salvar o arquivo após as alterações;
5. O arquivo vai então importar, descartar e recriar todas as tabelas do banco de dados e depois vai partir para a população de dados;
6. Tenha certeza de que todos os arquivos de `data.zip` do tipo **parsed.csv**  estão descompactados e localizados em /tmp;
7. O código vai fazer cada importação separadamente;
8. Depois ele realiza as consultas.

> Obs.: na tabela regiao_escolar o id é id_regiao, nas demais tabelas essa key é referenciada como regiao_id, cuidado para não confundir.

## 🤝 Grupo
- Pedro Henrique dos Reis Arcoverde `RA: 254719`
- Leonardo da Silva Giovanelli de Santana `RA: 256472`
- Antonio Carlos Carvalho Macedo `RA: 199152`
