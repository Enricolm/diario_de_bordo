# 🧭 Diário de Bordo - Pipeline de Dados com PySpark e Delta Lake

Este projeto implementa um pipeline de processamento de dados utilizando **Apache Spark**, com foco em dados de deslocamento (categoria, distância, propósito, etc.), permitindo o agrupamento, validação e tratamento de dados inválidos em uma arquitetura moderna com **Delta Lake**.

## 🔄 Fluxo de Processamento

1. **Leitura da Camada Bronze**
   - Os dados brutos são carregados a partir de arquivos CSV com formatação de datas personalizada.

2. **Validação dos Dados**
   - As linhas com colunas nulas (ex: `CATEGORIA`, `PROPOSITO`, `DISTANCIA`, `DATA_INICIO`) são filtradas.
   - Dados inválidos são armazenados em uma **tabela de quarentena** para análise posterior.

3. **Transformações**
   - As datas são convertidas para o padrão `yyyy-MM-dd`.
   - Agregações como média, máximo, mínimo e contagens por categoria e propósito são aplicadas.

4. **Gravação na Camada Silver**
   - Os dados transformados e agregados são salvos em formato Delta, particionados por data (`dt_refe`).

## 📁 Estrutura de Pastas

``` bash
diario_de_bordo/
├── app/
│ ├── DiarioDeBordo/
│ │ ├── DiarioDeBordoExecute.py
│ │ ├── DiarioDeBordoTransform.py
│ │ ├── DiarioDeBordoEnum.py
│ │ ├── DiarioDeBordoCreateTable.py
│ │ └── LoggerService.py
├── data/
│ └── bronze/
└── README.md
```

## 🛠️ Tecnologias Utilizadas

- Apache Spark
- Delta Lake
- PySpark
- Databricks
- Python 3.10

## 🚨 Tratamento de Erros

- Logs são gerenciados com o módulo `logging`, com exibição no console.
- Estrutura pronta para integração com tabelas de erro se necessário.

## 📌 Objetivo

Garantir o controle e a rastreabilidade de dados de transporte, permitindo análises confiáveis mesmo em ambientes com dados incompletos ou inconsistentes.

## ✍️ Autor

**Enrico Lopes Malachini**  
🔗 [GitHub](https://github.com/Enricolm)  
🔗 [LinkedIn](https://www.linkedin.com/in/enrico-malachini/)
