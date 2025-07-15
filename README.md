# Data Pipeline: Cotação do Bitcoin com PostgreSQL, Python, dbt e Airflow

Este projeto implementa um pipeline de dados completo para ingestão, tratamento e orquestração de dados da cotação diária do Bitcoin. Utiliza as melhores práticas de engenharia de dados, como containerização, ingestão programática com Python, modelagem em camadas com o **esquema Medallion (bronze → silver → gold)** via `dbt`, e automação com **Apache Airflow**.

---

## Visão Geral do Pipeline

1. **Banco de Dados PostgreSQL** com Docker Compose  e **Ingestão de dados via API** com Python (`requests` + `pandas`)  
2. **Modelagem dos dados com dbt** e o esquema Medallion  
3. **Orquestração completa com Apache Airflow**

---

## Estrutura do Projeto

## Etapa 1 - Banco de Dados com Docker Compose + Ingestão dos dados

O banco de dados PostgreSQL é definido no `docker-compose.yml`. Para subir o ambiente:

```bash
docker-compose up -d
```

O script Python realiza:

Requisição para uma API pública de cotação do Bitcoin, salvando os dados em arquivo CSV no diretório dbt_project/seeds/

### Extração dos campos:

|datetime    | open     | high     | low      | close    |
|:---:       |:---:     |:---:     |:---:     |:---:     |
|2025-07-14  |119086.65 |123218    |118905.18 |120610.02 |
|2025-07-13  |117420    |119488    |117224.79 |119086.64 |
|2025-07-12  |117527.66 |118200    |116900.05 |117420    |
|2025-07-11  |116010.01 |118869.98 |115222.22 |117527.66 |
|2025-07-10  |111234    |	116868 |110500    |116010    |



## Etapa 2 - Transformação com dbt

Com os dados no diretório seeds, o comando dbt build irá:

1. Carregar os dados do CSV para o banco (seed)
2. Criar modelos de transformação com o esquema:

    - bronze → dados brutos
    - silver → dados limpos e validados
    - gold → métricas agregadas e preparadas para análise

## Etapa 3 - Orquestração com Apache Airflow
A DAG no Airflow realiza a orquestração automática do pipeline:

- start_pipeline: início da DAG
- fetch_data: executa o script Python para coletar e salvar os dados
- dbt_seed: insere os dados no PostgreSQL via dbt seed
- dbt_build: executa os modelos dbt em sequência (bronze → silver → gold)