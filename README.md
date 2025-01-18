Personal Anime Data Project
Este repositório contém um projeto analítico baseado nos dados do perfil pessoal Gajs no site AniList.co. Ele utiliza uma arquitetura de dados analítica avançada para ingestão, processamento e visualização de dados de animes assistidos.

Arquitetura Analítica
O projeto implementa uma arquitetura de dados Medallion com as seguintes camadas:

1. Ingestão
Workflow: A ingestão é gerenciada por um workflow do Databricks.
Fonte dos Dados: Os dados são coletados diretamente da API do site AniList.co.
Objetivo: Captura dados brutos do perfil pessoal Gajs e os armazena em uma área de staging no Google Cloud Storage (GCS).
2. Staging Area
Tecnologia: Google Cloud Storage (GCS).
Uso: Armazena os arquivos brutos versionados antes de serem processados e ingeridos no BigQuery.
Controle: Garante rastreabilidade total com versionamento de objetos ativado.
3. Dados no BigQuery
Armazenamento: O pipeline move os dados do GCS para o Google BigQuery, onde eles são organizados em diferentes camadas conforme o modelo Medallion:
RAW: Dados históricos completos, atuando como staging area do pipeline.
TRU (Trusted): Dados confiáveis, armazenados por até 5 anos, com controle incremental (delta).
REF (Refined): Dados refinados e tratados, com retenção de até 3 anos.
DMT (Data Mart): Contém os indicadores e métricas finais, otimizados para dashboards, com retenção de até 2 anos.
Detalhes da Arquitetura de Dados Medallion
RAW:
Descrição:
Dados brutos e históricos completos.
Atua como a camada inicial do pipeline de dados.
Retenção: Sem limite de retenção.
Objetivo: Garante rastreabilidade e permite reconstrução do pipeline em caso de necessidade.
TRU (Trusted):
Descrição:
Dados limpos e confiáveis.
Aplicação de regras de negócios e garantia de consistência.
Retenção: Até 5 anos.
Objetivo: Fornecer uma base confiável para análises mais detalhadas.
REF (Refined):
Descrição:
Dados refinados e tratados para consumo específico.
Estruturados para facilitar análises exploratórias e relatórios.
Retenção: Até 3 anos.
Objetivo: Fornecer dados prontos para modelagem de indicadores.
DMT (Data Mart):
Descrição:
Contém métricas e indicadores finais para visualização em dashboards.
Dados otimizados para consumo direto em ferramentas como Tableau e Looker Studio.
Retenção: Até 2 anos.
Objetivo: Melhorar o desempenho e agilidade na geração de insights.
Fluxo de Dados
plaintext
Copiar
Editar
          AniList.co API
                 |
             Ingestão
       (Databricks Workflow)
                 |
           Staging Area
           (Google Cloud Storage)
                 |
               RAW
    (Histórico completo e bruto)
                 |
               TRU
     (Dados confiáveis - Até 5 anos)
                 |
               REF
  (Dados refinados e tratados - Até 3 anos)
                 |
               DMT
    (Métricas e Indicadores - Até 2 anos)
                 |
           Dashboards
      (Visualização e Insights)
Tecnologias Utilizadas
Ingestão: Databricks Workflow
Staging Area: Google Cloud Storage (GCS)
Armazenamento e Processamento: Google BigQuery
Visualização: Tableau, Looker Studio, ou ferramentas similares
Objetivos
Análise de Dados Pessoais: Identificar padrões e insights com base nos animes assistidos.
Visualização de Indicadores: Construir dashboards que mostram métricas como os gêneros mais assistidos, os animes mais populares e as notas médias.
Gerenciamento de Dados: Implementar um pipeline confiável e rastreável para ingestão e processamento de dados.