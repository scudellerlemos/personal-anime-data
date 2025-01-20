# **📊Personal Anime Data Project**

Este repositório apresenta um projeto analítico que utiliza dados do perfil pessoal Gajs no site [AniList.co](https://anilist.co). Ele foi desenvolvido com uma arquitetura de dados robusta para ingestão, processamento e visualização, com o objetivo de analisar os animes assistidos. O projeto também inclui um dashboard interativo, projetado para explorar métricas e insights sobre os hábitos de consumo de animes.


---

Tecnologias utilizadas:

| Tecnologia   | Descrição                                                                 |
|--------------|-------------------------------------------------------------------------|
| ![Python](https://img.shields.io/badge/-Python-3776AB?style=for-the-badge&logo=python&logoColor=white) | Linguagem de programação principal utilizada para automação e processamento de dados. |
| ![SQL](https://img.shields.io/badge/-SQL-4479A1?style=for-the-badge&logo=postgresql&logoColor=white) | Utilizado para manipulação e análise de dados no BigQuery. |
| ![BigQuery](https://img.shields.io/badge/-BigQuery-669DF6?style=for-the-badge&logo=googlecloud&logoColor=white) | Armazenamento e análise escalável de grandes volumes de dados na nuvem Google. |
| ![Databricks](https://img.shields.io/badge/-Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white) | Plataforma de análise unificada para engenharia de dados e machine learning. |
| ![Google Composer](https://img.shields.io/badge/-Composer-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white) | Orquestração de workflows baseada no Apache Airflow no Google Cloud. |
| ![Apache Airflow](https://img.shields.io/badge/-Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white) | Ferramenta de orquestração de pipelines de dados. |

---

## **🎌Arquitetura Analítica**

Esta seção detalha a implementação de um projeto completo de analytics com as seguintes camadas:

### **1. Ingestão**
- **Agendamento**: Gerenciada por um workflow no **Databricks**.
- **Fonte dos Dados**: Coletados diretamente da **API do AniList.co**.
- **Objetivo**: Captura dados brutos do perfil pessoal **Gajs** e os armazena em uma área de staging no **Google Cloud Storage (GCS)**.

### **2. Staging Area**
- **Tecnologia**: **Google Cloud Storage (GCS)**.
- **Uso**: Armazena arquivos brutos versionados antes de serem processados e ingeridos no BigQuery.
- **Controle**: Garante rastreabilidade total com versionamento de objetos ativado.

### **3. Dados no BigQuery**
- **Tecnologia**: Bigquery
- **Uso**: Armazena dados em várias camadas, semelhante a arquitetura de dados Medallion.

### **4. Orquestração de dados**
- **Tecnologia**: Cloud Composer
- **Uso**: Solicita a execução de scripts em SQL, em horario agendado, para garantir atualização de dados no BigQuery.

#### Esboço da Arquitetura
![image](https://github.com/user-attachments/assets/0e06b4a4-3f52-43a7-b9c3-a93dcca81690)




---

## **🎌Arquitetura de dados**

Esta seção detalha a funcionalidade de cada camada do pipeline de dados. Uma nova camada foi adicionada para separar exclusivamente as tabelas utilizadas em visualizações, como dashboards e painéis, ou para análises específicas.

#### **RAW**
- **Descrição**: Dados brutos e históricos completos.
- **Retenção**: Sem limite de retenção.
- **Objetivo**: Garante rastreabilidade e permite reconstrução do pipeline em caso de necessidade.

#### **TRU (Trusted)**
- **Descrição**: Dados limpos e confiáveis, com aplicação de regras de negócios e garantia de consistência.
- **Retenção**: Até 5 anos.
- **Objetivo**: Fornecer uma base confiável para análises mais detalhadas.

#### **REF (Refined)**
- **Descrição**: Dados refinados e tratados para consumo específico, estruturados para análises exploratórias e relatórios.
- **Objetivo**: Fornecer dados prontos para modelagem de indicadores.

#### DMT (Data Mart) 
- **Descrição**: Métricas e indicadores finais para visualização em dashboards, otimizados para consumo em ferramentas como Tableau ,Looker Studio ou Power BI.
- **Objetivo**: Melhorar o desempenho e a agilidade na geração de insights.

---

## **🎌Visualização**
Painel construido em tabela de camada DMT.
Pode ser visto em [Dashboard](https://public.tableau.com/app/profile/felipe.lemos.scudeller/viz/Minhahistoriacomanimes/VisaoGeral?publish=yes) 

![image](https://github.com/user-attachments/assets/b614528a-29cf-4997-8775-ec08c5dba759)


---

## **🎌Contribuições**

Contribuições são sempre bem-vindas! 

Se você tiver sugestões, melhorias ou ideias para expandir este projeto, sinta-se à vontade para contribuir. Basta seguir os passos abaixo:

1. **Faça um Fork** deste repositório.
2. Crie uma **branch** para a sua funcionalidade ou correção de bug:
   ```bash
   git checkout -b feature-sua-funcionalidade
3. Realize as alterações necessárias e faça o commit:
   ```bash
   git checkout -b feature-sua-funcionalidade
4. Envie suas alterações para o seu fork:
   ```bash
   git push origin feature-sua-funcionalidade
5. Abra um Pull Request (PR) neste repositório.


---
<div align="center">
  Fim! 
Obrigado!
</div>

---

<div align="center">
  <img src="https://media1.tenor.com/m/xk1Dypa4ZDkAAAAd/jeonzflwr.gif" alt="Jeonzflwr GIF" width="300" height="300" />
</div>




