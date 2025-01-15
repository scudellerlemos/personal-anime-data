CREATE OR REPLACE TABLE `personal-anime-data-2024.TRU_DATA.TRU_ANIMELIST` AS
  SELECT
    *
  FROM `personal-anime-data-2024.RAW_DATA.RAW_ANIMELIST`
  --Filtar para entrar no Lake somente 3 anos de dados
  WHERE SEASON_YEAR BETWEEN 2022 AND 2024;
