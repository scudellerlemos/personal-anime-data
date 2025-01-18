CREATE OR REPLACE TABLE `personal-anime-data-2024.TRU_DATA.TRU_ANIMELIST` AS
  SELECT
    *
  FROM `personal-anime-data-2024.RAW_DATA.RAW_ANIMELIST`
  -- Filtrar os últimos 5 anos com base no máximo de SEASON_YEAR
  WHERE SEASON_YEAR >= (
    SELECT MAX(SEASON_YEAR) - 4
    FROM `personal-anime-data-2024.RAW_DATA.RAW_ANIMELIST`
  );