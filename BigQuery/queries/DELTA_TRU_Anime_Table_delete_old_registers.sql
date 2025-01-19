-- Etapa 2: Remover registros antigos da tabela TRU_ANIMELIST
DELETE FROM `personal-anime-data-2024.TRU_DATA.TRU_ANIMELIST`
WHERE SEASON_YEAR < (
  SELECT MAX(SEASON_YEAR) - 4
  FROM `personal-anime-data-2024.RAW_DATA.RAW_ANIMELIST`
);