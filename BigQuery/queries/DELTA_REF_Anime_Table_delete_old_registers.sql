-- Remover registros antigos da tabela TB_FATO_ANIMELIST para manter apenas os últimos 3 anos
DELETE FROM `personal-anime-data-2024.REF_DATA.TB_FATO_ANIMELIST`
WHERE DATE_SEASON_YEAR < (
  SELECT MAX(DATE_SEASON_YEAR) - 2
  FROM `personal-anime-data-2024.REF_DATA.TB_FATO_ANIMELIST`
);
