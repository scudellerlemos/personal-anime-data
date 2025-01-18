-- Etapa 1: Criar a tabela TRU_ANIMELIST caso ela não exista
CREATE TABLE IF NOT EXISTS `personal-anime-data-2024.TRU_DATA.TRU_ANIMELIST` AS
SELECT
  *
FROM `personal-anime-data-2024.RAW_DATA.RAW_ANIMELIST`
WHERE 1 = 0; -- Cria a tabela vazia com o mesmo esquema da RAW_ANIMELIST

-- Etapa 2: Remover registros antigos da tabela TRU_ANIMELIST
DELETE FROM `personal-anime-data-2024.TRU_DATA.TRU_ANIMELIST`
WHERE SEASON_YEAR < (
  SELECT MAX(SEASON_YEAR) - 4
  FROM `personal-anime-data-2024.RAW_DATA.RAW_ANIMELIST`
);

-- Etapa 3: Inserir novos registros na tabela TRU_ANIMELIST
INSERT INTO `personal-anime-data-2024.TRU_DATA.TRU_ANIMELIST`
SELECT
  *
FROM `personal-anime-data-2024.RAW_DATA.RAW_ANIMELIST` AS RAW
WHERE SEASON_YEAR >= (
    SELECT MAX(SEASON_YEAR) - 4
    FROM `personal-anime-data-2024.RAW_DATA.RAW_ANIMELIST`
  )
  AND INITCAP(IFNULL(RAW.TITLE__ENGLISH_, 'NA')) NOT IN (
    SELECT INITCAP(IFNULL(TRU.TITLE__ENGLISH_, 'NA'))
    FROM `personal-anime-data-2024.TRU_DATA.TRU_ANIMELIST` AS TRU
  );
