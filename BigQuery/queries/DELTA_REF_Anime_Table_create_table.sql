-- Criar a tabela REF_ANIMELIST caso ela não exista
CREATE TABLE IF NOT EXISTS `personal-anime-data-2024.REF_DATA.TB_FATO_ANIMELIST` AS
SELECT
  *
FROM `personal-anime-data-2024.TRU_DATA.TRU_ANIMELIST`
WHERE 1 = 0; -- Cria a tabela vazia com o mesmo esquema da RAW_ANIMELIST
