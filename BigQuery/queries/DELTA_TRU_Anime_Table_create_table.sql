-- Etapa 1: Criar a tabela TRU_ANIMELIST caso ela não exista
CREATE TABLE IF NOT EXISTS `personal-anime-data-2024.TRU_DATA.TRU_ANIMELIST` AS
SELECT
  *
FROM `personal-anime-data-2024.RAW_DATA.RAW_ANIMELIST`
WHERE 1 = 0; -- Cria a tabela vazia com o mesmo esquema da RAW_ANIMELIST
