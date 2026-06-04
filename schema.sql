-- =====================================================
--  NETLIFE - Bot Auditor ATC
--  Tabla principal de auditorías
-- =====================================================

CREATE TABLE IF NOT EXISTS auditorias (
    id                      SERIAL PRIMARY KEY,
    id_bitrix               VARCHAR(50) NOT NULL,
    asesor                  VARCHAR(150),
    fecha_creacion_lead     TIMESTAMP,
    fecha_hora_auditada     TIMESTAMP DEFAULT NOW(),
    conversacion_anonimizada TEXT,
    puntuacion_venta        INTEGER CHECK (puntuacion_venta BETWEEN 0 AND 100),
    puntuacion_atc          INTEGER CHECK (puntuacion_atc BETWEEN 0 AND 100),
    calificacion            VARCHAR(10) CHECK (calificacion IN ('ATC', 'VENTA')),
    observacion             TEXT
);

-- Índices para consultas rápidas
CREATE INDEX IF NOT EXISTS idx_id_bitrix      ON auditorias (id_bitrix);
CREATE INDEX IF NOT EXISTS idx_calificacion   ON auditorias (calificacion);
CREATE INDEX IF NOT EXISTS idx_fecha_auditada ON auditorias (fecha_hora_auditada);
