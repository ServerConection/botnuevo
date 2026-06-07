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
    observacion             TEXT,
    empresa                 VARCHAR(100),
    tipo_canal              VARCHAR(20)
);

-- Migración: agregar columnas empresa y tipo_canal si no existen (para BDs ya creadas)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='auditorias' AND column_name='empresa') THEN
        ALTER TABLE auditorias ADD COLUMN empresa VARCHAR(100);
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name='auditorias' AND column_name='tipo_canal') THEN
        ALTER TABLE auditorias ADD COLUMN tipo_canal VARCHAR(20);
    END IF;
END $$;

-- Índices para consultas rápidas
CREATE INDEX IF NOT EXISTS idx_id_bitrix      ON auditorias (id_bitrix);
CREATE INDEX IF NOT EXISTS idx_calificacion   ON auditorias (calificacion);
CREATE INDEX IF NOT EXISTS idx_fecha_auditada ON auditorias (fecha_hora_auditada);
