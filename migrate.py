"""
Script de migración: agrega columnas empresa y tipo_canal a la tabla auditorias
Ejecutar UNA VEZ desde la carpeta BotAuditor:
    python migrate.py
"""
import os, sys
from dotenv import load_dotenv

load_dotenv()

try:
    import psycopg2
except ImportError:
    print("Instalando psycopg2-binary...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2

DB_HOST     = os.getenv("DB_HOST", "dpg-d5l6jvh4tr6s738gfr60-a.oregon-postgres.render.com")
DB_PORT     = int(os.getenv("DB_PORT", 5432))
DB_NAME     = os.getenv("DB_NAME", "bddgeneral")
DB_USER     = os.getenv("DB_USER", "bdd_admin")
DB_PASSWORD = os.getenv("DB_PASSWORD", "CdkOGqe7oDUgy5EkiQyecctFojMqJYi8")

print(f"Conectando a {DB_HOST}/{DB_NAME}...")

conn = psycopg2.connect(
    host=DB_HOST, port=DB_PORT,
    dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
    connect_timeout=20
)
conn.autocommit = True
cur = conn.cursor()

# Verificar columnas actuales
cur.execute("""
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'auditorias' ORDER BY ordinal_position
""")
existing = [row[0] for row in cur.fetchall()]
print(f"Columnas actuales: {existing}")

changes = 0

# Agregar empresa si no existe
if "empresa" not in existing:
    cur.execute("ALTER TABLE auditorias ADD COLUMN empresa VARCHAR(100);")
    print("✓ Columna 'empresa' agregada")
    changes += 1
else:
    print("- Columna 'empresa' ya existe")

# Agregar tipo_canal si no existe
if "tipo_canal" not in existing:
    cur.execute("ALTER TABLE auditorias ADD COLUMN tipo_canal VARCHAR(20);")
    print("✓ Columna 'tipo_canal' agregada")
    changes += 1
else:
    print("- Columna 'tipo_canal' ya existe")

# Índices
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_empresa ON auditorias (empresa);
""")
cur.execute("""
    CREATE INDEX IF NOT EXISTS idx_tipo_canal ON auditorias (tipo_canal);
""")
print("✓ Índices empresa y tipo_canal verificados")

# Verificar resultado
cur.execute("""
    SELECT column_name, data_type FROM information_schema.columns
    WHERE table_name = 'auditorias' ORDER BY ordinal_position
""")
print("\nEsquema final de 'auditorias':")
for row in cur.fetchall():
    print(f"  {row[0]:35} {row[1]}")

cur.execute("SELECT COUNT(*) FROM auditorias")
count = cur.fetchone()[0]
print(f"\nTotal registros en auditorias: {count}")

conn.close()
print(f"\n{'✓ Migración completada. ' + str(changes) + ' cambio(s) aplicado(s).' if changes else '✓ BD ya estaba al día. Sin cambios.'}")
input("\nPresiona ENTER para cerrar...")
