# Nubank Analytics Engineer Case — Skeleton

Esta estructura sigue la convención *staging → intermediate → marts* de dbt.
1. **`scripts/bootstrap.sh`**: crea la base RAW en Snowflake (o DuckDB) y carga los CSV.
2. **`models/`**: contiene la lógica SQL dividida en capas.
3. **`tests/`**: chequeos adicionales de data quality o reconciliación.
4. **`analysis/`**: notebooks o consultas exploratorias (no se despliegan).
5. **`.env.example`**: variables de entorno para credenciales.

Clona el repo, copia `.env.example` a `.env`, ajusta los valores y ejecuta:

```bash
pip install -r requirements.txt
./scripts/bootstrap.sh      # opcional
dbt seed && dbt run && dbt test
```
