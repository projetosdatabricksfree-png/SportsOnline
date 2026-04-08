# Databricks notebook source
# MAGIC %md
# MAGIC # Setup Unity Catalog — Cartola FC
# MAGIC Executa UMA VEZ para criar catalog e schemas.

# COMMAND ----------

CATALOG = "cartola_fc"
SCHEMAS = ["bronze", "silver", "gold", "models"]

# COMMAND ----------

# Cria o catalog se não existir
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"Catalog '{CATALOG}' pronto.")

# COMMAND ----------

# Cria os schemas
for schema in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"  Schema '{CATALOG}.{schema}' pronto.")

# COMMAND ----------

# Cria as tabelas Bronze
bronze_ddls = [
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_mercado_status (
        rodada_atual        INT,
        status_mercado      INT,
        temporada           INT,
        game_over           BOOLEAN,
        times_escalados     INT,
        mercado_pos_rodada  BOOLEAN,
        bola_rolando        BOOLEAN,
        nome_rodada         STRING,
        rodada_final        INT,
        cartoleta_inicial   FLOAT,
        esquema_default_id  INT,
        fechamento_dia      INT,
        fechamento_mes      INT,
        fechamento_ano      INT,
        fechamento_hora     INT,
        fechamento_minuto   INT,
        fechamento_timestamp LONG,
        _raw_json           STRING,
        _ingestao_timestamp TIMESTAMP,
        _source_endpoint    STRING,
        _batch_id           STRING
    ) USING DELTA
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'quality' = 'bronze')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_atletas_mercado (
        atleta_id           INT,
        nome                STRING,
        apelido             STRING,
        apelido_abreviado   STRING,
        slug                STRING,
        foto                STRING,
        clube_id            INT,
        posicao_id          INT,
        status_id           INT,
        rodada_id           INT,
        pontos_num          FLOAT,
        media_num           FLOAT,
        preco_num           FLOAT,
        variacao_num        FLOAT,
        jogos_num           INT,
        entrou_em_campo     BOOLEAN,
        scout_json          STRING,
        _raw_json           STRING,
        _ingestao_timestamp TIMESTAMP,
        _rodada_captura     INT,
        _batch_id           STRING
    ) USING DELTA
    PARTITIONED BY (_rodada_captura)
    TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'quality' = 'bronze')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_atletas_pontuados (
        atleta_id           INT,
        apelido             STRING,
        clube_id            INT,
        posicao_id          INT,
        pontuacao           FLOAT,
        scout_json          STRING,
        entrou_em_campo     BOOLEAN,
        _raw_json           STRING,
        _ingestao_timestamp TIMESTAMP,
        _rodada_id          INT,
        _batch_id           STRING
    ) USING DELTA
    PARTITIONED BY (_rodada_id)
    TBLPROPERTIES ('quality' = 'bronze')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_partidas (
        partida_id                    INT,
        rodada_id                     INT,
        clube_casa_id                 INT,
        clube_visitante_id            INT,
        clube_casa_posicao            INT,
        clube_visitante_posicao       INT,
        placar_oficial_mandante       INT,
        placar_oficial_visitante      INT,
        partida_data                  STRING,
        timestamp_partida             LONG,
        local                         STRING,
        valida                        BOOLEAN,
        campeonato_id                 INT,
        aproveitamento_mandante       STRING,
        aproveitamento_visitante      STRING,
        _raw_json                     STRING,
        _ingestao_timestamp           TIMESTAMP,
        _batch_id                     STRING
    ) USING DELTA
    PARTITIONED BY (rodada_id)
    TBLPROPERTIES ('quality' = 'bronze')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_clubes (
        clube_id            INT,
        nome                STRING,
        abreviacao          STRING,
        slug                STRING,
        apelido             STRING,
        nome_fantasia       STRING,
        url_editoria        STRING,
        escudo_60x60        STRING,
        escudo_45x45        STRING,
        escudo_30x30        STRING,
        _raw_json           STRING,
        _ingestao_timestamp TIMESTAMP,
        _batch_id           STRING
    ) USING DELTA
    TBLPROPERTIES ('quality' = 'bronze')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_rodadas (
        rodada_id           INT,
        nome_rodada         STRING,
        inicio              STRING,
        fim                 STRING,
        _raw_json           STRING,
        _ingestao_timestamp TIMESTAMP,
        _batch_id           STRING
    ) USING DELTA
    TBLPROPERTIES ('quality' = 'bronze')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_mercado_status_hist (
        rodada_id           INT,
        _raw_json           STRING,
        _ingestao_timestamp TIMESTAMP,
        _batch_id           STRING
    ) USING DELTA
    TBLPROPERTIES ('quality' = 'bronze')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_pos_rodada_destaques (
        rodada_id           INT,
        mito_time_id        INT,
        mito_nome           STRING,
        mito_nome_cartola   STRING,
        mito_slug           STRING,
        mito_clube_id       INT,
        media_cartoletas    FLOAT,
        media_pontos        FLOAT,
        _raw_json           STRING,
        _ingestao_timestamp TIMESTAMP,
        _batch_id           STRING
    ) USING DELTA
    TBLPROPERTIES ('quality' = 'bronze')
    """,
    f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.raw_ligas (
        liga_id             INT,
        slug                STRING,
        nome                STRING,
        descricao           STRING,
        tipo                STRING,
        imagem              STRING,
        criacao             STRING,
        quantidade_times    INT,
        vagas_restantes     INT,
        mata_mata           BOOLEAN,
        sem_capitao         BOOLEAN,
        _raw_json           STRING,
        _ingestao_timestamp TIMESTAMP,
        _batch_id           STRING
    ) USING DELTA
    TBLPROPERTIES ('quality' = 'bronze')
    """
]

for ddl in bronze_ddls:
    spark.sql(ddl.strip())

print("Todas as tabelas Bronze criadas.")

# COMMAND ----------

print("Setup concluído! Catalog cartola_fc pronto para uso.")
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").show()
