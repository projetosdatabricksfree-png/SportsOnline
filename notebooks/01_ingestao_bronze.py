# Databricks notebook source
# MAGIC %md
# MAGIC # Ingestão Bronze — Cartola FC
# MAGIC Executa a cada 15 minutos via Databricks Workflow.
# MAGIC Extrai dados de todas as APIs públicas e persiste em Delta Tables.

# COMMAND ----------

import requests
import json
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_json
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

CATALOG        = "cartola_fc"
BASE_URL       = "https://api.cartola.globo.com"
BATCH_ID       = str(uuid.uuid4())
INGESTAO_TS    = datetime.utcnow()

print(f"Iniciando ingestão — batch_id: {BATCH_ID}")
print(f"Timestamp: {INGESTAO_TS}")

# COMMAND ----------

def get_api(endpoint: str) -> dict | list | None:
    """Chama a API Cartola FC e retorna o JSON. Retorna None em erro."""
    url = f"{BASE_URL}{endpoint}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 204:
            print(f"  [INFO] {endpoint} retornou 204 (sem conteúdo)")
            return None
        else:
            print(f"  [WARN] {endpoint} retornou {resp.status_code}")
            return None
    except Exception as e:
        print(f"  [ERROR] {endpoint}: {e}")
        return None

# COMMAND ----------
# MAGIC %md ## 1. Mercado Status

mercado_status = get_api("/mercado/status")

if mercado_status:
    fechamento = mercado_status.get("fechamento", {})
    rows = [{
        "rodada_atual":          mercado_status.get("rodada_atual"),
        "status_mercado":        mercado_status.get("status_mercado"),
        "temporada":             mercado_status.get("temporada"),
        "game_over":             mercado_status.get("game_over"),
        "times_escalados":       mercado_status.get("times_escalados"),
        "mercado_pos_rodada":    mercado_status.get("mercado_pos_rodada"),
        "bola_rolando":          mercado_status.get("bola_rolando"),
        "nome_rodada":           mercado_status.get("nome_rodada"),
        "rodada_final":          mercado_status.get("rodada_final"),
        "cartoleta_inicial":     float(mercado_status.get("cartoleta_inicial", 0)),
        "esquema_default_id":    mercado_status.get("esquema_default_id"),
        "fechamento_dia":        fechamento.get("dia"),
        "fechamento_mes":        fechamento.get("mes"),
        "fechamento_ano":        fechamento.get("ano"),
        "fechamento_hora":       fechamento.get("hora"),
        "fechamento_minuto":     fechamento.get("minuto"),
        "fechamento_timestamp":  fechamento.get("timestamp"),
        "_raw_json":             json.dumps(mercado_status),
        "_ingestao_timestamp":   INGESTAO_TS,
        "_source_endpoint":      "/mercado/status",
        "_batch_id":             BATCH_ID
    }]

    df_status = spark.createDataFrame(rows)
    rodada_atual = mercado_status.get("rodada_atual", 0)
    bola_rolando = mercado_status.get("bola_rolando", False)

    (DeltaTable.forName(spark, f"{CATALOG}.bronze.raw_mercado_status")
        .alias("t")
        .merge(df_status.alias("s"), "t.rodada_atual = s.rodada_atual AND t._batch_id = s._batch_id")
        .whenNotMatchedInsertAll()
        .execute())

    print(f"  [OK] mercado_status — Rodada {rodada_atual} | bola_rolando={bola_rolando}")
else:
    rodada_atual = 0
    bola_rolando = False

# COMMAND ----------
# MAGIC %md ## 2. Atletas Mercado (PRINCIPAL — 740+ atletas)

atletas_data = get_api("/atletas/mercado")

if atletas_data:
    atletas = atletas_data.get("atletas", [])
    rows = []
    for a in atletas:
        rows.append({
            "atleta_id":          a.get("atleta_id"),
            "nome":               a.get("nome"),
            "apelido":            a.get("apelido"),
            "apelido_abreviado":  a.get("apelido_abreviado"),
            "slug":               a.get("slug"),
            "foto":               a.get("foto"),
            "clube_id":           a.get("clube_id"),
            "posicao_id":         a.get("posicao_id"),
            "status_id":          a.get("status_id"),
            "rodada_id":          a.get("rodada_id"),
            "pontos_num":         float(a.get("pontos_num", 0) or 0),
            "media_num":          float(a.get("media_num", 0) or 0),
            "preco_num":          float(a.get("preco_num", 0) or 0),
            "variacao_num":       float(a.get("variacao_num", 0) or 0),
            "jogos_num":          a.get("jogos_num", 0),
            "entrou_em_campo":    bool(a.get("entrou_em_campo", False)),
            "scout_json":         json.dumps(a.get("scout", {})),
            "_raw_json":          json.dumps(a),
            "_ingestao_timestamp": INGESTAO_TS,
            "_rodada_captura":    rodada_atual,
            "_batch_id":          BATCH_ID
        })

    df_atletas = spark.createDataFrame(rows)

    (DeltaTable.forName(spark, f"{CATALOG}.bronze.raw_atletas_mercado")
        .alias("t")
        .merge(
            df_atletas.alias("s"),
            "t.atleta_id = s.atleta_id AND t._rodada_captura = s._rodada_captura"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

    print(f"  [OK] atletas_mercado — {len(rows)} atletas processados")

# COMMAND ----------
# MAGIC %md ## 3. Atletas Pontuados (disponível após os jogos)

pontuados_data = get_api("/atletas/pontuados")

if pontuados_data:
    atletas_pont = pontuados_data.get("atletas", {})
    rows = []
    for atleta_id_str, a in atletas_pont.items():
        rows.append({
            "atleta_id":          int(atleta_id_str),
            "apelido":            a.get("apelido"),
            "clube_id":           a.get("clube_id"),
            "posicao_id":         a.get("posicao_id"),
            "pontuacao":          float(a.get("pontuacao", 0) or 0),
            "scout_json":         json.dumps(a.get("scout", {})),
            "entrou_em_campo":    bool(a.get("entrou_em_campo", False)),
            "_raw_json":          json.dumps(a),
            "_ingestao_timestamp": INGESTAO_TS,
            "_rodada_id":         rodada_atual,
            "_batch_id":          BATCH_ID
        })

    if rows:
        df_pont = spark.createDataFrame(rows)
        (DeltaTable.forName(spark, f"{CATALOG}.bronze.raw_atletas_pontuados")
            .alias("t")
            .merge(
                df_pont.alias("s"),
                "t.atleta_id = s.atleta_id AND t._rodada_id = s._rodada_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        print(f"  [OK] atletas_pontuados — {len(rows)} atletas")
    else:
        print("  [INFO] atletas_pontuados — sem dados (mercado ainda aberto)")

# COMMAND ----------
# MAGIC %md ## 4. Partidas

partidas_data = get_api("/partidas")

if partidas_data:
    partidas = partidas_data.get("partidas", [])
    rodada_p  = partidas_data.get("rodada", rodada_atual)
    rows = []
    for p in partidas:
        rows.append({
            "partida_id":                  p.get("partida_id"),
            "rodada_id":                   rodada_p,
            "clube_casa_id":               p.get("clube_casa_id"),
            "clube_visitante_id":          p.get("clube_visitante_id"),
            "clube_casa_posicao":          p.get("clube_casa_posicao"),
            "clube_visitante_posicao":     p.get("clube_visitante_posicao"),
            "placar_oficial_mandante":     p.get("placar_oficial_mandante"),
            "placar_oficial_visitante":    p.get("placar_oficial_visitante"),
            "partida_data":                p.get("partida_data"),
            "timestamp_partida":           p.get("timestamp"),
            "local":                       p.get("local"),
            "valida":                      bool(p.get("valida", False)),
            "campeonato_id":               p.get("campeonato_id"),
            "aproveitamento_mandante":     json.dumps(p.get("aproveitamento_mandante", [])),
            "aproveitamento_visitante":    json.dumps(p.get("aproveitamento_visitante", [])),
            "_raw_json":                   json.dumps(p),
            "_ingestao_timestamp":         INGESTAO_TS,
            "_batch_id":                   BATCH_ID
        })

    if rows:
        df_partidas = spark.createDataFrame(rows)
        (DeltaTable.forName(spark, f"{CATALOG}.bronze.raw_partidas")
            .alias("t")
            .merge(df_partidas.alias("s"), "t.partida_id = s.partida_id AND t.rodada_id = s.rodada_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        print(f"  [OK] partidas — {len(rows)} partidas")

# COMMAND ----------
# MAGIC %md ## 5. Clubes

clubes_data = get_api("/clubes")

if clubes_data:
    rows = []
    for clube_id_str, c in clubes_data.items():
        escudos = c.get("escudos", {})
        rows.append({
            "clube_id":           int(clube_id_str),
            "nome":               c.get("nome"),
            "abreviacao":         c.get("abreviacao"),
            "slug":               c.get("slug"),
            "apelido":            c.get("apelido"),
            "nome_fantasia":      c.get("nome_fantasia"),
            "url_editoria":       c.get("url_editoria"),
            "escudo_60x60":       escudos.get("60x60"),
            "escudo_45x45":       escudos.get("45x45"),
            "escudo_30x30":       escudos.get("30x30"),
            "_raw_json":          json.dumps(c),
            "_ingestao_timestamp": INGESTAO_TS,
            "_batch_id":          BATCH_ID
        })

    df_clubes = spark.createDataFrame(rows)
    (DeltaTable.forName(spark, f"{CATALOG}.bronze.raw_clubes")
        .alias("t")
        .merge(df_clubes.alias("s"), "t.clube_id = s.clube_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"  [OK] clubes — {len(rows)} clubes")

# COMMAND ----------
# MAGIC %md ## 6. Rodadas

rodadas_data = get_api("/rodadas")

if rodadas_data:
    rows = []
    for r in rodadas_data:
        rows.append({
            "rodada_id":          r.get("rodada_id"),
            "nome_rodada":        r.get("nome_rodada"),
            "inicio":             r.get("inicio"),
            "fim":                r.get("fim"),
            "_raw_json":          json.dumps(r),
            "_ingestao_timestamp": INGESTAO_TS,
            "_batch_id":          BATCH_ID
        })

    df_rodadas = spark.createDataFrame(rows)
    (DeltaTable.forName(spark, f"{CATALOG}.bronze.raw_rodadas")
        .alias("t")
        .merge(df_rodadas.alias("s"), "t.rodada_id = s.rodada_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"  [OK] rodadas — {len(rows)} rodadas")

# COMMAND ----------
# MAGIC %md ## 7. Pós-Rodada Destaques

destaques_data = get_api("/pos-rodada/destaques")

if destaques_data:
    mito = destaques_data.get("mito_rodada", {})
    rows = [{
        "rodada_id":          rodada_atual,
        "mito_time_id":       mito.get("time_id"),
        "mito_nome":          mito.get("nome"),
        "mito_nome_cartola":  mito.get("nome_cartola"),
        "mito_slug":          mito.get("slug"),
        "mito_clube_id":      mito.get("clube_id"),
        "media_cartoletas":   float(destaques_data.get("media_cartoletas", 0) or 0),
        "media_pontos":       float(destaques_data.get("media_pontos", 0) or 0),
        "_raw_json":          json.dumps(destaques_data),
        "_ingestao_timestamp": INGESTAO_TS,
        "_batch_id":          BATCH_ID
    }]

    df_dest = spark.createDataFrame(rows)
    (DeltaTable.forName(spark, f"{CATALOG}.bronze.raw_pos_rodada_destaques")
        .alias("t")
        .merge(df_dest.alias("s"), "t.rodada_id = s.rodada_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"  [OK] pos_rodada_destaques")

# COMMAND ----------
# MAGIC %md ## 8. Ligas

ligas_data = get_api("/ligas")

if ligas_data:
    rows = []
    for l in ligas_data:
        rows.append({
            "liga_id":            l.get("liga_id"),
            "slug":               l.get("slug"),
            "nome":               l.get("nome"),
            "descricao":          l.get("descricao"),
            "tipo":               l.get("tipo"),
            "imagem":             l.get("imagem"),
            "criacao":            l.get("criacao"),
            "quantidade_times":   l.get("quantidade_times"),
            "vagas_restantes":    l.get("vagas_restantes"),
            "mata_mata":          bool(l.get("mata_mata", False)),
            "sem_capitao":        bool(l.get("sem_capitao", False)),
            "_raw_json":          json.dumps(l),
            "_ingestao_timestamp": INGESTAO_TS,
            "_batch_id":          BATCH_ID
        })

    if rows:
        df_ligas = spark.createDataFrame(rows)
        (DeltaTable.forName(spark, f"{CATALOG}.bronze.raw_ligas")
            .alias("t")
            .merge(df_ligas.alias("s"), "t.liga_id = s.liga_id")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
        print(f"  [OK] ligas — {len(rows)} ligas")

# COMMAND ----------

print(f"\nIngestão Bronze concluída — batch_id: {BATCH_ID}")
print(f"Rodada atual: {rodada_atual} | Bola rolando: {bola_rolando}")
