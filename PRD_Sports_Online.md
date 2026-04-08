# CLAUDE.md — Plataforma de Análise em Tempo Real: Cartola FC

## 🎯 Visão Geral do Projeto

Plataforma de ELT em tempo real para extração, transformação e análise de dados do **Cartola FC** (Fantasy Game do Brasileirão), com arquitetura **Medallion (Bronze → Silver → Gold)** no Databricks, orquestração via **dbt**, e 3 modelos preditivos + 1 meta-modelo de ensemble para previsão de desempenho de atletas.

**Stack Tecnológica:**
- **IDE**: VS Code + Claude Code
- **Plataforma de Dados**: Databricks (Unity Catalog)
- **Transformação**: dbt-databricks
- **Linguagem**: Python (PySpark) + SQL
- **API Source**: Cartola FC (api.cartola.globo.com)
- **Orquestração**: Databricks Workflows + dbt

---

## 📡 Catálogo de APIs — Endpoints Testados (08/04/2026)

Todas as APIs abaixo foram testadas e validadas com dados reais da temporada 2026 do Brasileirão (Rodada 11 ativa no momento do teste).

### Endpoints Sem Autenticação

| # | Endpoint | Status | Descrição | Frequência de Atualização |
|---|----------|--------|-----------|---------------------------|
| 1 | `/mercado/status` | ✅ 200 | Status do mercado, rodada atual, temporada, fechamento | A cada mudança de estado do mercado |
| 2 | `/atletas/mercado` | ✅ 200 | **PRINCIPAL** — 740+ atletas com scout, preço, média, status | A cada rodada (mercado aberto) |
| 3 | `/atletas/pontuados` | ⚠️ 204 | Pontuação da rodada em andamento (vazio quando mercado aberto) | Durante/após jogos da rodada |
| 4 | `/pos-rodada/destaques` | ✅ 200 | Mito da rodada, média de cartoletas e pontos | Pós-rodada |
| 5 | `/clubes` | ✅ 200 | Cadastro de todos os clubes (id, nome, escudos, slug) | Início da temporada |
| 6 | `/posicoes` | ✅ 200 | Posições: Goleiro, Lateral, Zagueiro, Meia, Atacante, Técnico | Estático |
| 7 | `/patrocinadores` | ✅ 200 | Patrocinadores das ligas (Globoplay, etc.) | Início da temporada |
| 8 | `/partidas` | ✅ 200 | Partidas da rodada atual com aproveitamento, local, timestamp | A cada rodada |
| 9 | `/partidas/{rodada}` | ✅ 200 | Partidas de rodada específica (1-38) com placares oficiais | Histórico imutável |
| 10 | `/rodadas` | ✅ 200 | Calendário completo: 38 rodadas com datas início/fim | Início da temporada |
| 11 | `/mercado/destaques` | ✅ 200 | Destaques do mercado para escalação | Mercado aberto |
| 12 | `/mercado/destaques/reservas` | ❌ 404 | Indisponível na temporada 2026 | N/A |
| 13 | `/rankings` | ✅ 200 | Níveis de ranking (Inicial, Bronze I-III, Prata I-III, etc.) | Estático |
| 14 | `/ligas` | ✅ 200 | Ligas públicas com slug, tipo, vagas, mata-mata | Dinâmico |
| 15 | `/videos` | ✅ 200 | Vídeos de dicas, destaques e tutoriais do Cartola | Semanal |
| 16 | `/esquemas` (domínio cartolafc) | ⚠️ Vazio | Esquemas táticos — endpoint no domínio `api.cartolafc.globo.com` | Verificar |

### Endpoints Com Autenticação (Requerem Token GLB)

| # | Endpoint | Descrição |
|---|----------|-----------|
| 17 | `/auth/time` | Time do usuário autenticado |
| 18 | `/auth/ligas` | Ligas do usuário |
| 19 | `/auth/liga/{slug}` | Detalhes de liga específica |
| 20 | `/auth/convites` | Convites pendentes |
| 21 | `/auth/noticias` | Notícias personalizadas |
| 22 | `/auth/aviso` | Avisos do sistema |
| 23 | `/auth/express` | Escalação express |
| 24 | `/auth/reset/cartoletas` | Reset de cartoletas |
| 25 | `/auth/reativar/ligas` | Reativação de ligas |

---

## 📊 Schemas das APIs (Mapeamento Completo)

### Schema: `/mercado/status`
```
{
  rodada_atual: INT,              -- Rodada atual do campeonato (1-38)
  status_mercado: INT,            -- 1=aberto, 2=fechado, 4=em manutenção, 6=fim de rodada
  temporada: INT,                 -- Ano da temporada (2026)
  game_over: BOOLEAN,             -- Se o campeonato terminou
  times_escalados: INT,           -- Total de times escalados na rodada
  mercado_pos_rodada: BOOLEAN,    -- Se é pós-rodada
  bola_rolando: BOOLEAN,          -- Se há jogos em andamento
  nome_rodada: STRING,            -- "Rodada 11"
  rodada_final: INT,              -- Última rodada (38)
  cartoleta_inicial: FLOAT,       -- Cartoletas iniciais (100)
  esquema_default_id: INT,        -- Esquema tático padrão
  fechamento: STRUCT {            -- Fechamento do mercado
    dia: INT, mes: INT, ano: INT,
    hora: INT, minuto: INT,
    timestamp: LONG
  }
}
```

### Schema: `/atletas/mercado` (PRINCIPAL — 740+ registros)
```
{
  atleta_id: INT,                 -- ID único do atleta
  nome: STRING,                   -- Nome completo
  apelido: STRING,                -- Nome popular
  apelido_abreviado: STRING,      -- Abreviação
  slug: STRING,                   -- Slug para URL
  foto: STRING,                   -- URL da foto
  clube_id: INT,                  -- FK para clubes
  posicao_id: INT,                -- FK para posições (1-6)
  status_id: INT,                 -- Status do atleta (2,3,5,6,7)
  rodada_id: INT,                 -- Última rodada pontuada
  pontos_num: INT,                -- Pontos na última rodada
  media_num: FLOAT,               -- Média de pontos
  preco_num: FLOAT,               -- Preço em cartoletas
  variacao_num: INT,              -- Variação de preço
  jogos_num: INT,                 -- Número de jogos disputados
  entrou_em_campo: BOOLEAN,       -- Se jogou na última rodada
  scout: MAP<STRING, INT> {       -- Estatísticas detalhadas
    -- OFENSIVOS
    G:  INT,    -- Gol
    A:  INT,    -- Assistência
    FT: INT,    -- Finalização na trave
    FD: INT,    -- Finalização defendida
    FF: INT,    -- Finalização para fora
    FS: INT,    -- Falta sofrida
    DS: INT,    -- Desarme
    I:  INT,    -- Impedimento
    -- DEFENSIVOS
    SG: INT,    -- Saldo de gol (goleiro)
    DE: INT,    -- Defesa (goleiro)
    DP: INT,    -- Defesa de pênalti
    GC: INT,    -- Gol contra
    -- DISCIPLINARES
    CA: INT,    -- Cartão amarelo
    CV: INT,    -- Cartão vermelho
    FC: INT,    -- Falta cometida
    -- PASSES
    PS: INT,    -- Passe decisivo (key pass)
    PC: INT,    -- Passe completo
    PP: INT,    -- Passe para gol (pre-assist)
    V:  INT     -- Vitória
  }
}
```

### Schema: `/clubes`
```
{
  [clube_id]: {                   -- Chave = ID do clube (string)
    id: INT,                      -- ID numérico
    nome: STRING,                 -- Abreviação (3 letras)
    abreviacao: STRING,           -- Abreviação
    slug: STRING,                 -- Slug URL
    apelido: STRING,              -- Apelido popular
    nome_fantasia: STRING,        -- Nome completo
    url_editoria: STRING,         -- URL do GE
    escudos: STRUCT {
      "60x60": STRING,
      "45x45": STRING,
      "30x30": STRING
    }
  }
}
```

### Schema: `/posicoes`
```
{
  [posicao_id]: {
    id: INT,                      -- 1=GOL, 2=LAT, 3=ZAG, 4=MEI, 5=ATA, 6=TEC
    nome: STRING,                 -- Nome completo
    abreviacao: STRING            -- Abreviação (3 letras)
  }
}
```

### Schema: `/partidas` e `/partidas/{rodada}`
```
{
  clubes: MAP<STRING, ClubSchema>,  -- Clubes envolvidos na rodada
  rodada: INT,                      -- Número da rodada
  partidas: ARRAY<{
    partida_id: INT,                -- ID único da partida
    clube_casa_id: INT,             -- FK clube mandante
    clube_visitante_id: INT,        -- FK clube visitante
    clube_casa_posicao: INT,        -- Posição na tabela (mandante)
    clube_visitante_posicao: INT,   -- Posição na tabela (visitante)
    placar_oficial_mandante: INT,   -- Gols mandante (null se não jogou)
    placar_oficial_visitante: INT,  -- Gols visitante (null se não jogou)
    partida_data: STRING,           -- "2026-04-11 16:30:00"
    timestamp: LONG,                -- Unix timestamp
    local: STRING,                  -- Nome do estádio
    valida: BOOLEAN,                -- Se vale para o Cartola
    campeonato_id: INT,             -- ID do campeonato
    aproveitamento_mandante: ARRAY<STRING>,   -- ["v","d","v","d","e"]
    aproveitamento_visitante: ARRAY<STRING>,  -- últimos 5 resultados
    transmissao: STRUCT {
      label: STRING,
      url: STRING
    }
  }>
}
```

### Schema: `/rodadas`
```
ARRAY<{
  rodada_id: INT,                 -- 1 a 38
  nome_rodada: STRING,            -- "Rodada 1"
  inicio: STRING,                 -- "2026-01-28 19:00:00"
  fim: STRING                     -- "2026-01-29 21:30:00"
}>
```

### Schema: `/pos-rodada/destaques`
```
{
  mito_rodada: {
    time_id: INT,
    nome: STRING,
    nome_cartola: STRING,
    slug: STRING,
    clube_id: INT,
    esquema_id: INT,
    assinante: BOOLEAN,
    foto_perfil: STRING,
    url_escudo_svg: STRING,
    url_escudo_png: STRING,
    url_camisa_svg: STRING,
    url_camisa_png: STRING,
    ...
  },
  media_cartoletas: FLOAT,        -- Média geral de cartoletas gastas
  media_pontos: FLOAT             -- Média geral de pontos
}
```

### Schema: `/rankings`
```
ARRAY<{
  ranking_id: INT,                -- ID do nível
  nome: STRING,                   -- "Bronze I", "Prata II", etc.
  nome_fonetico: STRING,          -- Pronúncia
  descricao: STRING,              -- Descrição
  cor_base: STRING,               -- Hex color
  cor_adorno: STRING              -- Hex color
}>
```

### Schema: `/ligas`
```
ARRAY<{
  liga_id: INT,
  slug: STRING,
  nome: STRING,
  descricao: STRING,
  tipo: STRING,                   -- "Aberta", "Fechada", "Moderada"
  imagem: STRING,
  criacao: STRING,                -- Datetime
  quantidade_times: INT,
  vagas_restantes: INT,
  mata_mata: BOOLEAN,
  sem_capitao: BOOLEAN
}>
```

### Schema: `/videos`
```
{
  destaques: { nome: STRING, videos: ARRAY<VideoSchema> },
  dicas: { nome: STRING, videos: ARRAY<VideoSchema> },
  tutoriais: { nome: STRING, videos: ARRAY<VideoSchema> },
  ultimos: { nome: STRING, videos: ARRAY<VideoSchema> }
}

VideoSchema = {
  titulo: STRING,
  duracao: STRING,
  thumbnail: STRING,
  media_url: STRING,
  video_id: INT
}
```

### Tabela de Status dos Atletas
| status_id | Significado |
|-----------|-------------|
| 2 | Dúvida |
| 3 | Suspenso |
| 5 | Contundido |
| 6 | Nulo (sem status especial) |
| 7 | Provável |

### Tabela de Scouts — Pontuação Oficial Cartola FC
| Scout | Descrição | Pontuação |
|-------|-----------|-----------|
| G | Gol | +8.0 |
| A | Assistência | +5.0 |
| SG | Saldo de Gol (não sofrer gol) | +5.0 |
| DE | Defesa (goleiro) | +3.0 |
| DP | Defesa de Pênalti | +7.0 |
| DS | Desarme | +1.5 |
| FS | Falta Sofrida | +0.5 |
| FD | Finalização Defendida | +1.0 |
| FF | Finalização para Fora | +0.8 |
| FT | Finalização na Trave | +3.0 |
| PS | Passe Decisivo | +0.8 |
| PC | Passe Completo | +0.3 |
| PP | Pré-Assistência | +0.3 |
| I | Impedimento | -0.1 |
| V | Vitória | +1.0 |
| FC | Falta Cometida | -0.5 |
| CA | Cartão Amarelo | -2.0 |
| CV | Cartão Vermelho | -5.0 |
| GC | Gol Contra | -5.0 |
| GS | Gol Sofrido (goleiro) | -2.0 |

---

## 🏗️ Arquitetura Medallion no Databricks

### Unity Catalog — Estrutura

```
catalog: cartola_fc
├── schema: bronze          -- Dados brutos (raw JSON → Delta)
├── schema: silver          -- Dados limpos e normalizados
├── schema: gold            -- Agregações e métricas de negócio
└── schema: models          -- Modelos preditivos e meta-modelo
```

### Camada BRONZE — Ingestão Bruta

Tabelas Delta com dados exatamente como vieram da API, acrescidas de metadados de ingestão.

```sql
-- =====================================================
-- BRONZE: Ingestão Bruta com Metadados
-- =====================================================

-- bronze.raw_mercado_status
CREATE TABLE IF NOT EXISTS cartola_fc.bronze.raw_mercado_status (
    rodada_atual INT,
    status_mercado INT,
    temporada INT,
    game_over BOOLEAN,
    times_escalados INT,
    mercado_pos_rodada BOOLEAN,
    bola_rolando BOOLEAN,
    nome_rodada STRING,
    rodada_final INT,
    cartoleta_inicial FLOAT,
    esquema_default_id INT,
    fechamento_dia INT,
    fechamento_mes INT,
    fechamento_ano INT,
    fechamento_hora INT,
    fechamento_minuto INT,
    fechamento_timestamp LONG,
    _raw_json STRING,
    _ingestao_timestamp TIMESTAMP,
    _source_endpoint STRING,
    _batch_id STRING
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
);

-- bronze.raw_atletas_mercado
CREATE TABLE IF NOT EXISTS cartola_fc.bronze.raw_atletas_mercado (
    atleta_id INT,
    nome STRING,
    apelido STRING,
    apelido_abreviado STRING,
    slug STRING,
    foto STRING,
    clube_id INT,
    posicao_id INT,
    status_id INT,
    rodada_id INT,
    pontos_num INT,
    media_num FLOAT,
    preco_num FLOAT,
    variacao_num INT,
    jogos_num INT,
    entrou_em_campo BOOLEAN,
    scout_json STRING,
    _raw_json STRING,
    _ingestao_timestamp TIMESTAMP,
    _rodada_captura INT,
    _batch_id STRING
)
USING DELTA
PARTITIONED BY (_rodada_captura)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze'
);

-- bronze.raw_atletas_pontuados
CREATE TABLE IF NOT EXISTS cartola_fc.bronze.raw_atletas_pontuados (
    atleta_id INT,
    apelido STRING,
    clube_id INT,
    posicao_id INT,
    pontuacao FLOAT,
    scout_json STRING,
    entrou_em_campo BOOLEAN,
    _raw_json STRING,
    _ingestao_timestamp TIMESTAMP,
    _rodada_id INT,
    _batch_id STRING
)
USING DELTA
PARTITIONED BY (_rodada_id)
TBLPROPERTIES ('quality' = 'bronze');

-- bronze.raw_partidas
CREATE TABLE IF NOT EXISTS cartola_fc.bronze.raw_partidas (
    partida_id INT,
    rodada_id INT,
    clube_casa_id INT,
    clube_visitante_id INT,
    clube_casa_posicao INT,
    clube_visitante_posicao INT,
    placar_oficial_mandante INT,
    placar_oficial_visitante INT,
    partida_data STRING,
    timestamp_partida LONG,
    local STRING,
    valida BOOLEAN,
    campeonato_id INT,
    aproveitamento_mandante STRING,
    aproveitamento_visitante STRING,
    _raw_json STRING,
    _ingestao_timestamp TIMESTAMP,
    _batch_id STRING
)
USING DELTA
PARTITIONED BY (rodada_id)
TBLPROPERTIES ('quality' = 'bronze');

-- bronze.raw_clubes
CREATE TABLE IF NOT EXISTS cartola_fc.bronze.raw_clubes (
    clube_id INT,
    nome STRING,
    abreviacao STRING,
    slug STRING,
    apelido STRING,
    nome_fantasia STRING,
    url_editoria STRING,
    escudo_60x60 STRING,
    escudo_45x45 STRING,
    escudo_30x30 STRING,
    _raw_json STRING,
    _ingestao_timestamp TIMESTAMP,
    _batch_id STRING
)
USING DELTA
TBLPROPERTIES ('quality' = 'bronze');

-- bronze.raw_rodadas
CREATE TABLE IF NOT EXISTS cartola_fc.bronze.raw_rodadas (
    rodada_id INT,
    nome_rodada STRING,
    inicio STRING,
    fim STRING,
    _raw_json STRING,
    _ingestao_timestamp TIMESTAMP,
    _batch_id STRING
)
USING DELTA
TBLPROPERTIES ('quality' = 'bronze');

-- bronze.raw_pos_rodada_destaques
CREATE TABLE IF NOT EXISTS cartola_fc.bronze.raw_pos_rodada_destaques (
    rodada_id INT,
    mito_time_id INT,
    mito_nome STRING,
    mito_nome_cartola STRING,
    mito_slug STRING,
    mito_clube_id INT,
    media_cartoletas FLOAT,
    media_pontos FLOAT,
    _raw_json STRING,
    _ingestao_timestamp TIMESTAMP,
    _batch_id STRING
)
USING DELTA
TBLPROPERTIES ('quality' = 'bronze');

-- bronze.raw_ligas
CREATE TABLE IF NOT EXISTS cartola_fc.bronze.raw_ligas (
    liga_id INT,
    slug STRING,
    nome STRING,
    descricao STRING,
    tipo STRING,
    imagem STRING,
    criacao STRING,
    quantidade_times INT,
    vagas_restantes INT,
    mata_mata BOOLEAN,
    sem_capitao BOOLEAN,
    _raw_json STRING,
    _ingestao_timestamp TIMESTAMP,
    _batch_id STRING
)
USING DELTA
TBLPROPERTIES ('quality' = 'bronze');
```

### Camada SILVER — Dados Limpos e Normalizados

```sql
-- =====================================================
-- SILVER: Dados Limpos, Tipados e Enriquecidos
-- =====================================================

-- silver.dim_clubes
CREATE TABLE IF NOT EXISTS cartola_fc.silver.dim_clubes (
    clube_id INT NOT NULL,
    nome STRING,
    abreviacao STRING,
    slug STRING,
    apelido STRING,
    nome_fantasia STRING,
    url_editoria STRING,
    escudo_url STRING,
    _updated_at TIMESTAMP,
    CONSTRAINT pk_clube PRIMARY KEY (clube_id)
)
USING DELTA
TBLPROPERTIES ('quality' = 'silver');

-- silver.dim_posicoes
CREATE TABLE IF NOT EXISTS cartola_fc.silver.dim_posicoes (
    posicao_id INT NOT NULL,
    nome STRING,
    abreviacao STRING,
    CONSTRAINT pk_posicao PRIMARY KEY (posicao_id)
)
USING DELTA
TBLPROPERTIES ('quality' = 'silver');

-- silver.dim_atletas
CREATE TABLE IF NOT EXISTS cartola_fc.silver.dim_atletas (
    atleta_id INT NOT NULL,
    nome STRING,
    apelido STRING,
    apelido_abreviado STRING,
    slug STRING,
    foto_url STRING,
    posicao_id INT,
    clube_id INT,
    status_id INT,
    status_descricao STRING,
    _updated_at TIMESTAMP,
    CONSTRAINT pk_atleta PRIMARY KEY (atleta_id)
)
USING DELTA
TBLPROPERTIES ('quality' = 'silver');

-- silver.dim_rodadas
CREATE TABLE IF NOT EXISTS cartola_fc.silver.dim_rodadas (
    rodada_id INT NOT NULL,
    nome_rodada STRING,
    data_inicio TIMESTAMP,
    data_fim TIMESTAMP,
    temporada INT,
    _updated_at TIMESTAMP,
    CONSTRAINT pk_rodada PRIMARY KEY (rodada_id)
)
USING DELTA
TBLPROPERTIES ('quality' = 'silver');

-- silver.fct_atletas_rodada (SCD Type 2 — snapshot por rodada)
CREATE TABLE IF NOT EXISTS cartola_fc.silver.fct_atletas_rodada (
    atleta_id INT NOT NULL,
    rodada_id INT NOT NULL,
    clube_id INT,
    posicao_id INT,
    status_id INT,
    pontos_num FLOAT,
    media_num FLOAT,
    preco_num FLOAT,
    variacao_num FLOAT,
    jogos_num INT,
    entrou_em_campo BOOLEAN,
    -- Scout expandido (de JSON para colunas)
    scout_gol INT DEFAULT 0,
    scout_assistencia INT DEFAULT 0,
    scout_finalizacao_trave INT DEFAULT 0,
    scout_finalizacao_defendida INT DEFAULT 0,
    scout_finalizacao_fora INT DEFAULT 0,
    scout_falta_sofrida INT DEFAULT 0,
    scout_desarme INT DEFAULT 0,
    scout_impedimento INT DEFAULT 0,
    scout_saldo_gol INT DEFAULT 0,
    scout_defesa INT DEFAULT 0,
    scout_defesa_penalti INT DEFAULT 0,
    scout_gol_contra INT DEFAULT 0,
    scout_cartao_amarelo INT DEFAULT 0,
    scout_cartao_vermelho INT DEFAULT 0,
    scout_falta_cometida INT DEFAULT 0,
    scout_passe_decisivo INT DEFAULT 0,
    scout_passe_completo INT DEFAULT 0,
    scout_pre_assistencia INT DEFAULT 0,
    scout_vitoria INT DEFAULT 0,
    scout_gol_sofrido INT DEFAULT 0,
    _ingestao_timestamp TIMESTAMP,
    CONSTRAINT pk_atleta_rodada PRIMARY KEY (atleta_id, rodada_id)
)
USING DELTA
PARTITIONED BY (rodada_id)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'quality' = 'silver'
);

-- silver.fct_partidas
CREATE TABLE IF NOT EXISTS cartola_fc.silver.fct_partidas (
    partida_id INT NOT NULL,
    rodada_id INT,
    clube_casa_id INT,
    clube_visitante_id INT,
    clube_casa_posicao INT,
    clube_visitante_posicao INT,
    placar_mandante INT,
    placar_visitante INT,
    resultado_mandante STRING,     -- 'V', 'E', 'D'
    resultado_visitante STRING,    -- 'V', 'E', 'D'
    total_gols INT,
    partida_datetime TIMESTAMP,
    estadio STRING,
    valida BOOLEAN,
    aproveitamento_mandante_ultimos5 STRING,
    aproveitamento_visitante_ultimos5 STRING,
    _updated_at TIMESTAMP,
    CONSTRAINT pk_partida PRIMARY KEY (partida_id)
)
USING DELTA
PARTITIONED BY (rodada_id)
TBLPROPERTIES ('quality' = 'silver');

-- silver.fct_mercado_status
CREATE TABLE IF NOT EXISTS cartola_fc.silver.fct_mercado_status (
    rodada_atual INT,
    status_mercado INT,
    status_descricao STRING,       -- Enriquecido: 'Aberto', 'Fechado', etc.
    temporada INT,
    game_over BOOLEAN,
    times_escalados INT,
    bola_rolando BOOLEAN,
    mercado_pos_rodada BOOLEAN,
    fechamento_datetime TIMESTAMP,
    captura_timestamp TIMESTAMP,
    CONSTRAINT pk_status PRIMARY KEY (rodada_atual, captura_timestamp)
)
USING DELTA
TBLPROPERTIES ('quality' = 'silver');

-- silver.fct_destaques_rodada
CREATE TABLE IF NOT EXISTS cartola_fc.silver.fct_destaques_rodada (
    rodada_id INT,
    mito_time_id INT,
    mito_nome STRING,
    mito_clube_id INT,
    media_cartoletas FLOAT,
    media_pontos FLOAT,
    _ingestao_timestamp TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('quality' = 'silver');
```

### Camada GOLD — Métricas de Negócio e Features para ML

```sql
-- =====================================================
-- GOLD: Métricas Agregadas e Feature Store
-- =====================================================

-- gold.metricas_atleta_acumulado
-- Visão consolidada do desempenho de cada atleta na temporada
CREATE TABLE IF NOT EXISTS cartola_fc.gold.metricas_atleta_acumulado (
    atleta_id INT,
    apelido STRING,
    clube_id INT,
    posicao_id INT,
    temporada INT,
    -- Métricas acumuladas
    total_pontos FLOAT,
    media_pontos FLOAT,
    desvio_padrao_pontos FLOAT,
    max_pontos FLOAT,
    min_pontos FLOAT,
    mediana_pontos FLOAT,
    total_jogos INT,
    jogos_como_titular INT,
    -- Scouts acumulados
    total_gols INT,
    total_assistencias INT,
    total_desarmes INT,
    total_cartoes_amarelos INT,
    total_cartoes_vermelhos INT,
    total_saldo_gol INT,
    total_defesas INT,
    total_finalizacoes_trave INT,
    -- Indicadores derivados
    gols_por_jogo FLOAT,
    assistencias_por_jogo FLOAT,
    participacao_gols_por_jogo FLOAT,  -- (G + A) / jogos
    cartoes_por_jogo FLOAT,
    aproveitamento_campo FLOAT,         -- % jogos que entrou em campo
    -- Preço
    preco_atual FLOAT,
    variacao_preco_acumulada FLOAT,
    preco_por_ponto FLOAT,
    valorizacao_percentual FLOAT,
    -- Tendência (últimas 5 rodadas)
    media_ultimas_5 FLOAT,
    tendencia_pontos STRING,           -- 'subindo', 'estavel', 'caindo'
    consistencia_score FLOAT,          -- 1 - (desvio/media) → quanto mais perto de 1, mais consistente
    _updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES ('quality' = 'gold');

-- gold.metricas_clube_rodada
-- Desempenho agregado por clube em cada rodada
CREATE TABLE IF NOT EXISTS cartola_fc.gold.metricas_clube_rodada (
    clube_id INT,
    rodada_id INT,
    total_pontos_cartola FLOAT,
    media_pontos_cartola FLOAT,
    total_gols_marcados INT,
    total_gols_sofridos INT,
    saldo_gols INT,
    resultado STRING,                   -- V, E, D
    posicao_tabela INT,
    pontos_campeonato INT,
    aproveitamento_percentual FLOAT,
    -- Sequências
    sequencia_sem_perder INT,
    sequencia_sem_vencer INT,
    gols_marcados_casa INT,
    gols_marcados_fora INT,
    _updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (rodada_id)
TBLPROPERTIES ('quality' = 'gold');

-- gold.feature_store_previsao
-- Feature Store para alimentar os 3 modelos preditivos
CREATE TABLE IF NOT EXISTS cartola_fc.gold.feature_store_previsao (
    atleta_id INT,
    rodada_alvo INT,               -- Rodada que queremos prever
    -- Features do atleta
    media_pontos_geral FLOAT,
    media_ultimas_3 FLOAT,
    media_ultimas_5 FLOAT,
    desvio_padrao_pontos FLOAT,
    consistencia_score FLOAT,
    tendencia_linear FLOAT,         -- Coeficiente angular da regressão linear das últimas 5 rodadas
    total_jogos INT,
    aproveitamento_campo FLOAT,
    preco_atual FLOAT,
    variacao_preco FLOAT,
    posicao_id INT,
    -- Features do clube do atleta
    clube_posicao_tabela INT,
    clube_aproveitamento FLOAT,
    clube_media_gols_marcados FLOAT,
    clube_media_gols_sofridos FLOAT,
    clube_sequencia_resultado STRING,
    -- Features do adversário
    adversario_id INT,
    adversario_posicao_tabela INT,
    adversario_media_gols_sofridos FLOAT,
    adversario_media_gols_marcados FLOAT,
    -- Features de contexto
    mando_campo STRING,              -- 'casa' ou 'fora'
    eh_classico BOOLEAN,             -- Confronto entre rivais tradicionais
    dias_desde_ultima_partida INT,
    rodada_id_relativa INT,          -- Posição relativa no campeonato (início, meio, fim)
    -- Target (variável alvo)
    pontuacao_real FLOAT,            -- Preenchido após a rodada
    _generated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (rodada_alvo)
TBLPROPERTIES ('quality' = 'gold');

-- gold.tabela_brasileirao
-- Classificação calculada a partir das partidas
CREATE TABLE IF NOT EXISTS cartola_fc.gold.tabela_brasileirao (
    clube_id INT,
    rodada_id INT,
    posicao INT,
    pontos INT,
    jogos INT,
    vitorias INT,
    empates INT,
    derrotas INT,
    gols_pro INT,
    gols_contra INT,
    saldo_gols INT,
    aproveitamento_percentual FLOAT,
    _updated_at TIMESTAMP
)
USING DELTA
PARTITIONED BY (rodada_id)
TBLPROPERTIES ('quality' = 'gold');
```

---

## 🤖 Modelos Preditivos — Arquitetura

### Visão Geral

```
┌─────────────────────────────────────────────────────────────────┐
│                    GOLD: feature_store_previsao                  │
└──────────────┬──────────────┬──────────────┬────────────────────┘
               │              │              │
         ┌─────▼─────┐ ┌─────▼─────┐ ┌─────▼─────┐
         │ MODELO 1  │ │ MODELO 2  │ │ MODELO 3  │
         │ XGBoost   │ │ LightGBM  │ │ Poisson   │
         │ Regressão │ │ Regressão │ │ Bayesiano │
         └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
               │              │              │
         ┌─────▼──────────────▼──────────────▼─────┐
         │           META-MODELO (Stacking)         │
         │    Avalia cobertura (coverage) de cada   │
         │    modelo e seleciona o melhor preditor  │
         └─────────────────┬───────────────────────┘
                           │
                     ┌─────▼─────┐
                     │ PREVISÃO  │
                     │  FINAL    │
                     └───────────┘
```

### MODELO 1: XGBoost Regressor

**Objetivo**: Prever a pontuação do atleta na próxima rodada usando Gradient Boosting com regularização.

**Abordagem**:
- Regressão com XGBoost (xgboost.spark)
- Hiperparâmetros otimizados via Hyperopt no MLflow
- Features: todas as colunas da `feature_store_previsao`
- Target: `pontuacao_real`
- Validação: TimeSeriesSplit (5 folds temporais)

**Métricas de Avaliação**:
- RMSE (Root Mean Square Error)
- MAE (Mean Absolute Error)
- R² Score
- **Coverage Score** (% de previsões dentro de ±2 pontos do real)

### MODELO 2: LightGBM Regressor

**Objetivo**: Mesmo objetivo do Modelo 1, mas com arquitetura diferente para diversificação do ensemble.

**Abordagem**:
- LightGBM via SynapseML ou lightgbm nativo
- Leaf-wise growth (vs. level-wise do XGBoost)
- Feature importance via SHAP
- Categorical features nativas (posicao_id, mando_campo)
- Validação: TimeSeriesSplit (5 folds temporais)

**Métricas de Avaliação**: Mesmas do Modelo 1

### MODELO 3: Poisson-Bayesiano com Regularização

**Objetivo**: Modelar a pontuação como contagem/intensidade usando distribuição de Poisson com priors bayesianos.

**Abordagem**:
- Modelo Poisson Generalizado (para overdispersion)
- Priors informativos baseados em média histórica por posição
- Implementação via statsmodels GLM ou PyMC3
- Especialmente eficaz para scouts discretos (gols, assistências, cartões)
- Produz intervalos de credibilidade (não só ponto central)

**Métricas de Avaliação**:
- Deviance
- Log-likelihood
- **Coverage Score** (% de pontuações reais dentro do intervalo de credibilidade 80%)

### META-MODELO: Stacking com Seleção por Cobertura

**Objetivo**: Combinar as previsões dos 3 modelos base e selecionar dinamicamente o modelo (ou combinação) com melhor **cobertura** (coverage).

**Definição de Cobertura (Coverage)**:
```
Coverage Score = Σ(1 se |previsão - real| ≤ threshold) / N_total
```
Onde `threshold` é definido por posição:
- Goleiro: ±2.0 pontos
- Lateral/Zagueiro: ±2.5 pontos
- Meia: ±3.0 pontos
- Atacante: ±3.5 pontos
- Técnico: ±2.0 pontos

**Estratégia do Meta-Modelo**:
1. **Janela Deslizante**: Avalia coverage das últimas 3 rodadas para cada modelo base
2. **Peso Dinâmico**: Atribui pesos inversamente proporcionais ao erro de cada modelo
3. **Seleção Adaptativa**: Se um modelo tem coverage > 70% isolado, usa só ele; senão, faz média ponderada
4. **Segmentação por Posição**: O meta-modelo pode selecionar modelos diferentes para posições diferentes

**Implementação**:
```python
# Pseudo-código do Meta-Modelo
class MetaModeloCobertura:
    def avaliar_cobertura(self, modelo, dados_validacao, threshold_por_posicao):
        """Calcula coverage score por posição e geral"""
        ...

    def selecionar_modelo(self, coverages_janela):
        """Seleciona modelo com melhor coverage média nas últimas 3 rodadas"""
        ...

    def prever(self, features_rodada_alvo):
        """Gera previsão final usando modelo selecionado ou ensemble ponderado"""
        ...
```

---

## 📁 Estrutura do Projeto dbt

```
cartola_fc_dbt/
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── models/
│   ├── bronze/
│   │   ├── _bronze_sources.yml          -- Source definitions (APIs)
│   │   ├── stg_atletas_mercado.sql
│   │   ├── stg_atletas_pontuados.sql
│   │   ├── stg_partidas.sql
│   │   ├── stg_clubes.sql
│   │   ├── stg_rodadas.sql
│   │   ├── stg_mercado_status.sql
│   │   ├── stg_pos_rodada_destaques.sql
│   │   └── stg_ligas.sql
│   ├── silver/
│   │   ├── _silver_schema.yml           -- Tests & docs
│   │   ├── dim_clubes.sql
│   │   ├── dim_posicoes.sql
│   │   ├── dim_atletas.sql
│   │   ├── dim_rodadas.sql
│   │   ├── fct_atletas_rodada.sql       -- Scout expandido
│   │   ├── fct_partidas.sql
│   │   ├── fct_mercado_status.sql
│   │   └── fct_destaques_rodada.sql
│   ├── gold/
│   │   ├── _gold_schema.yml
│   │   ├── metricas_atleta_acumulado.sql
│   │   ├── metricas_clube_rodada.sql
│   │   ├── tabela_brasileirao.sql
│   │   └── feature_store_previsao.sql
│   └── models_ml/
│       ├── _models_schema.yml
│       ├── previsoes_xgboost.sql        -- Resultados do Modelo 1
│       ├── previsoes_lightgbm.sql       -- Resultados do Modelo 2
│       ├── previsoes_poisson.sql         -- Resultados do Modelo 3
│       └── previsao_final.sql           -- Output do Meta-Modelo
├── macros/
│   ├── parse_scout_json.sql             -- Macro para expandir scout JSON
│   ├── calcular_resultado.sql           -- V/E/D a partir de placares
│   ├── calcular_tendencia.sql           -- Tendência por janela deslizante
│   └── coverage_score.sql              -- Cálculo de cobertura
├── seeds/
│   ├── seed_posicoes.csv               -- Dados estáticos de posições
│   ├── seed_pontuacao_scouts.csv       -- Tabela de pontuação por scout
│   ├── seed_classicos.csv              -- Mapeamento de clássicos regionais
│   └── seed_status_atleta.csv          -- Mapeamento de status
├── tests/
│   ├── assert_atleta_unicidade.sql
│   ├── assert_rodada_range.sql
│   └── assert_placar_nao_negativo.sql
├── snapshots/
│   └── snap_preco_atletas.sql          -- SCD Type 2 do preço
└── analyses/
    ├── top_atletas_custo_beneficio.sql
    ├── analise_mando_campo.sql
    └── comparativo_modelos.sql
```

---

## ⚙️ Ingestão em Tempo Real — Notebooks Databricks

### Notebook: `01_ingestao_bronze.py`

```python
"""
Notebook de Ingestão Bronze - Cartola FC
Executa a cada 15 minutos via Databricks Workflow
Extrai dados de todas as APIs e persiste em Delta Tables
"""

import requests
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import uuid

spark = SparkSession.builder.getOrCreate()

# Configurações
CATALOG = "cartola_fc"
SCHEMA_BRONZE = "bronze"
BASE_URL = "https://api.cartola.globo.com"
BATCH_ID = str(uuid.uuid4())
INGESTAO_TS = datetime.now()

def fetch_api(endpoint: str) -> dict:
    """Fetch genérico com retry e logging"""
    url = f"{BASE_URL}/{endpoint}"
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 204:
            return None  # Sem conteúdo (ex: pontuados fora de rodada)
        else:
            print(f"[WARN] {endpoint} retornou {response.status_code}")
            return None
    except Exception as e:
        print(f"[ERROR] {endpoint}: {str(e)}")
        return None

def ingerir_mercado_status():
    """Ingere status do mercado"""
    data = fetch_api("mercado/status")
    if not data:
        return
    
    row = {
        "rodada_atual": data.get("rodada_atual"),
        "status_mercado": data.get("status_mercado"),
        "temporada": data.get("temporada"),
        "game_over": data.get("game_over"),
        "times_escalados": data.get("times_escalados"),
        "mercado_pos_rodada": data.get("mercado_pos_rodada"),
        "bola_rolando": data.get("bola_rolando"),
        "nome_rodada": data.get("nome_rodada"),
        "rodada_final": data.get("rodada_final"),
        "cartoleta_inicial": float(data.get("cartoleta_inicial", 0)),
        "esquema_default_id": data.get("esquema_default_id"),
        "fechamento_dia": data.get("fechamento", {}).get("dia"),
        "fechamento_mes": data.get("fechamento", {}).get("mes"),
        "fechamento_ano": data.get("fechamento", {}).get("ano"),
        "fechamento_hora": data.get("fechamento", {}).get("hora"),
        "fechamento_minuto": data.get("fechamento", {}).get("minuto"),
        "fechamento_timestamp": data.get("fechamento", {}).get("timestamp"),
        "_raw_json": json.dumps(data),
        "_ingestao_timestamp": INGESTAO_TS,
        "_source_endpoint": "mercado/status",
        "_batch_id": BATCH_ID
    }
    
    df = spark.createDataFrame([row])
    df.write.format("delta").mode("append").saveAsTable(
        f"{CATALOG}.{SCHEMA_BRONZE}.raw_mercado_status"
    )

def ingerir_atletas_mercado():
    """Ingere mercado de atletas (740+ registros)"""
    data = fetch_api("atletas/mercado")
    if not data or "atletas" not in data:
        return
    
    rodada_atual = data.get("status", {}).get("rodada_atual", 0)
    
    rows = []
    for atleta in data["atletas"]:
        rows.append({
            "atleta_id": atleta.get("atleta_id"),
            "nome": atleta.get("nome"),
            "apelido": atleta.get("apelido"),
            "apelido_abreviado": atleta.get("apelido_abreviado"),
            "slug": atleta.get("slug"),
            "foto": atleta.get("foto"),
            "clube_id": atleta.get("clube_id"),
            "posicao_id": atleta.get("posicao_id"),
            "status_id": atleta.get("status_id"),
            "rodada_id": atleta.get("rodada_id"),
            "pontos_num": atleta.get("pontos_num"),
            "media_num": float(atleta.get("media_num", 0)),
            "preco_num": float(atleta.get("preco_num", 0)),
            "variacao_num": atleta.get("variacao_num"),
            "jogos_num": atleta.get("jogos_num"),
            "entrou_em_campo": atleta.get("entrou_em_campo"),
            "scout_json": json.dumps(atleta.get("scout", {})),
            "_raw_json": json.dumps(atleta),
            "_ingestao_timestamp": INGESTAO_TS,
            "_rodada_captura": rodada_atual,
            "_batch_id": BATCH_ID
        })
    
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("append").partitionBy("_rodada_captura").saveAsTable(
        f"{CATALOG}.{SCHEMA_BRONZE}.raw_atletas_mercado"
    )
    print(f"[OK] Ingeridos {len(rows)} atletas do mercado")

def ingerir_partidas_todas_rodadas():
    """Ingere partidas de todas as rodadas com placares"""
    status = fetch_api("mercado/status")
    if not status:
        return
    
    rodada_atual = status.get("rodada_atual", 1)
    
    for rodada in range(1, rodada_atual + 1):
        data = fetch_api(f"partidas/{rodada}")
        if not data or "partidas" not in data:
            continue
        
        rows = []
        for p in data["partidas"]:
            rows.append({
                "partida_id": p.get("partida_id"),
                "rodada_id": rodada,
                "clube_casa_id": p.get("clube_casa_id"),
                "clube_visitante_id": p.get("clube_visitante_id"),
                "clube_casa_posicao": p.get("clube_casa_posicao"),
                "clube_visitante_posicao": p.get("clube_visitante_posicao"),
                "placar_oficial_mandante": p.get("placar_oficial_mandante"),
                "placar_oficial_visitante": p.get("placar_oficial_visitante"),
                "partida_data": p.get("partida_data"),
                "timestamp_partida": p.get("timestamp"),
                "local": p.get("local"),
                "valida": p.get("valida"),
                "campeonato_id": p.get("campeonato_id"),
                "aproveitamento_mandante": json.dumps(p.get("aproveitamento_mandante", [])),
                "aproveitamento_visitante": json.dumps(p.get("aproveitamento_visitante", [])),
                "_raw_json": json.dumps(p),
                "_ingestao_timestamp": INGESTAO_TS,
                "_batch_id": BATCH_ID
            })
        
        df = spark.createDataFrame(rows)
        df.write.format("delta").mode("overwrite").option(
            "replaceWhere", f"rodada_id = {rodada}"
        ).saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.raw_partidas")

def ingerir_clubes():
    """Ingere cadastro de clubes"""
    data = fetch_api("clubes")
    if not data:
        return
    
    rows = []
    for clube_id, clube in data.items():
        rows.append({
            "clube_id": int(clube_id),
            "nome": clube.get("nome"),
            "abreviacao": clube.get("abreviacao"),
            "slug": clube.get("slug"),
            "apelido": clube.get("apelido"),
            "nome_fantasia": clube.get("nome_fantasia"),
            "url_editoria": clube.get("url_editoria"),
            "escudo_60x60": clube.get("escudos", {}).get("60x60"),
            "escudo_45x45": clube.get("escudos", {}).get("45x45"),
            "escudo_30x30": clube.get("escudos", {}).get("30x30"),
            "_raw_json": json.dumps(clube),
            "_ingestao_timestamp": INGESTAO_TS,
            "_batch_id": BATCH_ID
        })
    
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{CATALOG}.{SCHEMA_BRONZE}.raw_clubes"
    )

def ingerir_rodadas():
    """Ingere calendário de rodadas"""
    data = fetch_api("rodadas")
    if not data:
        return
    
    rows = []
    for r in data:
        rows.append({
            "rodada_id": r.get("rodada_id"),
            "nome_rodada": r.get("nome_rodada"),
            "inicio": r.get("inicio"),
            "fim": r.get("fim"),
            "_raw_json": json.dumps(r),
            "_ingestao_timestamp": INGESTAO_TS,
            "_batch_id": BATCH_ID
        })
    
    df = spark.createDataFrame(rows)
    df.write.format("delta").mode("overwrite").saveAsTable(
        f"{CATALOG}.{SCHEMA_BRONZE}.raw_rodadas"
    )

# ===== EXECUÇÃO =====
print(f"[START] Ingestão Bronze — Batch: {BATCH_ID}")
ingerir_mercado_status()
ingerir_clubes()
ingerir_rodadas()
ingerir_atletas_mercado()
ingerir_partidas_todas_rodadas()
print(f"[END] Ingestão Bronze concluída")
```

---

## 🔧 Configuração dbt

### `dbt_project.yml`

```yaml
name: 'cartola_fc'
version: '1.0.0'
config-version: 2

profile: 'cartola_fc'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  cartola_fc:
    bronze:
      +schema: bronze
      +materialized: view
      +tags: ['bronze', 'staging']
    silver:
      +schema: silver
      +materialized: incremental
      +incremental_strategy: merge
      +tags: ['silver', 'transformacao']
    gold:
      +schema: gold
      +materialized: table
      +tags: ['gold', 'metricas']
    models_ml:
      +schema: models
      +materialized: table
      +tags: ['ml', 'previsao']

seeds:
  cartola_fc:
    +schema: silver
```

### `profiles.yml`

```yaml
cartola_fc:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: cartola_fc
      schema: dev
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 4
    prod:
      type: databricks
      catalog: cartola_fc
      schema: "{{ target.schema }}"
      host: "{{ env_var('DATABRICKS_HOST') }}"
      http_path: "{{ env_var('DATABRICKS_HTTP_PATH') }}"
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      threads: 8
```

---

## 🧠 Notebooks dos Modelos Preditivos

### Notebook: `02_modelo_xgboost.py`

```python
"""
MODELO 1: XGBoost Regressor
Previsão de pontuação de atletas no Cartola FC
"""
import mlflow
import mlflow.xgboost
from xgboost import XGBRegressor
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np
import pandas as pd

mlflow.set_experiment("/cartola_fc/modelos/xgboost_regressor")

# Carregar feature store
df = spark.table("cartola_fc.gold.feature_store_previsao").filter(
    col("pontuacao_real").isNotNull()
).toPandas()

# Features e target
FEATURE_COLS = [
    'media_pontos_geral', 'media_ultimas_3', 'media_ultimas_5',
    'desvio_padrao_pontos', 'consistencia_score', 'tendencia_linear',
    'total_jogos', 'aproveitamento_campo', 'preco_atual', 'variacao_preco',
    'posicao_id', 'clube_posicao_tabela', 'clube_aproveitamento',
    'clube_media_gols_marcados', 'clube_media_gols_sofridos',
    'adversario_posicao_tabela', 'adversario_media_gols_sofridos',
    'adversario_media_gols_marcados', 'dias_desde_ultima_partida',
    'rodada_id_relativa'
]

# Encoding de categorias
df['mando_campo_encoded'] = (df['mando_campo'] == 'casa').astype(int)
df['eh_classico_encoded'] = df['eh_classico'].astype(int)
FEATURE_COLS += ['mando_campo_encoded', 'eh_classico_encoded']

X = df[FEATURE_COLS].fillna(0)
y = df['pontuacao_real']

# TimeSeriesSplit
tscv = TimeSeriesSplit(n_splits=5)

THRESHOLD_POR_POSICAO = {1: 2.0, 2: 2.5, 3: 2.5, 4: 3.0, 5: 3.5, 6: 2.0}

def calcular_coverage(y_true, y_pred, posicoes, threshold_map):
    """Calcula coverage score segmentado por posição"""
    acertos = 0
    for real, pred, pos in zip(y_true, y_pred, posicoes):
        threshold = threshold_map.get(pos, 3.0)
        if abs(pred - real) <= threshold:
            acertos += 1
    return acertos / len(y_true)

with mlflow.start_run(run_name="xgboost_v1"):
    model = XGBRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_alpha=0.1,
        reg_lambda=1.0,
        random_state=42
    )
    
    scores = {'rmse': [], 'mae': [], 'r2': [], 'coverage': []}
    
    for train_idx, val_idx in tscv.split(X):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
        
        model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=False)
        y_pred = model.predict(X_val)
        
        scores['rmse'].append(np.sqrt(mean_squared_error(y_val, y_pred)))
        scores['mae'].append(mean_absolute_error(y_val, y_pred))
        scores['r2'].append(r2_score(y_val, y_pred))
        scores['coverage'].append(
            calcular_coverage(y_val, y_pred, df.iloc[val_idx]['posicao_id'], THRESHOLD_POR_POSICAO)
        )
    
    # Log métricas
    mlflow.log_params(model.get_params())
    mlflow.log_metric("avg_rmse", np.mean(scores['rmse']))
    mlflow.log_metric("avg_mae", np.mean(scores['mae']))
    mlflow.log_metric("avg_r2", np.mean(scores['r2']))
    mlflow.log_metric("avg_coverage", np.mean(scores['coverage']))
    
    # Treinar modelo final com todos os dados
    model.fit(X, y)
    mlflow.xgboost.log_model(model, "xgboost_model")
    
    print(f"Coverage médio: {np.mean(scores['coverage']):.4f}")
    print(f"RMSE médio: {np.mean(scores['rmse']):.4f}")
```

### Notebook: `03_modelo_lightgbm.py`

```python
"""
MODELO 2: LightGBM Regressor
Previsão de pontuação com árvore leaf-wise
"""
import mlflow
import mlflow.lightgbm
import lightgbm as lgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np
import pandas as pd

mlflow.set_experiment("/cartola_fc/modelos/lightgbm_regressor")

df = spark.table("cartola_fc.gold.feature_store_previsao").filter(
    col("pontuacao_real").isNotNull()
).toPandas()

FEATURE_COLS = [
    'media_pontos_geral', 'media_ultimas_3', 'media_ultimas_5',
    'desvio_padrao_pontos', 'consistencia_score', 'tendencia_linear',
    'total_jogos', 'aproveitamento_campo', 'preco_atual', 'variacao_preco',
    'posicao_id', 'clube_posicao_tabela', 'clube_aproveitamento',
    'clube_media_gols_marcados', 'clube_media_gols_sofridos',
    'adversario_posicao_tabela', 'adversario_media_gols_sofridos',
    'adversario_media_gols_marcados', 'dias_desde_ultima_partida',
    'rodada_id_relativa', 'mando_campo', 'eh_classico'
]

X = df[FEATURE_COLS].copy()
X['mando_campo'] = X['mando_campo'].astype('category')
X['eh_classico'] = X['eh_classico'].astype('category')
y = df['pontuacao_real']

THRESHOLD_POR_POSICAO = {1: 2.0, 2: 2.5, 3: 2.5, 4: 3.0, 5: 3.5, 6: 2.0}
tscv = TimeSeriesSplit(n_splits=5)

with mlflow.start_run(run_name="lightgbm_v1"):
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'boosting_type': 'gbdt',
        'num_leaves': 31,
        'learning_rate': 0.05,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'lambda_l1': 0.1,
        'lambda_l2': 1.0,
        'verbose': -1
    }
    
    scores = {'rmse': [], 'mae': [], 'r2': [], 'coverage': []}
    
    for train_idx, val_idx in tscv.split(X):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
        
        train_data = lgb.Dataset(X_train, label=y_train, categorical_feature=['mando_campo', 'eh_classico'])
        val_data = lgb.Dataset(X_val, label=y_val, reference=train_data)
        
        model = lgb.train(
            params, train_data,
            num_boost_round=500,
            valid_sets=[val_data],
            callbacks=[lgb.early_stopping(50), lgb.log_evaluation(0)]
        )
        
        y_pred = model.predict(X_val)
        scores['rmse'].append(np.sqrt(mean_squared_error(y_val, y_pred)))
        scores['mae'].append(mean_absolute_error(y_val, y_pred))
        scores['r2'].append(r2_score(y_val, y_pred))
        
        acertos = sum(1 for r, p, pos in zip(y_val, y_pred, df.iloc[val_idx]['posicao_id'])
                      if abs(p - r) <= THRESHOLD_POR_POSICAO.get(pos, 3.0))
        scores['coverage'].append(acertos / len(y_val))
    
    mlflow.log_params(params)
    mlflow.log_metric("avg_rmse", np.mean(scores['rmse']))
    mlflow.log_metric("avg_mae", np.mean(scores['mae']))
    mlflow.log_metric("avg_r2", np.mean(scores['r2']))
    mlflow.log_metric("avg_coverage", np.mean(scores['coverage']))
    
    # Modelo final
    final_data = lgb.Dataset(X, label=y, categorical_feature=['mando_campo', 'eh_classico'])
    final_model = lgb.train(params, final_data, num_boost_round=500)
    mlflow.lightgbm.log_model(final_model, "lightgbm_model")
    
    print(f"Coverage médio: {np.mean(scores['coverage']):.4f}")
```

### Notebook: `04_modelo_poisson.py`

```python
"""
MODELO 3: Poisson-Bayesiano Generalizado
Modela pontuação como intensidade de eventos com priors por posição
"""
import mlflow
import statsmodels.api as sm
from statsmodels.genmod.families import Poisson, NegativeBinomial
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error
import numpy as np
import pandas as pd
from scipy import stats

mlflow.set_experiment("/cartola_fc/modelos/poisson_bayesiano")

df = spark.table("cartola_fc.gold.feature_store_previsao").filter(
    col("pontuacao_real").isNotNull()
).toPandas()

# Transformar pontuação para escala positiva (Poisson requer >= 0)
# Offset para garantir valores positivos
OFFSET = 10.0
df['pontuacao_positiva'] = df['pontuacao_real'] + OFFSET

FEATURE_COLS = [
    'media_pontos_geral', 'media_ultimas_3', 'media_ultimas_5',
    'desvio_padrao_pontos', 'consistencia_score', 'tendencia_linear',
    'total_jogos', 'aproveitamento_campo', 'preco_atual',
    'clube_posicao_tabela', 'clube_aproveitamento',
    'clube_media_gols_marcados', 'adversario_media_gols_sofridos',
    'rodada_id_relativa'
]

X = df[FEATURE_COLS].fillna(0)
X = sm.add_constant(X)
y = df['pontuacao_positiva']

THRESHOLD_POR_POSICAO = {1: 2.0, 2: 2.5, 3: 2.5, 4: 3.0, 5: 3.5, 6: 2.0}
tscv = TimeSeriesSplit(n_splits=5)

with mlflow.start_run(run_name="poisson_v1"):
    scores = {'rmse': [], 'mae': [], 'coverage_80': [], 'deviance': []}
    
    for train_idx, val_idx in tscv.split(X):
        X_train, X_val = X.iloc[train_idx], X.iloc[val_idx]
        y_train, y_val = y.iloc[train_idx], y.iloc[val_idx]
        
        # Negative Binomial para overdispersion
        model = sm.GLM(y_train, X_train, family=NegativeBinomial())
        result = model.fit()
        
        # Previsões
        mu_pred = result.predict(X_val)
        y_pred_original = mu_pred - OFFSET
        y_val_original = y_val - OFFSET
        
        # Intervalo de credibilidade 80%
        alpha_nb = 1.0 / result.scale if hasattr(result, 'scale') else 1.0
        lower_80 = stats.nbinom.ppf(0.10, n=alpha_nb, p=alpha_nb/(alpha_nb + mu_pred)) - OFFSET
        upper_80 = stats.nbinom.ppf(0.90, n=alpha_nb, p=alpha_nb/(alpha_nb + mu_pred)) - OFFSET
        
        # Coverage: % dentro do intervalo 80%
        in_interval = ((y_val_original >= lower_80) & (y_val_original <= upper_80)).mean()
        
        scores['rmse'].append(np.sqrt(mean_squared_error(y_val_original, y_pred_original)))
        scores['mae'].append(mean_absolute_error(y_val_original, y_pred_original))
        scores['coverage_80'].append(in_interval)
        scores['deviance'].append(result.deviance)
        
        # Coverage por threshold de posição
        acertos = sum(1 for r, p, pos in zip(y_val_original, y_pred_original, df.iloc[val_idx]['posicao_id'])
                      if abs(p - r) <= THRESHOLD_POR_POSICAO.get(pos, 3.0))
        scores.setdefault('coverage_threshold', []).append(acertos / len(y_val))
    
    mlflow.log_metric("avg_rmse", np.mean(scores['rmse']))
    mlflow.log_metric("avg_mae", np.mean(scores['mae']))
    mlflow.log_metric("avg_coverage_80_interval", np.mean(scores['coverage_80']))
    mlflow.log_metric("avg_coverage_threshold", np.mean(scores.get('coverage_threshold', [0])))
    mlflow.log_metric("avg_deviance", np.mean(scores['deviance']))
    
    # Modelo final
    final_model = sm.GLM(y, X, family=NegativeBinomial()).fit()
    mlflow.statsmodels.log_model(final_model, "poisson_model")
    
    print(f"Coverage 80% interval: {np.mean(scores['coverage_80']):.4f}")
    print(f"Coverage threshold: {np.mean(scores.get('coverage_threshold', [0])):.4f}")
```

### Notebook: `05_meta_modelo.py`

```python
"""
META-MODELO: Stacking com Seleção Dinâmica por Cobertura
Avalia os 3 modelos base e seleciona o melhor (ou combina) para cada posição
"""
import mlflow
import numpy as np
import pandas as pd
from pyspark.sql.functions import *

CATALOG = "cartola_fc"

# Carregar previsões dos 3 modelos
df_xgb = spark.table(f"{CATALOG}.models.previsoes_xgboost").toPandas()
df_lgb = spark.table(f"{CATALOG}.models.previsoes_lightgbm").toPandas()
df_poi = spark.table(f"{CATALOG}.models.previsoes_poisson").toPandas()

# Merge por atleta_id + rodada_alvo
df_merged = df_xgb[['atleta_id', 'rodada_alvo', 'posicao_id', 'previsao_xgb', 'pontuacao_real']].merge(
    df_lgb[['atleta_id', 'rodada_alvo', 'previsao_lgb']], on=['atleta_id', 'rodada_alvo']
).merge(
    df_poi[['atleta_id', 'rodada_alvo', 'previsao_poisson']], on=['atleta_id', 'rodada_alvo']
)

THRESHOLD_POR_POSICAO = {1: 2.0, 2: 2.5, 3: 2.5, 4: 3.0, 5: 3.5, 6: 2.0}
JANELA_AVALIACAO = 3  # Últimas N rodadas para calcular coverage

class MetaModeloCobertura:
    """Meta-modelo que seleciona dinamicamente o melhor preditor por cobertura"""
    
    def __init__(self, threshold_map, janela=3):
        self.threshold_map = threshold_map
        self.janela = janela
        self.pesos = {}
    
    def calcular_coverage_por_posicao(self, df_rodada, col_previsao):
        """Calcula coverage de um modelo para uma rodada, segmentado por posição"""
        coverages = {}
        for pos_id in df_rodada['posicao_id'].unique():
            subset = df_rodada[df_rodada['posicao_id'] == pos_id]
            threshold = self.threshold_map.get(pos_id, 3.0)
            acertos = (abs(subset[col_previsao] - subset['pontuacao_real']) <= threshold).sum()
            coverages[pos_id] = acertos / len(subset) if len(subset) > 0 else 0
        
        # Coverage geral
        total_acertos = sum(
            (abs(row[col_previsao] - row['pontuacao_real']) <= self.threshold_map.get(row['posicao_id'], 3.0))
            for _, row in df_rodada.iterrows()
        )
        coverages['geral'] = total_acertos / len(df_rodada)
        return coverages
    
    def avaliar_janela(self, df, rodada_atual):
        """Avalia coverage dos 3 modelos nas últimas N rodadas"""
        rodadas_janela = list(range(max(1, rodada_atual - self.janela), rodada_atual))
        df_janela = df[df['rodada_alvo'].isin(rodadas_janela)]
        
        if len(df_janela) == 0:
            return {'xgb': 0.33, 'lgb': 0.33, 'poisson': 0.34}
        
        modelos = {
            'xgb': 'previsao_xgb',
            'lgb': 'previsao_lgb',
            'poisson': 'previsao_poisson'
        }
        
        coverages = {}
        for nome, col in modelos.items():
            cov = self.calcular_coverage_por_posicao(df_janela, col)
            coverages[nome] = cov
        
        return coverages
    
    def selecionar_e_prever(self, df, rodada_alvo):
        """Seleciona modelo por posição e gera previsão final"""
        coverages = self.avaliar_janela(df, rodada_alvo)
        df_rodada = df[df['rodada_alvo'] == rodada_alvo].copy()
        
        previsoes_finais = []
        modelo_selecionado = []
        
        for _, row in df_rodada.iterrows():
            pos = row['posicao_id']
            
            # Coverage de cada modelo para esta posição
            cov_xgb = coverages.get('xgb', {}).get(pos, 0)
            cov_lgb = coverages.get('lgb', {}).get(pos, 0)
            cov_poi = coverages.get('poisson', {}).get(pos, 0)
            
            # Se algum modelo tem coverage > 70% isolado, usa só ele
            best = max(cov_xgb, cov_lgb, cov_poi)
            
            if best >= 0.70:
                if cov_xgb == best:
                    previsoes_finais.append(row['previsao_xgb'])
                    modelo_selecionado.append('xgboost')
                elif cov_lgb == best:
                    previsoes_finais.append(row['previsao_lgb'])
                    modelo_selecionado.append('lightgbm')
                else:
                    previsoes_finais.append(row['previsao_poisson'])
                    modelo_selecionado.append('poisson')
            else:
                # Média ponderada por coverage
                total_cov = cov_xgb + cov_lgb + cov_poi
                if total_cov == 0:
                    total_cov = 3.0
                    cov_xgb = cov_lgb = cov_poi = 1.0
                
                w_xgb = cov_xgb / total_cov
                w_lgb = cov_lgb / total_cov
                w_poi = cov_poi / total_cov
                
                prev = (w_xgb * row['previsao_xgb'] +
                        w_lgb * row['previsao_lgb'] +
                        w_poi * row['previsao_poisson'])
                
                previsoes_finais.append(prev)
                modelo_selecionado.append(f'ensemble({w_xgb:.2f}/{w_lgb:.2f}/{w_poi:.2f})')
        
        df_rodada['previsao_final'] = previsoes_finais
        df_rodada['modelo_selecionado'] = modelo_selecionado
        
        return df_rodada

# Executar meta-modelo
meta = MetaModeloCobertura(THRESHOLD_POR_POSICAO, janela=JANELA_AVALIACAO)

resultados = []
rodadas_disponiveis = sorted(df_merged['rodada_alvo'].unique())

for rodada in rodadas_disponiveis:
    if rodada <= JANELA_AVALIACAO:
        continue  # Precisa de histórico para avaliar
    
    resultado = meta.selecionar_e_prever(df_merged, rodada)
    resultados.append(resultado)

df_final = pd.concat(resultados, ignore_index=True)

# Salvar previsão final
spark_df = spark.createDataFrame(df_final[[
    'atleta_id', 'rodada_alvo', 'posicao_id',
    'previsao_xgb', 'previsao_lgb', 'previsao_poisson',
    'previsao_final', 'modelo_selecionado', 'pontuacao_real'
]])

spark_df.write.format("delta").mode("overwrite").saveAsTable(
    f"{CATALOG}.models.previsao_final"
)

# Log métricas finais
coverage_final = (
    abs(df_final['previsao_final'] - df_final['pontuacao_real']) <=
    df_final['posicao_id'].map(THRESHOLD_POR_POSICAO)
).mean()

print(f"=== RESULTADO DO META-MODELO ===")
print(f"Coverage Final: {coverage_final:.4f}")
print(f"Distribuição de modelos selecionados:")
print(df_final['modelo_selecionado'].value_counts())
```

---

## 📋 Instruções para Claude Code

### Como Usar Este Documento

Este `CLAUDE.md` é o prompt completo para o **Claude Code** construir o projeto. Copie e cole as seções relevantes conforme a fase de implementação.

### Fases de Execução

**Fase 1 — Infraestrutura Databricks**
```
Tarefa: Criar o Unity Catalog, schemas (bronze, silver, gold, models) e todas
as tabelas Delta conforme definido na seção "Arquitetura Medallion no Databricks".
Use os SQLs exatos deste documento. Ative autoOptimize em todas as tabelas.
```

**Fase 2 — Ingestão Bronze**
```
Tarefa: Implementar o notebook 01_ingestao_bronze.py conforme especificado.
Criar um Databricks Workflow que execute a cada 15 minutos durante rodadas
(bola_rolando = true) e a cada 1 hora quando mercado está aberto.
```

**Fase 3 — Projeto dbt**
```
Tarefa: Criar o projeto dbt com a estrutura de diretórios especificada.
Implementar todos os modelos SQL seguindo a arquitetura medallion.
Criar macros para parse_scout_json, calcular_resultado e calcular_tendencia.
Seeds com dados estáticos de posições, pontuação e clássicos.
```

**Fase 4 — Feature Engineering (Gold)**
```
Tarefa: Implementar os modelos Gold no dbt:
- metricas_atleta_acumulado (janela deslizante + tendência)
- metricas_clube_rodada (aproveitamento + sequência)
- tabela_brasileirao (classificação calculada)
- feature_store_previsao (todas as features para ML)
```

**Fase 5 — Modelos Preditivos**
```
Tarefa: Implementar os 3 notebooks de modelos (XGBoost, LightGBM, Poisson)
e o Meta-Modelo. Registrar todos no MLflow. Criar Databricks Workflow
que execute após cada rodada na ordem:
1. Ingestão Bronze → 2. dbt run → 3. Modelos 1/2/3 → 4. Meta-Modelo
```

**Fase 6 — Dashboard e App**
```
Tarefa: Criar notebook de visualização com:
- Comparativo de coverage dos 3 modelos por rodada
- Top 20 atletas previstos para próxima rodada
- Análise custo-benefício (pontos previstos / preço)
- Histórico de acurácia do meta-modelo
```

### Regras para o Claude Code

1. **Idioma**: Todo código, comentários, docstrings e nomes de tabelas em português brasileiro
2. **Padrão dbt**: Seguir convenções do dbt (stg_ para staging, dim_ para dimensões, fct_ para fatos)
3. **Delta Lake**: Todas as tabelas em formato Delta com autoOptimize
4. **MLflow**: Todos os modelos registrados com métricas, parâmetros e artefatos
5. **Logging**: Toda ingestão com `_ingestao_timestamp`, `_batch_id` e `_raw_json`
6. **Testes dbt**: Mínimo de uniqueness, not_null e accepted_values para PKs e FKs
7. **Idempotência**: Toda ingestão deve ser idempotente (MERGE ou replaceWhere)
8. **Particionamento**: Bronze por `_rodada_captura`, Silver/Gold por `rodada_id`
9. **Documentação**: Cada modelo dbt deve ter description no schema.yml
10. **Coverage como métrica principal**: O meta-modelo deve priorizar coverage sobre RMSE

---

## 📌 Referência Rápida — URLs das APIs

```bash
# Status do mercado
curl https://api.cartola.globo.com/mercado/status

# Atletas (PRINCIPAL - 740+ jogadores com scouts)
curl https://api.cartola.globo.com/atletas/mercado

# Pontuações da rodada (disponível durante/após jogos)
curl https://api.cartola.globo.com/atletas/pontuados

# Partidas da rodada atual
curl https://api.cartola.globo.com/partidas

# Partidas de rodada específica (1-38)
curl https://api.cartola.globo.com/partidas/10

# Clubes, posições, rodadas (cadastros)
curl https://api.cartola.globo.com/clubes
curl https://api.cartola.globo.com/posicoes
curl https://api.cartola.globo.com/rodadas

# Destaques e rankings
curl https://api.cartola.globo.com/pos-rodada/destaques
curl https://api.cartola.globo.com/mercado/destaques
curl https://api.cartola.globo.com/rankings

# Ligas, patrocinadores, vídeos
curl https://api.cartola.globo.com/ligas
curl https://api.cartola.globo.com/patrocinadores
curl https://api.cartola.globo.com/videos
```
