# 📂 Estrutura do Projeto - Databricks Sports

## Organização de Pastas (v2.0)

### ✅ O que mudou na reorganização

A estrutura foi reorganizada para separar melhor **skills/agentes** de **projetos ativos**:

**ANTES:**
```
DATABRICKS_SPORTS/
├── databrickssports/          ← Projeto misturado com skills
├── .agents/skills/            ← Skills Databricks
├── .bob/, .claude/, etc/      ← Pastas de agentes desnecessárias
└── venv/
```

**DEPOIS:**
```
DATABRICKS_SPORTS/
├── projects/                  ← 🎯 Todos os projetos em um lugar
│   └── databricks-sports/     ← Projeto dbt core
├── .agents/skills/            ← 📚 Skills Databricks (reference)
├── skills/                    ← Configuração compartilhada
├── venv/                      ← Dependências Python
└── README.md                  ← Guia principal
```

## 📊 Detalhes de Cada Pasta

### 1️⃣ `projects/databricks-sports/` 
**Projeto dbt core para transformação de dados**

```
projects/databricks-sports/
├── models/              # Modelos SQL dbt
│   └── example/         # Exemplos iniciais
├── tests/               # Testes dbt (data quality)
├── macros/              # Macros reutilizáveis
├── seeds/               # Dados estáticos (CSV)
├── snapshots/           # SCD (Slowly Changing Dimensions)
├── analyses/            # Análises exploratórias
├── logs/                # Logs de execução (gitignored)
├── dbt_project.yml      # Configuração principal
└── README.md            # Documentação do projeto
```

**O que fazer aqui:**
- Desenvolver modelos dbt
- Escrever testes de qualidade
- Documentar transformações
- Configurar semântica e governance

### 2️⃣ `.agents/skills/` 
**Skills oficiais do Databricks (referência)**

```
.agents/skills/
├── databricks-apps/           # 📱 Dashboards e Apps
├── databricks-core/           # 🔧 CLI, Auth, Data Exploration
├── databricks-dabs/           # 📦 Asset Bundles
├── databricks-jobs/           # ⚙️ Jobs e Pipelines
├── databricks-lakebase/       # 🗄️ Postgres Autoscaling
├── databricks-model-serving/  # 🤖 Model Serving
└── databricks-pipelines/      # 🔄 Lakeflow Pipelines
```

**O que fazer aqui:**
- Usar como referência de documentação
- Não modificar diretamente (são skills oficiais)
- Acessar via slash commands na CLI

### 3️⃣ `skills/`
**Configuração compartilhada**

```
skills/
└── skills-lock.json    # Lock file de versionamento
```

### 4️⃣ `venv/`
**Ambiente Virtual Python (não commitar)**

```
venv/
├── bin/                 # Executáveis
├── lib/                 # Pacotes instalados
└── pyvenv.cfg           # Configuração
```

## 🎯 Como Usar a Nova Estrutura

### Iniciar um novo projeto dbt
```bash
cd projects/
mkdir meu-novo-projeto
cd meu-novo-projeto
dbt init .
```

### Trabalhar com o projeto existente
```bash
cd projects/databricks-sports/
dbt run
dbt test
dbt docs generate
```

### Acessar skills Databricks
```bash
# Via CLI Claude Code
/databricks-core            # Skills de core
/databricks-pipelines       # Skills de pipelines
/databricks-jobs            # Skills de jobs
```

## 📋 Checklist de Limpeza

- ✅ Criada pasta `projects/` centralizada
- ✅ Movido `databrickssports/` → `projects/databricks-sports/`
- ✅ Movido `logs/` → `projects/databricks-sports/logs/`
- ✅ Mantido `.agents/skills/` centralizado
- ✅ Criado `.gitignore` apropriado
- ✅ Documentação atualizada

## 🚀 Próximos Passos

1. **Configurar Git** (se não tiver):
   ```bash
   git init
   git add .
   git commit -m "chore: reorganizar estrutura do projeto"
   ```

2. **Configurar dbt_profiles.yml**:
   ```bash
   cd projects/databricks-sports/
   # Criar arquivo de configuração com suas credenciais Databricks
   ```

3. **Instalar dependências dbt**:
   ```bash
   cd projects/databricks-sports/
   dbt deps
   ```

4. **Testar primeiro run**:
   ```bash
   dbt run --select state:new+
   ```

## 💡 Tips

- Use `projects/` para adicionar novos projetos (analytics, ml, etc)
- Skills em `.agents/` são apenas referência - não editar
- Mantenha `venv/` sempre com `.gitignore`
- Documente seus modelos dbt no `projects/databricks-sports/models/`

---

**Versão**: 2.0  
**Data**: 2026-04-08  
**Status**: ✅ Reorganização Completa
