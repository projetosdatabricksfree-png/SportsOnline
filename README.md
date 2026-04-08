# Databricks Sports 🏆

Estrutura organizada de projetos e skills do Databricks para análise de dados esportivos.

## 📁 Estrutura do Projeto

```
DATABRICKS_SPORTS/
│
├── 📁 projects/                  # Seus projetos de análise e transformação
│   └── 📁 databricks-sports/    # Projeto dbt core para pipelines
│       ├── 📁 models/           # Modelos dbt (transformações SQL)
│       ├── 📁 tests/            # Testes dbt
│       ├── 📁 macros/           # Macros reutilizáveis
│       ├── 📁 seeds/            # Dados seed estáticos
│       ├── 📁 snapshots/        # Snapshots para SCD
│       ├── 📁 analyses/         # Análises exploratórias
│       ├── 📁 logs/             # Logs das execuções
│       ├── dbt_project.yml      # Configuração dbt
│       └── README.md            # Documentação do projeto
│
├── 📁 .agents/                   # Skills e agentes Databricks
│   └── 📁 skills/
│       ├── databricks-apps/
│       ├── databricks-core/
│       ├── databricks-dabs/
│       ├── databricks-jobs/
│       ├── databricks-lakebase/
│       ├── databricks-model-serving/
│       └── databricks-pipelines/
│
├── 📁 skills/                    # Configuração compartilhada de skills
│   └── skills-lock.json
│
├── 📁 venv/                      # Ambiente virtual Python
│   └── lib/python3.12/site-packages/
│
└── 📄 README.md                  # Este arquivo
```

## 🚀 Quick Start

### Estrutura de Pastas Explicada

| Pasta | Descrição |
|-------|-----------|
| **projects/** | Seus projetos dbt core e outros projetos de análise |
| **projects/databricks-sports/** | Projeto principal de transformação de dados esportivos |
| **.agents/skills/** | Skills oficiais do Databricks (reference documentation) |
| **venv/** | Ambiente Python virtual com dependências |
| **skills/** | Arquivo de lock compartilhado entre skills |

### Trabalhando com dbt

```bash
cd projects/databricks-sports

# Instalar dependências
dbt deps

# Executar modelos
dbt run

# Rodar testes
dbt test

# Gerar documentação
dbt docs generate
```

## 📚 Recursos Disponíveis

### Skills Databricks
- **databricks-core**: Operações CLI, autenticação e exploração de dados
- **databricks-pipelines**: Lakeflow Spark Declarative Pipelines
- **databricks-jobs**: Desenvolvimento de Lakeflow Jobs
- **databricks-dabs**: Bundles de automação declarativa
- **databricks-apps**: Construção de dashboards e aplicações
- **databricks-lakebase**: Gestão de Postgres Autoscaling
- **databricks-model-serving**: Model Serving endpoints

### Usando as Skills
```bash
# As skills podem ser acessadas via slash commands
/databricks-core
/databricks-pipelines
/databricks-jobs
# ... etc
```

## 🔧 Configuração

### Ambiente Virtual
```bash
# Ativar venv
source venv/bin/activate

# Desativar
deactivate
```

## 📖 Documentação do Projeto dbt

Para informações detalhadas sobre o projeto dbt core:
```bash
cd projects/databricks-sports
cat README.md
```

## 🎯 Próximos Passos

1. ✅ Estrutura de pastas organizada
2. 🔄 Configurar dbt profiles
3. 📊 Desenvolver primeiros modelos
4. 🧪 Adicionar testes
5. 📈 Documentar transformações

---

**Última atualização**: 2026-04-08
