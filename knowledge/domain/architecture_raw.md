### 1. Paradigm Shift: Classic Batch ETL vs. State-Driven Change Detection

Um _Pipeline_ de **ETL Clássico** opera sob a premissa de movimentação periódica de volume: extrair lotes completos, transformar em uma _Staging Area_ e carregar em um repositório analítico (_OLAP_), conforme documentado no [Data Architecture Copilot](https://gemini.google.com/notebook/76f60a6e-b2a1-4792-9313-d573e5126359). No monitoramento competitivo de preços e estoques (posicionamento **Tier 1 / Fast Wedge** do [B2B Data Engineering Freelance Hub](https://gemini.google.com/notebook/45bee522-aa11-41af-8c2e-1b1d1a007bea)), reprocessar 98% de páginas estáticas a cada janela de execução gera **Crawl Redundancy**, desperdício de _compute_ e cegueira operacional entre janelas de _Batch_, problema mapeado pela arquitetura de referência da [PromptCloud](https://www.promptcloud.com/blog/event-triggered-price-monitoring/).

O **B2B Price & Market Monitor** substitui o paradigma de _Batch ETL_ por uma arquitetura de **Application-Level Change Data Capture (CDC)** combinada com **Dual-Path Routing** (_Hot Path_ operacional e _Cold Path_ analítico). O sistema trata a página web pública ou rota oculta de e-commerce como uma fonte não cooperativa onde o estado atual (_Snapshot_) é confrontado em memória contra o último estado conhecido antes de qualquer escrita ou notificação.

|**Dimensão Arquitetural**|**Classic Batch ETL**|**State-Driven Change Detection (B2B Monitor)**|
|---|---|---|
|**Unidade de Processamento**|Lote integral (_Full Catalog Scan_)|_Delta_ semântico de atributos (_Field-Scoped Diff_)|
|**Gatilho de Escrita (_Sink_)**|Temporal (toda execução grava ou sobrescreve partições)|Condicional por mutação de estado (_State Transition_)|
|** Semântica de Entrega**|Orientada a repositório (_Data Warehouse_ / _Lakehouse_)|_Dual-Path_: _Hot Path_ (_Webhook Push_) + _Cold Path_ (Histórico)|
|**Métrica Primária de SLA**|_Throughput_ (linhas processadas por hora)|_Signal Freshness_ e _Signal-to-Noise Ratio_ (zero falso-positivo)|
|**Custo de Retries / Falhas**|Reprocessamento de partição inteira|Isolamento por SKU/URL com _Idempotency Key_ e _Circuit Breaker_|

### 2. Layer Breakdown & Referência de Mercado

Inspirado nas arquiteturas modernas de **Hybrid Scraping** da [Zyte](https://dev.to/extractdata/hybrid-scraping-the-architecture-for-the-modern-web-4p9h), nos motores de **Diff-Based Alerting** da [PromptCloud](https://www.promptcloud.com/blog/event-triggered-price-monitoring/) e nas diretrizes de inteligência competitiva da [Tendem](https://tendem.ai/blog/competitor-price-monitoring-guide), o sistema divide-se em cinco camadas desacopladas:

1. **Adaptive Acquisition Layer (_Source_ & _Anti-Bot Evasion_):** Camada de coleta com escalonamento progressivo de custo (_Tiered Fetching_). Inicia pela rota de menor _Overhead_ (requisição HTTP direta a _endpoints_ JSON ocultos ou _SSR state blobs_ como `window.__INITIAL_STATE__` com _TLS Fingerprinting_) e escala para _Headless Browser_ apenas para renovar cookies de sessão ou contornar desafios JavaScript.
    
2. **Shift-Left Edge Validation & Normalization (_Data Contracts_):** Aplicação do padrão **Shift-Left Data Quality** do [Data Architecture Copilot](https://gemini.google.com/notebook/76f60a6e-b2a1-4792-9313-d573e5126359). Todo _payload_ extraído passa por validação sintática e semântica em memória (tipagem numérica de preço, presença de moeda, estado de estoque permitido). Falhas de _Parsing_ causadas por **Template Drift** (mudança de layout HTML no concorrente) desviam o registro para uma **Dead Letter Queue (DLQ)** local e acionam um _Circuit Breaker_ por domínio.
    
3. **State & Diff Engine (_Fingerprinting_ & _Change Detection_):** Motor determinístico que separa a identidade da entidade (`entity_key = hash(domain + canonical_sku_or_url)`) da assinatura do estado (`state_hash = hash(normalized_price + currency + availability_status)`). Compara o `state_hash` recém-extraído com o último _Snapshot_ válido no banco via _Point Lookup_ indexado.
    
4. **Temporal Persistence Layer (_State Store_ & _Audit Trail_):** Armazenamento relacional dividido em três estruturas enxutas: (a) **Current State Table** (_SCD Type 1_) para _lookups_ em sub-milissegundos do estado atual de cada SKU; (b) **Price History Ledger** (_SCD Type 2_ / _Append-Only_) gravada exclusivamente quando há mutação de `state_hash`; e (c) **Pipeline Runs Audit** (`pipeline_runs_audit`, conforme contrato do [B2B Data Engineering Freelance Hub](https://gemini.google.com/notebook/45bee522-aa11-41af-8c2e-1b1d1a007bea)) para rastreabilidade operacional.
    
5. **Actionable Dispatch Layer (_Hot Path Sink_):** Motor de regras comerciais e despacho assíncrono via **Webhooks** ([Web Scraper](https://webscraper.io/blog/webhooks-vs-polling-scraping-job-results)). Filtra variações irrelevantes (_Magnitude Thresholds_, ignorando oscilações de centavos), classifica o evento com _Reason Codes_ (`PRICE_DROP`, `OUT_OF_STOCK`, `BACK_IN_STOCK`, `MAP_VIOLATION`) e garante entrega _At-Least-Once_ com controle de _Idempotency_ no canal de destino (Discord, Slack ou Microsoft Teams).
    

## Architectural Decisions & Trade-off Analysis (ADRs)

**Status:** Proposed | **Domain:** Data Architecture / B2B Productized Services (Tier 1)

### 1. The Core Architectural Question

Como estruturar o **B2B Price & Market Monitor** para entregar garantias de nível _Enterprise_ (_Idempotency_, _Data Contracts_, histórico temporal e zero ruído de alerta) mantendo o _Operational Overhead_ compatível com contratos de ciclo curto (3 a 5 dias úteis, ticket de US$ 180 a US$ 350 ou R$ 800 a R$ 1.500 no [B2B Data Engineering Freelance Hub](https://gemini.google.com/notebook/45bee522-aa11-41af-8c2e-1b1d1a007bea))?

### 2. Trade-off Matrix Across Architectural Vectors

|**Vetor de Decisão**|**Option A: Abordagem Ingênua (Anti-Pattern)**|**Option B: Abordagem Over-Engineered (Enterprise Heavy)**|**Option C: Arquitetura Recomendada (Lean State-Driven B2B)**|
|---|---|---|---|
|**1. Coleta Web (_Acquisition_)**|_Headless Browser_ (Playwright/Selenium) em 100% das requisições. Alto consumo de RAM/CPU e baixa velocidade.|Cluster distribuído de _Scrapy_ + _Proxy Mesh_ residencial pago fixo. Custo inviável para margem PME.|**Hybrid Tiered Scraping** ([Zyte](https://dev.to/extractdata/hybrid-scraping-the-architecture-for-the-modern-web-4p9h)): cliente HTTP com _TLS Impersonation_ como padrão; _Headless Browser_ restrito a _Fallback_ ou captura inicial de cookie de sessão.|
|**2. Granularidade do Hash (_Deduplication_)**|`hash(URL + data + preco)`. Dispara alertas falsos na virada da meia-noite mesmo quando o preço não mudou, ou faz _hash_ do DOM inteiro sofrendo com elementos dinâmicos.|Comparação em grafos ou _Diff_ visual via LLM a cada execução. Latência elevada e custo de tokens desnecessário.|**Dual-Hash Semantics** ([PromptCloud](https://www.promptcloud.com/blog/event-triggered-price-monitoring/)): `entity_hash` (identidade imutável do produto) separado do `state_hash` (atributos de negócio sem carimbo de data), evitando alertas repetidos em dias consecutivos.|
|**3. Modelagem de Estado (_Persistence_)**|_Append_ cego de todas as coletas ou _SCD Type 1_ puro (sobrescreve o preço antigo perdendo a série histórica de variação).|_Lakehouse_ distribuído (_Delta Lake_ / _Iceberg_ sobre S3) para poucos milhares de SKUs. _Overhead_ de infraestrutura incompatível com Tier 1.|**Hybrid SCD Type 1 + Type 2 em SQLite (WAL Mode)**: tabela de estado atual para _Point Lookup_ instantâneo + tabela _Append-Only_ de eventos de transição de preço.|
|**4. Qualidade & _Schema Drift_**|_Parsing_ direto para o banco sem validação. Se o site mudar um seletor CSS, grava `NULL` ou `0.00` e dispara alerta falso de queda de 100% no preço.|Testes _a posteriori_ no banco de dados (dbt) após a contaminação da tabela de estado.|**Shift-Left Data Contracts + Circuit Breaker** ([Data Architecture Copilot](https://gemini.google.com/notebook/76f60a6e-b2a1-4792-9313-d573e5126359)): validação estrita em memória antes do _Diff Engine_. Taxa de erro > X% aborta a escrita e emite alerta técnico de _Template Drift_.|
|**5. Despacho de Alertas (_Delivery_)**|Envio síncrono acoplado ao _loop_ de coleta. Se a API do Slack/Discord sofrer _Rate Limit_ (HTTP 429), o coletor quebra e perde o estado.|_Broker_ de mensageria dedicado (Apache Kafka ou RabbitMQ) em containers separados. Complexidade excessiva de manutenção.|**Transactional Outbox Table**: o _Diff Engine_ grava a mudança no banco com `notification_status = PENDING`. O despachante lê a fila local, envia com _Exponential Backoff_ e marca `SENT`.|
|**6. Empacotamento & Agendamento**|Script avulso rodando na máquina do cliente ou _Apache Airflow_ completo com múltiplos containers (Webserver, Scheduler, Redis, Postgres).|Kubernetes / Serverless Cloud proprietário que gera _Vendor Lock-in_ e fricção de _Onboarding_ em PMEs.|**Single-Container Autonomous Daemon**: container único `python-slim` com agendador em processo (ou _Cron_ leve) e volume persistente mapeado via `docker-compose.yml`.|

### 3. Deep-Dive nos Trade-offs Críticos do Projeto

#### 3.1. A Armadilha do Hash com Timestamp (`URL + Data + Preço` vs. `State Transition Hash`)

Na nota inicial do projeto, a premissa de deduplicação menciona o hash `SHA-256` de `(URL + data + preço)`. Em um sistema de monitoramento contínuo de preços, incluir a **data** na chave de deduplicação de estado introduz um efeito colateral severo: se um produto custa R$ 100 hoje e continua custando R$ 100 amanhã, a mudança da data gerará um novo hash à meia-noite e disparará um falso alerta de variação para o cliente.

- **Decisão Arquitetural:** Remover a data do cálculo do `state_hash`. O hash de estado deve conter estritamente os atributos monitorados (`canonical_url | normalized_price | currency | availability`).
    
- **Como preservar a saúde do sistema sem ruído:** O carimbo de tempo (`last_checked_at`) é atualizado silenciosamente na tabela de estado atual (_Heartbeat_ operacional), enquanto uma nova linha na tabela de histórico (`price_change_events`) e um disparo de _Webhook_ só ocorrem quando `new_state_hash != current_state_hash`. Para garantir ao cliente que o "vigia eletrônico" está vivo mesmo em dias sem alteração de preço, implementa-se um **Daily Digest Heartbeat** separado do fluxo de alertas críticos.
    

#### 3.2. SQLite (WAL Mode) vs. PostgreSQL Containerizado no Tier 1

Ambos constam como opções no [B2B Data Engineering Freelance Hub](https://gemini.google.com/notebook/45bee522-aa11-41af-8c2e-1b1d1a007bea). A escolha não deve ser arbitrária, mas guiada pelo padrão de acesso e concorrência:

- **Por que SQLite com `WAL (Write-Ahead Logging)` vence como Default no Tier 1:** Em escopos fechados de até 2 domínios e execução em _Single-Node_, o SQLite elimina 100% do consumo de RAM em _idle_ de um SGBD cliente-servidor, reduz o `docker-compose.yml` a um único serviço e realiza _Point Lookups_ de hashes em microsegundos sem latência de _socket_ de rede. O modo `WAL` permite leituras concorrentes enquanto o coletor grava o lote.
    
- **Trade-off Aceito:** O SQLite opera com _Database-Level Locking_ na escrita, não suportando múltiplos _Workers_ distribuídos em máquinas diferentes nem conexão direta de ferramentas de BI corporativas em rede (Power BI / Looker) sem expor o arquivo.
    

#### 3.3. Proteção contra _Template Drift_ e Falsos Positivos (_Shift-Left Quarantine_)

O maior risco operacional em contratos de _Web Scraping_ B2B (e principal fonte do _Retainer_ mensal de R$ 250/mês ou US$ 60/mês previsto no [B2B Data Engineering Freelance Hub](https://gemini.google.com/notebook/45bee522-aa11-41af-8c2e-1b1d1a007bea)) é a quebra silenciosa de seletores CSS/XPath no site alvo.

- **Mecanismo de Defesa:** Se um e-commerce altera sua estrutura HTML, um _scraper_ sem **Data Contracts** na borda pode interpretar um campo vazio como preço `0` ou `Indisponível`, inundando o canal executivo do cliente com alertas falsos.
    
- **Isolamento Arquitetural:** Nenhuma extração com falha de _schema_ toca a tabela de estado. O registro inválido vai para a tabela de quarentena (_DLQ_), preserva-se o último _Snapshot_ íntegro como válido e, caso a taxa de falha de _Parsing_ de um domínio ultrapasse o limite configurado (ex.: > 20% dos SKUs da execução), o **Circuit Breaker** interrompe o disparo de alertas comerciais daquele domínio e envia uma notificação exclusiva ao canal de _DataOps/Admin_ informando necessidade de manutenção de seletor.
    

### 4. Recommendation & Verdict

**Selected: Single-Container State-Driven CDC Engine (SQLite WAL + Dual-Hash Diff + Outbox Pattern)**

Para posicionar o **B2B Price & Market Monitor** acima dos scripts amadores do mercado freelancer sem violar o teto de 6 a 10 horas líquidas de implementação do **Pacote 02** ([B2B Data Engineering Freelance Hub](https://gemini.google.com/notebook/45bee522-aa11-41af-8c2e-1b1d1a007bea)), a arquitetura deve adotar coleta híbrida parametrizada por contrato (`config.yaml`), validação _Shift-Left_ na borda, separação estrita entre identidade (`entity_key`) e estado (`state_hash`), persistência dupla (_SCD Type 1_ para estado atual + _SCD Type 2_ para histórico de mudanças) em SQLite `WAL` e fila transacional interna (_Outbox Pattern_) para despacho resiliente de _Webhooks_.

### 5. What Would Change My Mind (Falsifiability Triggers)

- **Gatilho 1 (Concorrência de Escrita e Consumo Externo de BI):** Se o cliente exigir que um dashboard externo (Power BI, Metabase ou Looker Studio) consulte o histórico de preços via rede em tempo real, ou se a coleta exigir mais de 4 _Workers_ paralelos gravando simultaneamente, substituir o **SQLite** pelo container **PostgreSQL 16** com _Bulk Upsert_ (`COPY` + `ON CONFLICT`), conforme modelado no [Data Architecture Copilot](https://gemini.google.com/notebook/76f60a6e-b2a1-4792-9313-d573e5126359).
    
- **Gatilho 2 (Latência Sub-Minuto / Flash Sales):** Se o SLA do cliente exigir detecção em segundos para categorias de alta volatilidade (onde o _Polling_ via _Cron_ a cada 15-60 minutos é insuficiente), substituir o agendador temporal puro por uma camada de **Lightweight Signal Watchers** (monitoramento de `<lastmod>` em _Sitemaps_, _RSS/Atom feeds_ ou _ETag/HTTP 304 HEAD probes_ de baixíssimo custo) que disparam o coletor pesado apenas sob sinal de alteração ([PromptCloud](https://www.promptcloud.com/blog/event-triggered-price-monitoring/)).
    
- **Gatilho 3 (Escala de Catálogo > 50.000 SKUs/dia):** Se a volumetria ultrapassar dezenas de milhares de páginas com proteção _Anti-Bot_ agressiva (Cloudflare Turnstile / DataDome), desacoplar a camada de coleta da camada de estado por meio de uma fila assíncrona externa (Redis/Celery) e delegar a resolução de desafio para um _Gateway_ de _Smart Proxy_ com rotação de IP residencial.
    

### 6. Assumptions & Immediate Next Step

- **Assumptions:** O ativo será operado inicialmente como um produto de escopo fechado (**Tier 1**), monitorando de 1 a 2 domínios públicos com volumetria entre 50 e 5.000 SKUs por ciclo, rodando em uma VPS Linux enxuta (1 a 2 vCPUs, 2 a 4 GB RAM) ou ambiente local via Docker.
    
- **Immediate Next Step:** Definir o contrato de dados canônico do sistema (os campos obrigatórios do _Snapshot_ de produto, a composição exata do `entity_key` vs. `state_hash` e os estados da tabela _Outbox_) antes de extrair e refatorar os módulos do `discord-job-hunter-2`.