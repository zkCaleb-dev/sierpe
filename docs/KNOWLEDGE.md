# Cómo se construye bien un indexer de Stellar

> Base de conocimiento del proyecto OSS (codename: indexer). Fecha: 2026-08-14.
> Fuentes: código real de nebu, cdp-pipeline-workflow, stellar/wallet-backend,
> stellar ingest SDK, Ponder, graph-node, Chainhook, y los dos indexers propios
> en producción (trustlesswork-indexer-go y Umbra). Cada principio cita su origen.
> Este documento precede al DESIGN.md: aquí está el *porqué*; allá irá el *qué*.

---

## 1. Anatomía canónica

Todos los indexers serios convergen en el mismo pipeline de 5 etapas:

```
┌─────────┐   ┌──────────┐   ┌───────────┐   ┌─────────┐   ┌──────────────┐
│ SOURCE   │──▶│ EXTRACT  │──▶│ PROCESS   │──▶│ STORE   │──▶│ SERVE/DELIVER│
│ ledgers  │   │ XDR→tipos│   │ dominio   │   │ PG      │   │ API/webhook  │
└─────────┘   └──────────┘   └───────────┘   └─────────┘   └──────────────┘
      └──────────────── STATE: cursor · gaps · coverage ────────────────┘
```

La 6ª pieza — **estado** (cursor, gaps, cobertura) — atraviesa todas las demás
y es la que separa un indexer honesto de uno que miente. Las decisiones de
arquitectura son: qué interfaz tiene cada etapa, quién es dueño de cada dato,
y qué garantiza cada frontera.

---

## 2. Las escuelas estudiadas

### 2.1 Escuela oficial Stellar (ingest SDK + wallet-backend + CDP)

**ingest SDK** (`stellar/go-stellar-sdk/ingest`): la fuente es una interfaz
`LedgerBackend` (captive core, RPC, datastore/lake — intercambiables). Encima,
tres readers: `LedgerTransactionReader` (txs de un ledger),
`LedgerChangeReader` (cambios de entries por fees+meta+upgrades),
`CheckpointChangeReader` (estado completo a un checkpoint desde buckets).
⚠️ Advertencia textual del SDK: los readers emiten transacciones exitosas **y
fallidas** — chequear `Result.Successful()` es responsabilidad del consumidor
(lección M2 del TW indexer: teníamos CERO llamadas a eso).

**wallet-backend** (`internal/indexer/`): la taxonomía de procesadores oficial —
participants, token-transfer (state changes), trustlines, accounts,
sac-balances, liquidity-pool shares/instances, contract-deploy,
protocol-wasms/contracts, effects. Patrón: interfaces chicas por procesador
(`ProcessOperation(ctx, opWrapper) ([]T, error)` genérico con `LedgerChangeProcessor[T]`),
un **IndexerBuffer** que acumula todo lo del ledger en memoria y se vuelca en
una transacción, worker pool (`pond`), y `IngestionMetrics` inyectadas.
Es la respuesta oficial a "¿qué tipos de dato existen?": la lista de Caleb
(eventos, estados, SAC, trustlines) es un subconjunto de esta taxonomía.

**CDP / token-transfer-processor**: la dirección estratégica de SDF es
"building blocks componibles". No competir contra esto: consumirlo.
`getEvents v2` (discussion stellar#1872) todavía no existe → la historia
profunda de eventos sigue siendo territorio de indexers.

### 2.2 Escuela Obsrvr (nebu + cdp-pipeline-workflow)

**nebu** — lo más valioso que encontré para nuestra calidad de código Go:

- **Contrato estable mínimo** en `pkg/processor`: `Processor{Name,Type}` +
  tres roles — `Origin` (recibe `xdr.LedgerCloseMeta`), `Transform` (recibe
  `proto.Message`), `Sink` (escribe `proto.Message`) — y un
  `Emitter[T proto.Message]` genérico sobre canal buffereado. Cinco
  interfaces, un archivo cada una. Eso es todo el contrato público.
- **Estabilidad enforced por CI**: snapshots del API en `.api/` + check que
  falla si cambia. Prometer estabilidad no es un README, es un test.
- **`--describe-json`**: cada procesador emite su propio JSON-schema —
  autodescripción como protocolo.
- **Sobre versionado**: `{"_schema":"nebu.token_transfer.v1","_nebu_version":...,
  "meta":{ledgerSequence,closedAtUnix,txHash,transactionIndex,contractAddress},
  "transfer":{...}}` — separación meta/payload, versión en el dato mismo.
- **Registry externo**: los procesadores viven en repos propios, dependen solo
  del contrato, se registran vía `description.yml`. El core no crece con cada
  protocolo nuevo.

**cdp-pipeline-workflow** — el ensamblado por YAML:
`pipeline: {source: {type, config}, processors: [...], consumers: [...]}`.
Fuentes: S3, GCS, filesystem, RPC, captive core. Consumers: PG, Mongo,
ClickHouse, DuckDB, Redis, ZeroMQ, WebSocket. Confirma que "multi-fuente ×
multi-salida por config" es viable y deseado. Su costo: CGO (zmq, sodium) —
nosotros lo evitaremos (CGO complica el binario estático).

### 2.3 Escuela EVM madura (Ponder, graph-node, Envio)

**Ponder** (lo operacional que nos falta a todos en Stellar):

- **Un schema de Postgres por deployment** (`DATABASE_SCHEMA` = pod name o git
  sha). El schema es la unidad de aislamiento y de rollback.
- **Views como interfaz estable**: `--views-schema` crea vistas que apuntan al
  deployment vigente → los consumidores SQL nunca se enteran de un redeploy.
- **`/health` vs `/ready`**: health = proceso vivo (200 inmediato);
  ready = índice al día (503 durante backfill). Los orquestadores necesitan
  ambos; nosotros solo teníamos /healthz+/readyz en TW (bien) — formalizarlo.
- **Resume automático**: mismo schema ⇒ retoma del checkpoint. Crash recovery
  no es una feature, es el default.
- **Direct SQL read-only**: si dejás leer las tablas, es SELECT-only y sobre
  vistas, jamás sobre internas.
- **Separación serve/index**: `ponder serve` = réplicas HTTP sin motor de
  ingesta, misma base. Nuestra API debe poder escalar aparte del ingestor.
- Latencia app↔DB >50ms degrada — documentar co-locación.

**graph-node**: el caso de terror de crecimiento (7 GB/día indexando cadenas
enteras con historia versionada) y su solución: **pruning configurable**
(`history_blocks`), corre en paralelo sin bloquear. Nuestra ventaja
estructural: indexamos contratos registrados, no la cadena → el volumen es
proporcional a la actividad del usuario; y podar es seguro porque la
cadena+archives permiten re-materializar (graph-node poda para siempre).

**Envio**: Postgres + Hasura (GraphQL gratis encima del almacén). ClickHouse
solo como opción avanzada. Refuerza el consenso: Postgres único motor.

### 2.4 Escuela de entrega (Chainhook)

Predicados `if_this/then_that` registrados contra el motor; eventos empaquetados
y empujados a un destino (webhook). **Reorg-aware por diseño** (Bitcoin/Stacks
lo exigen; en Stellar SCP nos regala finalidad — nuestra "reorg" equivalente es
el reset de testnet y la divergencia de hash, ya resuelta en TW). Lección:
la entrega push es un *producto aparte* con su propia complejidad (reintentos,
DLQ, orden); por eso queda en v2 y el pull con cursor es el canon de v1.

### 2.5 Segunda pasada: los repos citados en el Discord

**getEvents v2 (stellar#1872) — leído el proposal completo.** Más importante
de lo que parecía; define el estándar que nuestra API debería hablar:

- Filtros planos con **topics posicionales explícitos** (`topic0..topic3`,
  omitido = wildcard; AND dentro del filtro, OR entre filtros, máx 256).
- **Cursor opaco que codifica LA QUERY ENTERA** (bounds, orden, filtros) —
  no expira y respeta los límites originales. Exactamente nuestro P17.
- Respuesta con `scanStatus` (`HAS_MORE | WAITING_FOR_LEDGERS |
  OLDEST_REACHED | COMPLETE`) + `scannedLedger` — es la versión oficial de
  nuestro "coverage declarado en la respuesta" (P7/P17): el cliente siempre
  sabe si una página vacía significa "no hay" o "aún no escaneé".
- **Term budget** (15 términos/query default) como control de costo.
- ⚠️ CLAVE: tamirms afirma que v2 "funciona idéntico sobre 7 días o historia
  completa" (queries ancladas a ledger + límite de escaneo interno de 10k).
  → **La historia profunda de eventos por RPC está PLANEADA oficialmente.**
  El foso temporal se acorta; el valor durable del appliance es todo lo demás
  (self-host, dominio propio, coverage, estado de contratos, entrega).
- **Decisión derivada: diseñar nuestra API de eventos como superconjunto
  compatible con getEvents v2** (mismos filtros, mismo scanStatus) — cuando
  exista, somos la implementación self-hosted con historia; la fachada
  (patrón Umbra 10) sale casi gratis.

**Creit-Tech/Stellar-Indexer-SDK** (TS, JSR): su diferenciador NO es
historia de eventos — es **contract DATA en vivo**: `fetchContractData()`
consulta entries de storage (instance, keys simbólicas, vecs) de N contratos
en un request (el caso Blend: 35 RPC calls → 1). Y el patrón **protocol
extensions**: clases TS por protocolo (Blend, Axis, Reflector,
StellarDomains) con schemas expuestos, PRs de terceros bienvenidos. Es
nuestro "decoder de protocolo" pero client-side y hosteado. Confirma que
"estado de contrato consultable" (la parte de la lista de Caleb que los
event-indexers ignoran) tiene demanda probada.

**nebu-processor-registry / BUILDING_PROTO_PROCESSORS.md**: flujo proto-first
(schema .proto → protoc → implementar → `RunProtoOriginCLI`). Argumentos:
tipado en compile-time, JSON gratis vía protojson, versionado explícito,
multi-lenguaje, gRPC en 10 líneas. Refuerza la opción proto en la decisión
abierta #2.

**flowctl**: capa de ORQUESTACIÓN de producción (YAML `flowctl/v1`, control
plane embebido, heartbeats, supervisión de procesos). Su propia doc dice:
nebu para prototipar, flowctl para operar. Posicionamiento nuestro: el
appliance elimina la necesidad de orquestador para el caso común — flowctl
compite en el segmento "ensamblá tu pipeline", nosotros en "no quiero
ensamblar nada".

**soroswap/subql**: implementación SubQuery estándar (GraphQL, Docker,
fork-and-code TS). Sin señal de diseño nueva; confirma el modelo framework.

**Mercury/retroshades**: docs públicas superficiales; el concepto = "custom
indexers definidos dentro de tus smart contracts" corriendo en SU infra.
Modelo opuesto al nuestro (lógica en el contrato + hosting de ellos).

**ttp-processor-demo: 404 — ya no existe público.** Lección de ecosistema:
el tooling pre-1.0 de terceros desaparece o se renombra (flowctl/nebu
pivotearon varias veces en un año). → En la decisión #6: adoptar
CONVENCIONES (sobres, nombres, semántica), no DEPENDENCIAS de repos jóvenes.

### 2.6 Los propios (las lecciones pagadas)

**trustlesswork-indexer-go** — la escuela de robustez operacional:

1. Multi-RPC failover con watchdog de tip (el SDK **bloquea** en tips
   congelados; `WithMetrics` panickea en rotación — ambos rodeados).
2. La autoridad de retención es **getLedgers, no getHealth** (el clamp que
   rechazaba rangos que el proveedor SÍ servía).
3. Backpressure: reintento infinito SOLO para reject-publish; canales amqp no
   sanan solos → fatal-fast y que el supervisor reinicie.
4. Publisher confirms por **identidad** (deferred confirmation atada al
   mensaje), no por posición — el desfase de confirmaciones era un bug de
   dinero.
5. Guard de plausibilidad en removals masivos (una respuesta 200 vacía no es
   prueba de ausencia; >50% de un lote ≥10 sin entrada = proveedor roto,
   suprimir y contar en métrica).
6. Catch-up stamping: NO estampar estado del tip con seq histórico durante
   catch-up (defer hasta llegar al tip).
7. Continuidad por hash: verificar `PreviousLedgerHash` al reanudar;
   divergencia = fatal con ambos hashes. Detección de reset de testnet
   (cursor >> tip).
8. Gaps como dato de primera clase, persistidos ANTES de procesar, con id
   determinístico, republicados hasta resolverse.
9. Chunking de getLedgerEntries (≤200 claves) — el poison ledger.
10. `replay --from --to` como subcomando sin lock/estado (corre junto al vivo)
    — recuperación de incidentes en segundos.
11. Plano de control: **pull, no push; reconciliar conjuntos, no eventos**
    (A1: el canal de comandos AMQP se BORRÓ, no se blindó). Todo plano admin
    autenticado y con entropía mínima en el token.
12. Config que miente es deuda: variables sin lector (`STRICT_MODE`) se
    eliminan; secretos redactados en el print de config (`secret:"true"`).
13. Estado dividido: cursor (se escribe siempre) vs watchlist (solo al
    cambiar). Menos I/O, menos corrupción.
14. Idempotencia derivada del dato (`network:contractId:txHash:eventIndex`),
    nunca del wire.
15. Lo que el buffer acumula por ledger (state changes de 11 categorías, txs,
    ops, trustlines, contract changes, escrows, participants) ya está
    documentado en `docs/pluggable-sink-architecture.md` — la investigación de
    sinks pluggables YA EXISTE en ese repo y es insumo directo del nuestro.

**Umbra** — la escuela del appliance:

1. **Cursor y eventos en la misma transacción PG = exactly-once por diseño**
   (sin broker, sin two-phase). La decisión más importante del proyecto.
2. **Registro dinámico de contratos**: tabla `contracts` sembrada por config
   (config manda) + registros por API que sobreviven reinicios; snapshot
   inmutable tras `atomic.Pointer` releído por ledger (costo marginal cero
   porque la ingesta baja ledgers completos y filtra local).
3. **Clasificación por spec on-chain** (`contractspecv0` → event names;
   SAC por ejecutable built-in; fallback por function names; OJO: nombres de
   struct CamelCase → normalizar a snake_case).
4. **Backfill en chunks descendentes** (2000) con cobertura persistida por
   chunk: interrupción = progreso conservado, historia reciente primero,
   clamp al muro de retención con UN gap honesto.
5. **Crudo + derivadas + `rederive`**: guardar el evento crudo con
   procedencia (ledger, tx_index, event_index) permite regenerar las tablas
   derivadas al mejorar un decoder — es la historia de upgrade completa.
6. **Pierna de archivo**: captive core replayando desde History Archives
   rompe la ventana de 7 días; verificado byte-idéntico. ⚠️ Sin
   `EmitUnifiedEvents` (+BeforeProtocol22) el replay OMITE transfers de SAC
   de operaciones clásicas — el replay debe igualar la config del RPC.
7. Frontera de `recover` alrededor de los Get* del SDK (panickean con
   punteros nil).
8. Fallos transitorios no matan el proceso (35 reinicios por un timeout +
   DNS roto de Docker enseñaron backoff en frío y `dns:` fijo en compose).
9. Cobertura por contrato expuesta en `/status` (source, covered_from,
   conteo por event name — delata kinds que un decoder ignora).
10. Fachada de compatibilidad (JSON-RPC byte-idéntica al bootnode) = adopción
    drop-in sin cambiar una línea del cliente. Patrón reutilizable.

---

## 3. Principios de diseño destilados (el contrato del proyecto)

### Ingesta (el módulo primordial según Caleb)

- **P1. Una interfaz de fuente, N implementaciones.** `LedgerSource` estilo
  ingest-SDK/nebu: RPC (vivo + retención), captive core (archivo profundo),
  datastore/lake (S3/GCS/galexie). El loop no sabe de dónde vienen los
  ledgers. El cambio de fuente es config, no código. [ingest SDK, cdp-pipeline, Umbra]
- **P2. El ledger completo es la unidad de ingesta.** Se baja
  `LedgerCloseMeta` entero y se filtra localmente — así registrar un contrato
  más cuesta cero RPC y el filtro es nuestro. [Umbra, Pakana]
- **P3. Failover con watchdog.** Pool de endpoints, deadline por intento,
  clasificación de errores de ventana (beyond-tip espera, below-retention
  falla rápido), autoridad de alcance = getLedgers. [TW 1, 2]
- **P4. Workers en background con pool acotado** (pond o equivalente), pero
  el commit por ledger es de un solo escritor — paralelismo en el proceso,
  serialización en el estado. [wallet-backend, TW]
- **P5. Config de captive core = config del RPC** (unified events). Un gate
  de byte-igualdad entre fuentes antes de confiar en una nueva. [Umbra 6]

### Estado y honestidad

- **P6. Cursor + datos en la misma transacción.** Exactly-once por diseño.
  [Umbra 1]
- **P7. Gaps y cobertura como datos de primera clase**, persistidos antes de
  procesar, expuestos en la API. Un rango sin declarar es una mentira.
  [TW 8, Umbra 9, memoria read-model-data-traps]
- **P8. Continuidad verificable**: PreviousLedgerHash al reanudar, detección
  de reset, divergencia = fatal ruidoso. [TW 7]
- **P9. Dos relojes, siempre**: `ledger_closed_at` (verdad de negocio) vs
  `ingested_at` (verdad operacional). Jamás mezclarlos en queries de negocio.
  [read-model-data-traps]

### Procesamiento

- **P10. Contrato de procesador mínimo y estable**: Origin/Transform/Sink +
  Emitter, con snapshots de API en CI. Los decoders de protocolo viven FUERA
  del core (registry). [nebu]
- **P11. Buffer por ledger**: acumular todo y volcar atómico. [wallet-backend, TW 15]
- **P12. Desconfianza sistemática del dato**: Result.Successful() siempre;
  recover en fronteras XDR; guard de plausibilidad ante ausencias masivas;
  validación de origen (topics[2]==contract_id). [SDK doc, TW 5, Umbra 7]
- **P13. Crudo primero, derivadas después, rederive siempre posible.** [Umbra 5]

### Almacén

- **P14. Postgres, único motor, dueño del schema** (migraciones automáticas,
  el usuario da un DATABASE_URL vacío). Consenso unánime de la categoría.
- **P15. Un schema por deployment + vistas estables** para el que quiera SQL
  directo (read-only). [Ponder]
- **P16. Retención configurable y segura**: podar solo lo re-materializable;
  el archive leg es la red de seguridad. [graph-node, Umbra 6]

### API y entrega

- **P17. Pull con cursor = canon.** IDs determinísticos (TOID-compatibles),
  orden total, `coverage` declarado en cada respuesta paginada. Push
  (webhook/cola) = consumidores del mismo log, v2. [lección A1, Chainhook]
- **P18. Fachadas de compatibilidad valen oro** (la superficie que el
  ecosistema ya habla — hoy getEvents-like; mañana getEvents v2 cuando
  exista). [Umbra 10; watch stellar#1872]

### Observabilidad (marketing, no plomería)

- **P19. `/health` + `/ready` + `/status` + `/metrics` Prometheus** desde el
  commit uno. `/status` con cobertura por contrato. Un status page público
  (Gatus o similar) es la cara del proyecto — Creit y Obsrvr lo demuestran.
  [Ponder, TW, stellarindexer.com]
- **P20. Métricas que cuentan lo suprimido** (removals suprimidos, gaps
  abiertos, mensajes descartados): un guard silencioso cambia un fallo
  invisible por otro. [TW 5]

### Configuración y operación

- **P21. Dos niveles de config**: arranque (env: DATABASE_URL, NETWORK,
  ADMIN_TOKEN, RPCs) vs caliente (en la DB: contratos, tipos de dato,
  retención — sobrevive redeploys, se cambia por API/CLI/UI sobre el MISMO
  contrato admin autenticado).
- **P22. Cero config muerta, secretos redactados, validación al boot.** [TW 12]
- **P23. Recuperación como subcomandos**: `replay`, `reseed`, `rederive` —
  sin tocar el proceso vivo. [TW 10, Umbra 5]
- **P24. Resume automático es el default**; los fallos transitorios hacen
  backoff, no exit. [Ponder, Umbra 8]

### Calidad Go (objetivo #1 de Caleb)

- **P25. Paquetes por funcionalidad** (`internal/ingest`, `internal/source`,
  `internal/process`, `internal/store`, `internal/api`, `internal/health`,
  `internal/registry`) — el layout que TW y Umbra ya validaron.
- **P26. Interfaces definidas por el consumidor, chicas** (1-3 métodos), con
  el patrón nebu como techo de minimalismo.
- **P27. CI con dientes**: `-race`, govulncheck, gosec, staticcheck, gofmt,
  y snapshots de API pública si prometemos estabilidad. [nebu, deuda TW]
- **P28. Sin CGO** (binario estático, cross-compile trivial) — evitar la
  trampa zmq/sodium de cdp-pipeline-workflow.
- **P29. Context con deadline en toda llamada externa**; typed-nil y canales
  que "sanan" son bugs conocidos — ya los pagamos. [TW 3, A2]

---

## 4. Decisiones abiertas para el DESIGN.md

1. **Granularidad de la config de tipos de dato**: ¿por contrato ("de este
   quiero eventos+estado, de aquel solo transfers") o global por instancia?
   (wallet-backend es global; Umbra es por kind de contrato).
2. **¿Proto o JSON en el contrato interno de procesadores?** nebu usa
   `proto.Message` (tipado fuerte, versionable); Umbra usa structs+JSONB
   (simple). Proto paga si queremos registry de procesadores externos.
3. **Esquema del sobre propio** (inspirado en nebu meta/payload + el
   envelope 1.1 de TW): definir campos mínimos y política de versionado
   ANTES de la primera fila escrita.
4. **Naming de recursos de la API v1** (contracts, events, state, transfers,
   coverage) y formato de cursor (TOID vs compuesto propio).
5. **Qué taxonomía de wallet-backend adoptamos en v1** vs roadmap
   (eventos + contract state primero; SAC transfers/trustlines después —
   validar con ACTA).
6. **Relación con nebu/CDP**: ¿consumir sus processors como dependencia,
   implementar su contrato, o solo adoptar sus convenciones de sobre?
   → Inclinación tras la 2ª pasada: **convenciones sí, dependencias no**
   (ttp-processor-demo ya murió; flowctl/nebu pivotean rápido; lo estable es
   el ingest SDK oficial).
7. **Compatibilidad getEvents v2**: diseñar la API de eventos como
   superconjunto del proposal #1872 (filtros topic0-3, cursor opaco con
   query embebida, scanStatus) — decisión de bajo costo hoy, fachada casi
   gratis mañana. ¿Confirmar como requisito de v1?
8. **Contract data en vivo** (el diferenciador de Creit): ¿entra en la
   taxonomía v1 como cuarto recurso (`/state` con snapshot + historia de
   entries), o v1.x? Los event-indexers lo ignoran y la demanda está probada
   (caso Blend 35→1).

---

## 5. Registro de fuentes

- nebu: github.com/withObsrvr/nebu (README + pkg/processor leídos 2026-08-14, v0.6.11)
- cdp-pipeline-workflow: github.com/withObsrvr/cdp-pipeline-workflow
- wallet-backend: github.com/stellar/wallet-backend (internal/indexer)
- ingest SDK: stellar/go-stellar-sdk/ingest/doc.go
- Ponder: ponder.sh/docs/production/self-hosting + query/direct-sql
- graph-node: pruning docs + issue #5557
- Chainhook: github.com/hirosystems/chainhook + docs.hiro.so
- getEvents v2: github.com/orgs/stellar/discussions/1872 (ABIERTO — vigilar)
- Propios: trustlesswork-indexer-go (docs/pluggable-sink-architecture.md,
  docs/control-plane.md), umbra (docs/DESIGN.md, docs/ARCHIVE-BACKFILL.md)
- Paisaje competitivo: canal #indexers del Discord de Stellar (capturas
  2025-06→2026-08) — resumen en memoria oss-indexer-project.md
