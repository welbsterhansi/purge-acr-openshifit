# ACR Purge — CLAUDE.md

## Workflow

**TDD obrigatório.** Nenhum código de produção sem teste falhando primeiro.

```
RED → Verify RED → GREEN → Verify GREEN → REFACTOR
```

- Escrever o teste → rodar → confirmar que falha pelo motivo certo
- Implementar o mínimo para passar
- Rodar a suite completa antes de considerar done

**Regra de validação obrigatória:** a cada avanço (GREEN, REFACTOR, ou conclusão de qualquer goal),
rodar `python3 -m pytest test_purge.py -v` e confirmar que **todos os testes anteriores continuam passando**.
Nenhum step é considerado done se a suite regredir.

```bash
python3 -m pytest test_purge.py -v
```

---

## Estado atual

Suite: **259 testes passando** (`test_purge.py` + `test_openshift_prune.py`). Todos os goals P1–P5 + hardening + features adicionais completos.

```bash
python3 -m pytest test_purge.py test_openshift_prune.py -v
```

### Implementado

- **P1** `delete_manifest` retenta em 429 com backoff exponencial (`MAX_RETRIES = 3`)
- **P2** `analyze_all_repos` analisa repos em paralelo via `ThreadPoolExecutor`
- **P3** `_load_all` escaneia namespaces em paralelo; `_add_active`/`_add_historical` protegidos por `threading.Lock`
- **P4** `_print_candidates_table` aceita `max_rows=50`; exibe rodapé `"... and N more images"` quando truncada
- **P5** `delete_all_candidates` deleta em paralelo com `concurrency=10`
- **CLI** todos os parâmetros validados e testados: `--dry-run`, `--auto-approve`, `--keep`, `--max-age-days`, `--prefix`, `--registry`, `--skip-openshift`, `--in-cluster`, `--protected-tags`
- **`--protected-tags`** tags nunca deletadas independente da idade (ex: `latest,stable,production`)
- **Output moderno** cabeçalhos com `◈`, tabelas sem bordas, alinhamento por f-strings, sem `===`
- **PHASE 2 labels** distingue origem: `[running in pod]`, `[referenced in workload]`, `[in imagestream]`
- **Thread safety** hardening em `_load_all` e `delete_all_candidates` com stress tests

### OpenShift (`openshift_client.py`)

Resources escaneados como `_active` (execução real):
- Pods (`container_statuses[].image_id`)
- Deployments, StatefulSets, DaemonSets, ReplicaSets (containers + init_containers)
- DeploymentConfigs (containers + init_containers)
- ReplicationControllers (containers + init_containers — protege rollback de DC)
- Jobs, CronJobs
- Builds (output + input via `strategy.*.from`)
- BuildConfigs (input via `strategy.*.from`)

Resources escaneados como `_historical` (existência no catálogo, não execução):
- ImageStream — todos os items, incluindo `items[0]`
- ImageStreamTags — imagem resolvida de cada tag

> **Decisão de design:** ImageStream/IST marcam presença no registry, não execução.
> Proteção real vem de pods e workloads. Evita que release tags antigas (ex: `1.0.0-release-20231214`)
> apareçam como `[running]` quando nenhum pod as usa.

### Detecção de cluster (`openshift_client.py`)

**Premissa fundamental:** o app detecta automaticamente o cluster em que o usuário está logado (`oc login`). Não existe prefixo de namespace hardcoded — `namespace_prefix=""` sempre, escaneando todos os projetos acessíveis via Projects API.

**Por que Pod é a fonte mais confiável:**
Workloads (Deployment, RC, etc.) referenciam imagens por tag (`app:v1.2.3`), não por digest. O campo `status.containerStatuses[].imageID` do Pod contém o digest real em execução. Se `list pods` retornar 403 em algum namespace, os digests daquele namespace não entram em `_active` — o scan fica parcial.

**`permission_incomplete`:** quando qualquer namespace retorna 403/401, a flag é setada silenciosamente e um aviso é exibido ao final do PHASE 2. O app **não bloqueia** as deleções — imagens encontradas em qualquer namespace continuam protegidas, imagens não encontradas continuam deletáveis. Isso é intencional: bloquear tudo em vez de avisar causava 0 deleções mesmo com 6000+ candidatos válidos.

> **Risco:** se o service account não tem `list pods` no namespace da aplicação, imagens em execução podem aparecer como `[not in cluster — safe to delete]`. Garantir RBAC correto é responsabilidade do operador.

### OpenShift (`openshift_prune.py`)

Script separado para prune do histórico de ImageStream. Útil para times que usam **poucos tags com muitas revisões** (ex: `latest` atualizado semanalmente).

> Para times com **uma tag por release**, usar `oc adm prune images` primeiro para limpar tags antigas.

---

## Parâmetros CLI

| Parâmetro | Padrão | Descrição |
|---|---|---|
| `--registry` | obrigatório | Nome do ACR (ex: `bdsoregistry`) |
| `--prefix` | obrigatório | Prefixo dos repos a escanear |
| `--keep` | `2` | Número de imagens mais recentes a preservar por repo |
| `--max-age-days` | `15` | Idade mínima em dias para candidatura à deleção |
| `--dry-run` | `true` | `true` = simulação \| `false` = deleta de verdade |
| `--auto-approve` | `false` | Pula confirmação interativa (usar em pipelines) |
| `--skip-openshift` | `false` | Pula verificação de cluster |
| `--in-cluster` | `false` | Carrega kubeconfig de dentro do cluster (pipeline pods) |
| `--protected-tags` | `""` | Tags nunca deletadas (ex: `latest,stable,production`) |

> **Removido:** `--namespace-prefix` foi removido intencionalmente. O cluster é detectado dinamicamente pelo contexto `oc login` ativo — não existe e não deve existir filtro por prefixo de namespace em `purge.py`.

---

## Ordem de execução recomendada

**Manual (desenvolvedor logado no cluster):**
```
oc adm prune images --keep-tag-revisions=10 --keep-younger-than=60m --confirm
          ↓
python3 purge.py --registry <r> --prefix <p> --keep 10 --dry-run true
          ↓
python3 purge.py --registry <r> --prefix <p> --keep 10 --dry-run false
          ↓
az acr gc --registry <r>
```

**Pipeline (pod dentro do cluster):**
```
python3 purge.py \
  --registry <r> \
  --prefix <p> \
  --keep 10 \
  --dry-run false \
  --auto-approve \
  --in-cluster
```

**Pré-requisitos de RBAC para o service account do pipeline:**
- `list`, `get` em `pods` em todos os namespaces de aplicação
- `list`, `get` em `deployments`, `replicationcontrollers`, `statefulsets`, `daemonsets`, `replicasets`, `jobs`, `cronjobs`
- `list`, `get` em `deploymentconfigs`, `builds`, `buildconfigs`, `imagestreams`, `imagestreamtags` (OpenShift)
- `list` em `projects` (para descobrir namespaces acessíveis)

---

## Restrições

- **Não adicionar features** sem alinhamento
- **TDD obrigatório** — nenhum código de produção sem teste RED primeiro
- Relatórios JSON/CSV não devem ser afetados por mudanças de output
- Manter compatibilidade com todos os parâmetros CLI listados acima
