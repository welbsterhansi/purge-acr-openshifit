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

Suite: **139 testes passando**. Todos os goals P1–P5 + hardening + features adicionais completos.

### Implementado

- **P1** `delete_manifest` retenta em 429 com backoff exponencial (`MAX_RETRIES = 3`)
- **P2** `analyze_all_repos` analisa repos em paralelo via `ThreadPoolExecutor`
- **P3** `_load_all` escaneia namespaces em paralelo; `_add_active`/`_add_historical` protegidos por `threading.Lock`
- **P4** `_print_candidates_table` aceita `max_rows=50`; exibe rodapé `"... and N more images"` quando truncada
- **P5** `delete_all_candidates` deleta em paralelo com `concurrency=10`
- **CLI** todos os parâmetros validados e testados: `--dry-run`, `--auto-approve`, `--keep`, `--max-age-days`, `--prefix`, `--registry`, `--skip-openshift`, `--in-cluster`
- **`--protected-tags`** tags nunca deletadas independente da idade (ex: `latest,stable,production`)
- **Output moderno** cabeçalhos com `◈`, tabelas sem bordas, alinhamento por f-strings, sem `===`
- **OpenShift** digest não encontrado no cluster exibe `✅ [not in cluster — safe to delete]`
- **Thread safety** hardening em `_load_all` e `delete_all_candidates` com stress tests

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
| `--in-cluster` | `false` | Carrega kubeconfig de dentro do cluster |
| `--protected-tags` | `""` | Tags nunca deletadas (ex: `latest,stable,production`) |

---

## Ordem de execução recomendada

```
oc adm prune images --keep-tag-revisions=10 --keep-younger-than=60m --confirm
          ↓
python3 purge.py --registry <r> --prefix <p> --keep 10 --dry-run true
          ↓
python3 purge.py --registry <r> --prefix <p> --keep 10 --dry-run false
          ↓
az acr gc --registry <r>
```

---

## Restrições

- **Não adicionar features** sem alinhamento
- **TDD obrigatório** — nenhum código de produção sem teste RED primeiro
- Relatórios JSON/CSV não devem ser afetados por mudanças de output
- Manter compatibilidade com todos os parâmetros CLI listados acima
