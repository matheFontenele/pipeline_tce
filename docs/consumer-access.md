# Acesso ao lakehouse — instruções para o app consumidor

Documento de handoff para o agente/equipe responsável pelo app que lê os
dados do `pipeline_tce`. Vale para o ambiente atual (servidor
`controle-interno-H81`, `172.16.0.200`): dados abertos do TCE-CE extraídos da
API pública e gravados em Parquet no lakehouse local.

## 1. Endereço

Leitura direta do filesystem, no próprio servidor (app roda como usuário
`controle-interno`):

```
/home/controle-interno/pipeline_tce/lakehouse
```

Layout dos datasets:

```
lakehouse/bronze/<endpoint>/<ano>/<arquivo>.parquet
lakehouse/bronze/<endpoint>/<ano>/<arquivo>.parquet.manifest.json
```

Nome do arquivo: `<endpoint>_<ano>_<mês>_<município>.parquet`

- mês com 2 dígitos (ex.: `02`); endpoints anuais não têm mês;
- município com 3 dígitos (ex.: `057` = Fortaleza);
- `endpoint` é o prefixo do catálogo (`notas_empenho`, `notas_pagamentos`, …).

Exemplo real disponível hoje:

```
/home/controle-interno/pipeline_tce/lakehouse/bronze/notas_empenho/2024/notas_empenho_2024_02_057.parquet
/home/controle-interno/pipeline_tce/lakehouse/bronze/notas_empenho/2024/notas_empenho_2024_02_057.parquet.manifest.json
```

O app **não** acessa o Postgres de estado, o container do Airflow nem o volume
Docker antigo. A pasta acima é o único contrato de leitura.

## 2. Manifesto é o commit

Cada Parquet tem um manifesto JSON ao lado. Ele é gravado **depois** do
Parquet; portanto, Parquet sem manifesto (ou com manifesto de outro sha256)
não deve ser consumido.

Campos:

| Campo | Uso pelo app |
|---|---|
| `partition` | `endpoint_prefix`, `year`, `month` (nulo em anuais), `municipality_code`, `filename` |
| `file` | nome do Parquet |
| `content_sha256` | hash do Parquet; use como versão do dado |
| `size_bytes` | tamanho esperado do arquivo |
| `row_count` | linhas esperadas |
| `columns` | colunas e ordem esperadas |
| `schema_version` | versão do esquema da partição (`"1"` hoje) |
| `contract_version` | versão do contrato de validação (`null` enquanto não publicado) |
| `format_version` | versão do formato do manifesto (`1`) |
| `run_id` | execução do Airflow que gerou o artefato |
| `created_at` | instante UTC da gravação |

## 3. Regras obrigatórias

1. Ler **somente** `lakehouse/bronze/**`.
2. Ignorar `lakehouse/_staging/**`, arquivos `*.tmp` e qualquer caminho que
   comece com `_` ou `.`.
3. Abrir o Parquet apenas se o manifesto existir e:
   - `content_sha256` do arquivo conferir com o manifesto;
   - `row_count` e `columns` do manifesto baterem com o Parquet lido.
4. Se a conferência falhar, **não** usar o arquivo: aguarde a próxima
   execução e tente novamente (pode ser substituição em andamento).
5. Abrir sempre em modo somente leitura. O app nunca escreve, move ou apaga
   nada dentro de `lakehouse/`.
6. Não deduzir atualidade por data de modificação do arquivo; use
   `content_sha256` + `created_at` do manifesto.

## 4. Fluxo de carga recomendado

```python
import json
from pathlib import Path

import pyarrow.parquet as pq

LAKEHOUSE = Path("/home/controle-interno/pipeline_tce/lakehouse")


def ler_particao(endpoint: str, ano: int, mes: int | None, municipio: str):
    nome = f"{endpoint}_{ano}_" + (f"{mes:02d}_" if mes else "") + f"{municipio.zfill(3)}.parquet"
    parquet_path = LAKEHOUSE / "bronze" / endpoint / str(ano) / nome
    manifest_path = parquet_path.with_suffix(parquet_path.suffix + ".manifest.json")

    if not manifest_path.is_file():
        return None  # artefato ainda não commitado

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    file = pq.ParquetFile(parquet_path)

    if file.metadata.num_rows != manifest["row_count"]:
        raise RuntimeError(f"linhas divergem do manifesto: {parquet_path}")
    if list(file.schema_arrow.names) != manifest["columns"]:
        raise RuntimeError(f"colunas divergem do manifesto: {parquet_path}")

    return file.read().to_pandas()  # ou processe em batches
```

Conferência de integridade por linha de comando:

```bash
cd /home/controle-interno/pipeline_tce/lakehouse/bronze/notas_empenho/2024
sha256sum -c <(python3 - <<'PY'
import json
m = json.load(open("notas_empenho_2024_02_057.parquet.manifest.json"))
print(f'{m["content_sha256"]}  {m["file"]}')
PY
)
```

## 5. Descoberta de partições novas

Não há fila nem endpoint de notificação. Para descobrir o que existe, varra os
manifestos (poucos milhares de arquivos no total):

```bash
find /home/controle-interno/pipeline_tce/lakehouse/bronze -name '*.manifest.json'
```

Se o app precisar de carga incremental, guarde o `content_sha256` já
processado por partição e reprocesse apenas quando ele mudar.

## 6. Cobertura atual

- Histórico completo de 2008 a 2025, 2026/01–09 completo, 2026/10 em
  preenchimento, 2026/11 sem publicação na API e 2026/12 inexistente.
- Volume: 828 mil Parquets / ~13,6 GB, no layout
  `bronze/<endpoint>/<ano>/<arquivo>.parquet`.
- Estado do pipeline: `extracted` (bronze conferido contra o manifesto),
  **sem** validação contratual/qualidade e **sem** publicação formal.
- Atualização: DAG `tce_atualizacao_diaria` reextrai mês corrente + anterior
  todos os dias às 02:30 `America/Fortaleza`; fora dessa janela o histórico é
  imutável.

Consumo liberado para desenvolvimento e operação interna. O gate de publicação
da Fase 2 (contrato versionado + checks de qualidade) ainda não foi
implementado; quando entrar, `contract_version` deixará de ser `null` e haverá
uma pasta publicada separada. A árvore do extrator antigo
(`~/automacao_api_tse/data`) não é mais fonte de dados — ver
[`handover-extrator-legado.md`](handover-extrator-legado.md).

## 7. Permissões

- Os arquivos são gravados com modo `0644`; diretórios `0755`/`0775`.
- O app roda como `controle-interno`, dono do diretório — leitura garantida.
- Se o app mudar de usuário, será necessário liberar a travessia do home
  (`/home/controle-interno` é `750`) com ACL.

## 8. O que não fazer

- Não ler `_staging` nem arquivos temporários.
- Não assumir que todo Parquet existente é válido: valide o manifesto.
- Não abrir conexão com o Postgres `pipeline_state` nem com o banco do
  Airflow.
- Não montar o volume Docker `pipeline-tce_airflow_pipeline_data` — ele é
  apenas backup da migração.
- Não escrever, mover nem apagar arquivos no lakehouse.
- Não depender de ordem de colunas diferente da informada em `columns`.

## 9. Suporte

Dúvidas sobre layout, esquema ou partições ausentes: responsável pelo
`pipeline_tce`. Alterações de contrato (nome de arquivo, manifesto, layout)
serão acompanhadas de atualização deste documento e de `schema_version` ou
`format_version`.
