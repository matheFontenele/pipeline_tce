# Proveniência dos catálogos TCE-CE

Os arquivos em `catalog/` são cópias controladas dos catálogos do repositório `/home/matheus/automacao_api_tse`, branch `main`, commit `df28d12fe8979fd8d8013a061a3fbb95d95a3359` (inspecionado em 2026-10-29). O repositório de origem permaneceu intacto.

| Arquivo | Conteúdo | SHA-256 da cópia e da origem |
|---|---|---|
| `endpoints.json` | 21 endpoints, prefixos, frequências e parâmetros; também contém metadados históricos de UI que a plataforma pode não consumir. | `1f1143acfce5b7129d0362bb71b6ba6cfc0a5d2b479b99f1e95f5f54a886da8a` |
| `municipios.json` | 185 códigos e nomes de municípios, incluindo código `001` (T.C.M.). | `20df5a85caf828954346ee3401558b52b33c9e031490665586cb97da563909a5` |

As cópias não são sincronizadas em runtime. Toda alteração futura deve atualizar a cópia com revisão do diff, data/hash e commit de origem, e incluir testes de compatibilidade. O código rejeita hosts fora do domínio da API do TCE e falha cedo para frequência/parâmetros não implementados.

Nota de compatibilidade: o catálogo contém o prefixo legado `controle_besn_notas_empenho` (grafia existente na origem). Foi preservado sem correção automática porque o prefixo participa da nomenclatura dos arquivos. Qualquer correção é mudança de contrato e requer migração compatível.
