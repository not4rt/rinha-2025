# Rinha de Backend - 2025
Implementação da [**Rinha de Backend 2025**](https://github.com/zanfranceschi/rinha-de-backend-2025) utilizando Rust e a biblioteca de coroutines May.

## Arquitetura
- 1 loadbalancer - haproxy
- 2+ web servers - Rust utilizando May e May-postgres
- 1+ worker - Utilizando May-postgres, é possível rodar em background em conjunto com o server, porém para manter o tempo de resposta da API < 2ms, é melhor manter ele separado em um container diferente.
- 1 banco de dados postgres

## Objetivos dessa implementação
- [ ] Tempos de resposta da API < 2ms
    - [X] Localmente
    - [ ] Nos testes oficiais
- [X] Aprender sobre implementação de sistemas distribuidos e filas
- [X] Aprender sobre coroutines
- [X] Aprender sobre performance tunning no postgres
- [X] Aprender sobre performance tunning no haproxy
- [X] Aprender sobre rust channels
