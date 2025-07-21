# Rinha de Backend - 2025
Implementação da [**Rinha de Backend 2025**](https://github.com/zanfranceschi/rinha-de-backend-2025) utilizando Rust e a biblioteca de coroutines May.

## Arquitetura
- 1 loadbalancer - HAProxy
- 2+ web servers - Rust (May + Redis)
- 2+ worker - Rust (May + Redis)
- 1 cache - Redis

## Objetivos dessa implementação
- [ ] Tempos de resposta da API < 2ms
    - [X] Localmente
    - [ ] Nos testes oficiais
- [X] Aprender sobre implementação de sistemas distribuidos e filas
- [X] Aprender sobre coroutines
- [X] Aprender sobre performance tunning no postgres
- [X] Aprender sobre performance tunning no haproxy
- [X] Aprender sobre scripts no redis
- [X] Aprender sobre named lifetimes no rust
- [X] Aprender sobre pointer handling no rust
