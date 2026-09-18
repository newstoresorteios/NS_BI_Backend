# NS BI Backend

Backend analítico da New Store baseado no BI da XNaMai e conectado ao
`TRAYadaptor`. Ele sincroniza pedidos completos, itens, clientes, produtos,
categorias e usuários em um banco próprio, sem consultar a Tray durante a
navegação do dashboard.

## Recursos

- dashboard executivo, vendas, pedidos e ticket médio;
- produtos, estoque, curva ABC e oportunidades comerciais;
- clientes, retenção, LTV, coortes e CRM;
- vendedores, filtros globais e exportação;
- trilha de sincronizações e auditoria de qualidade;
- logística por pedido com modalidade, custo, rastreio, postagem, entrega,
  integrador, destino e estatísticas operacionais;
- sincronização incremental de pedidos e clientes;
- carga paginada de produtos, variantes, propriedades, categorias, marcas,
  clientes, endereços, usuários, kits, cupons, centros de distribuição e
  métodos de envio;
- detalhes completos de cada pedido, incluindo todos os itens.

## Execução local

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements-dev.txt
copy .env.example .env
uvicorn app.main:app --reload
```

Swagger: `http://localhost:8000/docs`.

## Configuração

Defina `DATABASE_URL`, `TRAY_ADAPTOR_URL`, `TRAY_ADAPTOR_TOKEN` e
`CORS_ORIGINS`. O token é o mesmo valor
de `TRAY_ADAPTER_TOKEN` configurado no serviço TrayAdaptor.

## Primeira carga

Depois de subir o backend e aplicar as migrações:

```bash
curl -X POST "https://SEU-BACKEND/api/v1/sync/all"
```

Durante a carga, acompanhe `/api/v1/sync/status`. A Tray limita o volume de
requisições; por isso, a primeira carga de pedidos completos pode levar tempo.
As cargas posteriores reaproveitam o cursor de alteração.

## Arquitetura

`Tray API -> TRAYadaptor -> NS BI Backend -> PostgreSQL -> NS BI Frontend`

As colunas internas que terminam em `mercos_id` foram preservadas nesta
primeira versão para manter compatibilidade com as análises e migrações do
modelo XNaMai. No projeto NS elas armazenam o identificador de origem da Tray.
