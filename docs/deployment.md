# Deploy

## Ordem recomendada

1. Confirme que o `TRAYadaptor` responde em `/health` e `/health/tray`.
2. Crie um PostgreSQL/Supabase separado para o NS BI.
3. Publique o backend; o container aplica `alembic upgrade head` antes do Uvicorn.
4. Configure `TRAY_ADAPTOR_URL` e `TRAY_ADAPTOR_TOKEN` com os mesmos valores
   usados pelo NSAgent.
5. Configure login, CORS e segredos do BI.
6. Publique o frontend somente com `VITE_BI_API_URL`.
7. Execute `POST /api/v1/sync/all`. A primeira execução importa o histórico disponível e as seguintes continuam pelo cursor salvo.
8. Confira `/api/v1/sync/status` e `/api/v1/data-quality`.

## Backend

Variáveis obrigatórias:

- `DATABASE_URL`
- `TRAY_ADAPTOR_URL`
- `TRAY_ADAPTOR_TOKEN`
- `JWT_SECRET`
- `AUTH_ADMIN_USERNAME` e `AUTH_ADMIN_PASSWORD`
- `CORS_ORIGINS`

O CORS deve conter apenas os domínios reais do frontend. Nunca registre o token
do TrayAdaptor, senhas ou payloads com dados pessoais em logs.

## Frontend

Configure apenas `VITE_BI_API_URL`. Não coloque chaves em variáveis Vite,
porque elas são incorporadas ao bundle público.

## Verificação

```bash
python -m pytest -q
python scripts/export_openapi.py
```

No frontend:

```bash
npm ci
npm test
npm run build
```
