# Dicionário de métricas

- **Pedido**: registro de pedido recebido da Tray, independentemente do
  status.
- **Venda válida**: pedido pago, aguardando envio, enviado ou finalizado, cujo
  status interno normalizado é `order`.
- **Cancelamento**: pedido cujo status pertence a `0`, `5`, `cancelled` ou
  `cancelado`. A regra canônica está em `app/domain/order_status.py`.
- **Faturamento bruto**: soma dos preços originais dos itens da Tray antes dos
  descontos, quando disponíveis; caso contrário, usa o total do pedido.
- **Faturamento líquido**: soma dos totais históricos dos pedidos válidos da
  Tray. Em recortes por produto ou categoria, soma os subtotais dos itens.
- **Ticket médio**: faturamento líquido dividido pela quantidade de vendas
  válidas.
- **Desconto total**: diferença entre preço original e preço vendido dos itens,
  ou o desconto informado no cabeçalho quando a Tray o disponibiliza.
- **Taxa de cancelamento**: cancelamentos divididos por todos os pedidos nos
  mesmos filtros.
- **Compradores únicos**: clientes distintos com venda válida.
- **Novo comprador**: cliente cuja primeira venda válida ocorreu no período.
- **Comprador recorrente**: cliente comprado no período cuja primeira venda
  válida ocorreu antes dele.
- **Quantidade de itens**: soma de linhas de item registradas no cabeçalho.
- **SKUs**: soma dos produtos distintos por pedido.
- **Curva ABC**: participação acumulada no faturamento: A até 80%, B até 95%,
  C acima de 95%.
- **RFM**: notas 1–5 de recência, frequência e valor monetário entre os
  clientes contidos nos filtros.
- **Coorte de clientes**: grupo definido pelo mês da primeira compra observada
  dentro do recorte selecionado.
- **Retenção M+N**: percentual dos clientes de uma coorte que realizou ao
  menos uma compra no enésimo mês após a primeira compra observada. Coortes
  que ainda não completaram esse tempo não entram na taxa consolidada.
- **LTV observado**: faturamento analítico acumulado dividido pelos clientes da
  coorte ou do conjunto analisado. É um valor realizado no histórico disponível,
  não uma projeção de receita futura.
- **Velocidade média**: quantidade vendida dividida pelos dias do período.
- **Cobertura estimada**: estoque atual dividido pela velocidade média.
  É indisponível para período `all` ou produto sem venda.
- **Valor de estoque**: estoque atual multiplicado pelo preço de tabela. Não é
  custo, CMV, margem ou lucro.

Todos os valores monetários persistidos usam `Numeric`/`Decimal`. A API pode
serializá-los como números JSON, mas nunca calcula totais globais no navegador.

## Reconciliação SQL

O faturamento analítico sem filtros adicionais deve fechar com:

```sql
SELECT COALESCE(SUM(COALESCE(oi.total, oi.quantity * oi.unit_price)), 0) AS net_revenue
FROM order_items oi
JOIN orders o ON o.mercos_id = oi.order_mercos_id
WHERE LOWER(TRIM(o.status)) IN ('2', 'pedido', 'order')
  AND COALESCE(oi.excluded, false) = false
  AND o.issued_at >= :date_from_utc
  AND o.issued_at < :date_to_exclusive_utc;
```

As datas recebidas pelo BI são convertidas de `America/Sao_Paulo` para UTC. O
limite final é exclusivo. O valor histórico do cabeçalho (`orders.total`)
permanece no relatório `GET /api/v1/data-quality` para auditoria; ele não
compõe mais o faturamento das telas.
