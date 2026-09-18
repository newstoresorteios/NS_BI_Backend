# Limitações conhecidas da fonte Tray

- A listagem de pedidos não contém todos os itens; o BI consulta o pedido
  completo antes de persistir linhas de produto.
- A primeira carga pode consumir muitas requisições e deve respeitar a cota da
  loja configurada no `TRAYadaptor`.
- Custo real, CMV, margem, comissão e lucro não são inferidos quando a Tray não
  fornece esses valores de forma factual.
- Estoque é uma fotografia do catálogo no momento da sincronização; ainda não
  há histórico de snapshots por centro de distribuição.
- O vínculo de vendedor depende dos campos disponíveis no pedido. Quando a
  origem não informa um usuário responsável, o pedido aparece sem vendedor.
- Pedidos aguardando pagamento não entram como venda válida. Pedidos pagos,
  aguardando envio, enviados e finalizados entram no faturamento.
- Cobertura e risco de ruptura são estimativas baseadas no histórico observado
  e não substituem previsão de demanda.
- Os payloads normalizados ficam em `raw` para auditoria. Campos ausentes não
  são inventados.
