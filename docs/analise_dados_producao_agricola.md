Análise de Dados da Produção Agrícola
Descrição

Este documento apresenta análises realizadas na base fato_producao, com foco em identificação de tendências, principais indicadores e validação da qualidade dos dados.

As consultas foram desenvolvidas em SQL e contemplam análises temporais, ranking de produtos e detecção de inconsistências.

a) Preço médio mensal e variação percentual

Foi calculado o preço médio mensal por commodity, incluindo a variação percentual em relação ao mês anterior utilizando a função de janela LAG.

Essa análise permite acompanhar a evolução dos preços ao longo do tempo e identificar tendências de crescimento ou queda.

b) Top 5 produtos mais negociados no último ano

Foram identificados os 5 produtos com maior volume no último ano disponível na base.

Essa análise permite destacar as commodities mais relevantes em termos de produção ou valor.

c) Identificação de registros anômalos

Foram aplicadas validações para identificar possíveis inconsistências nos dados, incluindo:

Valores negativos em métricas (área, produção e valor)
Valores fora de faixa definidos por regra de negócio
Outliers identificados por percentil (P95)
Rendimento produtivo considerado inconsistente

Essas verificações têm como objetivo garantir a confiabilidade das análises realizadas.

d) Contagem de anomalias por tipo

Foi realizada a consolidação das anomalias identificadas, permitindo quantificar os diferentes tipos de inconsistência presentes na base.

Essa análise auxilia na avaliação da qualidade geral dos dados.

e) Resumo estatístico por commodity

Foram calculadas métricas descritivas por commodity, incluindo:

Valor mínimo
Valor máximo
Média
Desvio padrão

Essa análise permite compreender a distribuição dos dados e apoiar a identificação de padrões e possíveis anomalias.

Otimização e Performance

As consultas foram avaliadas considerando seu impacto de performance.

Foram sugeridos:

Índices nas colunas utilizadas em filtros e agrupamentos (id_commodity, ano, mes)
Índice composto para melhorar consultas analíticas
Uso de EXPLAIN ANALYZE para avaliação de execução

Essas ações visam reduzir varreduras completas na tabela e melhorar o tempo de resposta das consultas.

Conclusão

As análises realizadas permitem identificar tendências relevantes na produção agrícola, além de garantir maior confiabilidade dos dados por meio de validações.

A aplicação de boas práticas de otimização contribui para a escalabilidade das consultas em cenários com maior volume de dados.