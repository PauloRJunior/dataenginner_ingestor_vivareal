# Ingestão de dados, via site Vivareal

# Objetivo
  Obter informações de anúncios de imóveis para alugar em capitais dos estados brasileiros e apresetar indicadores referente ao assunto via Power BI.

# Método de Execução
  Foi criado um script Python que executa a busca via API ao site,trazendo informações pertinentes ao assunto. Por se tratar de uma API gratuita, ela apresenta limitações, onde é possível apenas consultar em média 10 mil anúncios por requisição.
  O site também apresenta uma característica, a repetição de anúncios pagos, onde o mesmo é apresentado em várias páginas e portanto pode se repetir o mesmo anúnico, para dibrar essa caracteristíca, foi acrescentado um tratamento de dados, para , somente trazer, valores únicos.

# Melhorias
  O script é um projeto inicial, portanto existem n melhorias a serem executados, na parte do script, estrutura e conceitos.

