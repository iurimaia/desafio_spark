# Requisitos
1. Conversão do formato dos arquivos: Converter o arquivo CSV presente no diretório s3://test-cognitivo/data/input, para um formato colunar de alta performance de leitura de sua escolha. Justificar brevemente a escolha do formato;

2. Deduplicação dos dados convertidos: No conjunto de dados convertidos haverão múltiplas entradas para um mesmo registro, variando apenas os valores de alguns dos campos entre elas. Será necessário realizar um processo de deduplicação destes dados, a fim de apenas manter a última entrada de cada registro, usando como referência o id para identificação dos registros duplicados e a data de atualização (update_date) para definição do registro mais recente;

3. Conversão do tipo dos dados deduplicados: No diretório s3://test-cognitivo/config haverá um arquivo JSON de configuração (types_mapping.json), contendo os nomes dos campos e os respectivos tipos desejados de output. Utilizando esse arquivo como input, realizar um processo de conversão dos tipos dos campos descritos, no conjunto de dados deduplicados;

# Notas gerais
- Todas as operações devem ser realizadas utilizando Spark. O serviço de execução (EMR, Glue, Zeppelin, etc.) fica a critério do candidato. Justificar brevemente a escolha do serviço.

- Cada operação deve ser realizada no dataframe resultante do passo anterior, podendo ser persistido e carregado em diferentes conjuntos de arquivos após cada etapa ou executados em memória e apenas persistido após operação final.

- O arquivo ou o conjunto de arquivos finais devem ser persistidos no diretório s3://test-cognitivo/data/output.