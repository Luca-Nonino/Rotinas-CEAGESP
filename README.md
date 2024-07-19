# CEAGESP IPV GERADOR

Este projeto automatiza o processo de geração de arquivos IPV a partir de dados de cotação do CEAGESP. O fluxo de dados é dividido em várias etapas, cada uma realizada por scripts específicos.

## Estrutura do Projeto

A estrutura do projeto é organizada em diversas pastas principais, incluindo `data` e `scripts`. A pasta `data` contém os dados brutos, processados e logs, enquanto a pasta `scripts` contém os scripts Python que realizam as operações de processamento.

### Pasta `data`

- `data/raw`: Contém os arquivos de cotação brutos em formato Excel.
- `data/processed`: Contém os arquivos IPV gerados e um arquivo de referência de IDs (`id_reference.csv`).
- `data/logs`: Contém logs de erros e uma lista de arquivos processados (`processed_list.json`).

### Pasta `scripts`

- `email_sync.py`: Sincroniza os arquivos de cotação recebidos por e-mail com a pasta `data/raw`.
- `add_entry_reference.py`: Atualiza a lista de referência de IDs com novas entradas encontradas nos arquivos de cotação.
- `check_cod_ipv.py`: Verifica se todos os códigos na lista de referência estão presentes nos arquivos IPV.
- `file_conversion.py`: Converte os arquivos de cotação em arquivos IPV.
- `check_completion.py`: Verifica se todos os arquivos foram processados com sucesso.
- `clear_data.py`: Limpa os dados da pasta `data/raw` após o processamento.

## Configuração

- `config.json`: Arquivo de configuração que contém informações essenciais para a execução dos scripts, como credenciais de e-mail e parâmetros de conexão.

## Execução do Projeto

Para executar o projeto, siga os passos abaixo:

1. Execute `email_sync.py` para buscar e-mails e salvar os arquivos de cotação na pasta `data/raw`.
2. Execute `add_entry_reference.py` para atualizar a lista de referência de IDs com novas entradas.
3. Execute `file_conversion.py` para converter os arquivos de cotação em arquivos IPV.
4. Execute `check_cod_ipv.py` para verificar se todos os códigos na lista de referência estão presentes nos arquivos IPV.
5. Execute `check_completion.py` para verificar se todos os arquivos foram processados com sucesso.
6. Execute `clear_data.py` para limpar a pasta `data/raw` após o processamento.

## Logs de Execução

### Logs da Execução Quando Não Há Novos Boletins

        ```
        Executando scripts/email_sync.py...
        Script start.
        Connecting to server imap.hostinger.com
        Logged in, selecting inbox.
        Found emails, processing...
        File Cotacao 23.02.2024.xlsx is already processed. Skipping...
        File Cotacao 19.02.2024.xlsx is already processed. Skipping...
        File Cotacao 09.02.2024.xlsx is already processed. Skipping...
        File Cotacao 16.02.2024.xlsx is already processed. Skipping...
        File Cotacao 28.02.2024.xlsx is already processed. Skipping...
        File Cotacao 08.03.2024.xlsx is already processed. Skipping...
        File Cotacao 11.03.2024.xlsx is already processed. Skipping...
        File Cotacao 06.03.2024.xlsx is already processed. Skipping...
        Email processing complete.
        Script end.
        Executando scripts/check_completion.py...
        All files processed successfully.
        Pipeline concluído com sucesso.
        ```

### Logs da Execução Quando Há Novos Boletins

        ```
        Executando scripts/email_sync.py...
        Script start.
        Connecting to server imap.hostinger.com
        Logged in, selecting inbox.
        Found emails, processing...
        File Cotacao 23.02.2024.xlsx is already processed. Skipping...
        File Cotacao 19.02.2024.xlsx is already processed. Skipping...
        File Cotacao 09.02.2024.xlsx is already processed. Skipping...
        File Cotacao 16.02.2024.xlsx is already processed. Skipping...
        Saving attachment: Cotacao 28.02.2024.xlsx
        Attachment saved: Cotacao 28.02.2024.xlsx
        Updated processed_list with Cotacao 28.02.2024.xlsx.
        Saving attachment: Cotacao 08.03.2024.xlsx
        Attachment saved: Cotacao 08.03.2024.xlsx
        Updated processed_list with Cotacao 08.03.2024.xlsx.
        Saving attachment: Cotacao 11.03.2024.xlsx
        Attachment saved: Cotacao 11.03.2024.xlsx
        Updated processed_list with Cotacao 11.03.2024.xlsx.
        Saving attachment: Cotacao 06.03.2024.xlsx
        Attachment saved: Cotacao 06.03.2024.xlsx
        Updated processed_list with Cotacao 06.03.2024.xlsx.
        Email processing complete.
        Script end.
        Executando scripts/check_completion.py...
        scripts/check_completion.py failed with exit status 1. Continuing with the next script.
        Executando scripts/add_entry_reference.py...
        No new entries found.
        Executando scripts/file_conversion.py...
        Unprocessed files: [\data\raw\Cotacao 28.02.2024.xlsx', '\data\raw\Cotacao 08.03.2024.xlsx', '\data\raw\Cotacao 11.03.2024.xlsx', '\data\raw\Cotacao 06.03.2024.xlsx']
        Saving file to: scriptsdata\processed\IPVS\IPVS_28022024.csv
        Saving file to: scriptsdata\processed\IPVS\IPVS_08032024.csv
        Saving file to: scriptsdata\processed\IPVS\IPVS_11032024.csv
        Saving file to: scriptsdata\processed\IPVS\IPVS_06032024.csv
        Executando scripts/check_cod_ipv.py...
        All cod values from id_reference.csv are present in IPVS files.
        Executando scripts/clear_data.py...
        Pipeline concluído com sucesso.
        ```

## Finalidade

O objetivo deste projeto é automatizar a geração de arquivos IPV a partir de dados de cotação do CEAGESP, garantindo a precisão e eficiência do processo.
