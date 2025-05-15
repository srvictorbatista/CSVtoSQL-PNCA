
<p align="center">
    <a href="#PNCA">
        <img src="https://i.postimg.cc/hjnK7xHV/stand.png" alt="CSV-SQL PNCA">
    </a>
</p>
<h1 align="center">PNCA: Script de conversão de CSV para SQL</h1>
<h2>WANTED: No pandas, no bamboo. Just trees and data. Because pandas don't eat trees.</h2>

**Contexto e Significado**: O subtítulo faz uma brincadeira sobre um "Panda foragido" enfatizando a ausência de "bambu". Referência ao fato de que pandas (os ursos) só comem bambu, mas esse projeto, para fins de conceito e didática. Evita a biblioteca Pandas (que trata os dados de forma vertical, como um bambu, que por sinal, é um tipo de grama/relva). A expressão "Just trees and data" deixa claro que o foco é em manipulação de dados, sem o uso da biblioteca Pandas. Até porque como veremos, a abordagem proposta (algoritmo de manipulação e classificação) se estende para além da linguagem python. Como exemplo adicional (mas não limitando-se a ela), veremos um exemplo em PHP.

**Descrição do Projeto**: O projeto recebe dados brutos, analisa e gera scripts SQL, compatível com bancos de dados MySQL e Postgres. Ele se baseia em árvores de decisão para manipulação, analise e classificação de dados estruturais para lidar com os dados, ao invés de recorrer a biblioteca "Pandas".
O produto final é um script SQL compatível de forma rápida e automática. Que pode ser tanto subdividido em lotes, sem perder a atomicidade (o que lhe permite ajustar a saída à carga suportada do servidor), auditável (devido a sua fácil leitura, edição, endentação e compreensão).
A saída também incorpora um **"CREATE IF NOT EXISTS"** estruturado de forma automática, recursiva e reversível. Portanto, segura. Garantindo que nenhum dado se perca, durante todo o processo. Mais detalhes de como isto ocorre, veremos no decorrer desta proposta.

**Ambiente**: O ambiente utilizado para desenvolvimento e testes foi **Python 3.10.11** e **PHP 8.2.28**. 
Se preferir uma versão completa e pronta para uso, de todo o ambiente utilizado, em um único container docker. Você poderá usar o projeto **[TSMX-ETL](https://github.com/srvictorbatista/tsmx-etl)** ofertado pelo mesmo autor.



## 🧠 Detecção Automática de Tipo de Coluna (análise sintática)

### 📌 detect_column_type(); 
**Objetivo**
Estas funções têm como objetivo **analisar uma amostra de dados de uma coluna** (lista de valores) e **inferir automaticamente o tipo de dado** mais adequado (por exemplo: inteiro, texto, booleano, data, etc.), considerando diferentes SGBDs (MySQL e PostgreSQL).



### 🧮 Árvores de Decisão

📄 Esta função simula uma árvore de decisão de forma implícita:

* Cada condicional representa um nó de decisão;
* As regras são aplicadas em sequência, classificando cada valor;
* A contagem final determina a classe "vencedora" por peso.

Trata-se de uma árvore de decisão, onde as regras são explícitas, em vez de amostrais. 

---
_Ambas as funções são uteis e eficientes para inferir (pesar) e classificar tipos de dado, especialmente em processos de **importação de dados (ETL) auditáveis**, em **transações atomizadas** em blocos de **tamanho regulável**. Embora a versão da função em Python ofereça uma detecção mais robusta, se considerando exclusivamente, o algoritmo  de classificação dos pesos. A opção em PHP é menos verbosa e direta. O que **não significa**, menos eficaz ou mais lenta. Especialmente se considerarmos a escalabilidade, portabilidade e versatilidade das aplicações em ambos os casos._

Isto sem falar que uma vez entendendo a lógica usada para a classificação. Esta poderá ser facilmente adaptada para atender quaisquer atividades, eventualmente não mapeadas. Por isso, vamos a lógica:

--- 
<br> &nbsp;

### 🐍 Versão em Python: `detect_column_type(values, sgbd)`

#### Descrição:

```python
def detect_column_type(values, sgbd):
```

* Parametros:

  * `values`: uma lista de strings representando os valores de uma coluna;
  * `sgbd`: o nome do Sistema Gerenciador de Banco de Dados (ex: 'mysql', 'postgres').

```python
    bool_set = {'true', 'false', 'yes', 'no', 'sim', 'não', '0', '1'}
```

* Define um conjunto de strings representando possíveis valores booleanos.

```python
    non_empty_values = [v.strip().lower() for v in values if v.strip()]
```

* Remove espaços e ignora valores vazios, convertendo tudo para minúsculo.

```python
    type_balance = {'boolean': 0, 'date': 0, 'integer': 0, 'float': 0, 'text': 0}
```

* Inicializa um dicionário que servirá como "placar" de pesos, para contar quantas vezes cada tipo foi detectado.

```python
    for v in non_empty_values:
```

* Itera sobre os valores não vazios.

```python
        if v in bool_set:
            type_balance['boolean'] += 1
```

* Se o valor está no conjunto booleano, conta como booleano.

```python
        elif v.lstrip('0') != v and len(v) > 1:
            type_balance['text'] += 1
```

* Detecta possíveis números com zero à esquerda (como "0123"), classificando como texto.

```python
        elif v.replace(',', '.').replace('.', '', 1).isdigit():
            type_balance['float' if '.' in v or ',' in v else 'integer'] += 1
```

* Verifica se o valor é numérico (inteiro ou decimal).

```python
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', v):
            type_balance['date'] += 1
```

* Verifica se o valor está no formato ISO de data (AAAA-MM-DD).

```python
        else:
            try:
                parse_date(v, dayfirst=True)
                type_balance['date'] += 1
            except:
                type_balance['text'] += 1
```

* Tenta parsear como data (ex: "dd/mm/YYYY"). Se falhar, assume que é texto.

```python
    best_type = max(type_balance, key=type_balance.get) if len(non_empty_values) == 0 or type_balance[max(type_balance, key=type_balance.get)] / len(non_empty_values) >= 0.8 else 'text'
```

* Define o tipo final com base no tipo que obteve mais "saldo/peso" e se for representativo (>=80% dos dados).

```python
    return {
        'mysql': {'boolean': 'BOOLEAN', 'date': 'DATE', 'integer': 'INT', 'float': 'FLOAT', 'text': 'TEXT'},
        'postgres': {'boolean': 'BOOLEAN', 'date': 'DATE', 'integer': 'INTEGER', 'float': 'REAL', 'text': 'TEXT'}
    }.get(sgbd, {}).get(best_type, 'TEXT')
```

* Mapeia o tipo genérico para o tipo real do SGBD. Se não reconhecido, retorna TEXT como fallback (medida de segurança). Que também atuará em caso de falha na classificação evitando a perda de dados.

---



### 🐘 Versão em PHP: `detect_column_type($values, $SGBD)`

#### Descrição:

```php
function detect_column_type($values, $SGBD) {
```

* Define parametros:

  * `$values`: array de valores da coluna;
  * `$SGBD`: tipo do banco de dados ('mysql', 'postgres').

```php
    $isNumeric = true;
    $isText = false;
```

* Flags para sondar se os valores são numéricos e/ou texto.

```php
    foreach ($values as $value){
```

* Itera sobre todos os valores.

```php
        if (is_numeric($value)) continue;
```

* Se for número, segue para o próximo.

```php
        $isNumeric = false;
        if (strlen($value) > 0) $isText = true;
    }
```

* Se não for numérico, marca que não é número. Se for string não vazia, marca que é texto.

```php
    if ($SGBD == "mysql"){
        return $isNumeric ? 'INT' : ($isText ? 'VARCHAR(255)' : 'TEXT');
```

* Para MySQL:

  * Se tudo é número, retorna INT;
  * Se há texto, VARCHAR(255);
  * Caso contrário, TEXT.

```php
    }elseif($SGBD == "postgres"){
        return $isNumeric ? 'INTEGER' : ($isText ? 'VARCHAR(255)' : 'TEXT');
```

* Lógica semelhante para compatibilidade com PostgreSQL.

```php
    }else{
        return 'TEXT';
    }
```

* Se o SGBD falhar ou for desconhecido, retorna TEXT como fallback seguro. Como já explicado em python.

---

### ⚖️ Comparação entre Python e PHP

| Característica                     | Python                                         | PHP                                               |
| ---------------------------------- | ---------------------------------------------- | ------------------------------------------------- |
| Tipos Detectados                   | Booleano, Data, Inteiro, Float, Texto          | Apenas Numérico e Texto                           |
| Sofisticação                       | Alta (análise detalhada, regras de formatação) | Simples (só `is_numeric` e verificação de string) |
| Suporte a Zeros à esquerda         | Sim                                            | **Não**                                               |
| Detecção de Data                   | Sim (ISO + tentativa com parser)               | **Não**                                               |
| Similaridade com Árvore de Decisão | Alta (multi-condicional com contagem de pesos) | Baixa (condicional binária)                       |
| Configuração de Tipo por SGBD      | Sim                                            | Sim                                               |
| Fallback Seguro                    | Sim (retorna TEXT se dúvida)                   | Sim                                               |

---

### 📚 Notas sobre ``detect_column_type()``

Ambas as versões da função refletem conceitos de classificação por regras claras e explícitas. Especialmente o Python, que age mediante uma árvore de decisão simples, permitindo decidir tipos com base em regras fixas mais completas, o que é excelente para dados estruturados com padrões reconhecíveis. O que também guardam apesar de suas simplicidades (tanto em python, quanto em PHP) uma escalabilidade real e mensurável. Que pode ser aferida e comparada com outras propostas para este fim, a nível de produção. Através de um print de desempenho (disponível na versão completa de ambos os scripts).

Algumas características como **Não**, na **[Tabela de  Comparação](#-comparação-entre-python-e-php)**:
_Note que **as implementações em python não foram completamente espelhadas** em PHP, por mera pontualidade do desenvolvedor, que preferiu ser arbitrariamente breve neste ponto (disparidade esta, não motivada por quaisquer impedimentos ou limitações da linguagem). Haja visto que isto não impede ou limita a observação, analise e exibição prática dos conceitos expostos. Observados de forma mais completa, adiante._ 

---
<br> &nbsp; 



# SCRIPTS COMPLETOS (Python & PHP)

## Descrição

Ambos os scripts têm como objetivo facilitar a conversão de arquivos CSV em instruções SQL compatíveis com bancos de dados MySQL ou PostgreSQL, exibindo de forma prática, a proposta e conceitos abordados anteriormente. O primeiro script é escrito em Python e o segundo em PHP. Ambos possuem funcionalidades semelhantes, mas com diferenças na implementação, dependências e ambientes de execução.

## Como usar

### Python

1.  Para rodar o script em Python, utilize o comando:
    
    Para uso de forma interativa:
    ```bash
    source ~/.bashrc; python csv_to_sql.py 
    
    ```
    
    Ou, para uma conversão direta:
    
    ```bash
    source ~/.bashrc; python csv_to_sql.py "seu_arquivo.csv" 
    
    ```
    
    E, caso queira especificar o nome da tabela diretamente:
    
    ```bash
    source ~/.bashrc; python csv_to_sql.py "seu_arquivo.csv" "nome_da_tabela"
    
    ```
    

### PHP

1.  Para rodar o script em PHP, utilize o comando:

    Para uso de forma interativa:
    ```bash
    source ~/.bashrc; php csv_to_sql.php
    
    ```
    
    Ou, para uma conversão direta:
    
    ```bash
    source ~/.bashrc; php csv_to_sql.php "seu_arquivo.csv"
    
    ```
    
    E, caso queira especificar o nome da tabela diretamente:
    
    ```bash
    source ~/.bashrc; php csv_to_sql.php "seu_arquivo.csv" "nome_da_tabela"
    
    ```
    **NOTA:** 
    Recomenda-se a limpeza do terminal, para melhor visualização dos retornos. Por isso o acréscimo de 
    ```bash
    source ~/.bashrc
    ```
    resultando em um reload do bash.

----------


## Detalhamento dos Scripts

A seguir, observaremos as funções de ambos os scripts (Python e PHP), explicando a lógica e o que acontece em cada parte. Como ambos os scripts também se prestam a um fim didático. Algumas das descrições consideradas chave, também estão presentes no próprio script. Afim de auxiliar, os mais iniciantes, curiosos e estudantes do tema:

----------

### **Script Python (`csv_to_sql.py`)**

#### 1. Importação de Bibliotecas

```python
import os
import sys
import csv
import re
from datetime import datetime
from dateutil.parser import parse as parse_date
import time

```

-   **os**: Usado para manipulações no sistema de arquivos, como verificar a existência de arquivos ou diretórios.
    
-   **sys**: Usado para acessar os argumentos da linha de comando e outras variáveis de ambiente.
    
-   **csv**: Fornece funções para leitura e escrita de arquivos CSV.
    
-   **re**: Módulo de expressões regulares para manipulação de strings.
    
-   **datetime**: Para manipulação de datas e formatação de timestamps.
    
-   **dateutil.parser**: Biblioteca externa que oferece funções robustas para detectar e parsear datas de diversos formatos.
    
-   **time**: Para medir o tempo de execução do script.
    

#### 2. Configurações Iniciais

```python
SGBD = "postgres"
rView = 5
batchSize = 500
outputPath = "/app/backups/"

```

Aqui, são definidas as configurações padrão, como o tipo de SGBD (Postgres ou MySQL), o número de linhas para visualização preliminar (`rView`), o tamanho dos lotes de inserção (`batchSize`) e o diretório de saída dos arquivos gerados (`outputPath`).

#### 3. Função `normalize_name(name)`

```python
def normalize_name(name):
    name = name.strip().lower()
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'[\s\-\.]+', '_', name)
    return name

```

Esta função serve para normalizar o nome de colunas e tabelas. Ela remove espaços, acentos, pontuações e converte os caracteres para minúsculas. O objetivo é garantir que o nome da tabela ou coluna esteja no formato adequado para SQL, com caracteres simples e sem espaços.

#### 4. Função `detect_delimiter(file_path)`

```python
def detect_delimiter(file_path):
    delimiters = [",", ";", "\t", "|"]
    with open(file_path, 'r', encoding='utf-8') as f:
        line = f.readline()
        results = {d: len(line.split(d)) for d in delimiters}
    return max(results, key=results.get)

```

Esta função detecta automaticamente o delimitador do arquivo CSV, verificando qual é o mais comum em uma linha do arquivo (pode ser vírgula, ponto e vírgula, tabulação ou pipe).

#### 5. Função `detect_column_type(values, sgbd)`

```python
def detect_column_type(values, sgbd):
    bool_set = {'true', 'false', 'yes', 'no', 'sim', 'não', '0', '1'}
    non_empty_values = [v.strip().lower() for v in values if v.strip()]
    type_balance = {'boolean': 0, 'date': 0, 'integer': 0, 'float': 0, 'text': 0}
    for v in non_empty_values:
        if v in bool_set:
            type_balance['boolean'] += 1
        elif v.lstrip('0') != v and len(v) > 1:
            type_balance['text'] += 1
        elif v.replace(',', '.').replace('.', '', 1).isdigit():
            type_balance['float' if '.' in v or ',' in v else 'integer'] += 1
        elif re.match(r'^\d{4}-\d{2}-\d{2}$', v):
            type_balance['date'] += 1
        else:
            try:
                parse_date(v, dayfirst=True)
                type_balance['date'] += 1
            except:
                type_balance['text'] += 1
    best_type = max(type_balance, key=type_balance.get) if len(non_empty_values) == 0 or type_balance[max(type_balance, key=type_balance.get)] / len(non_empty_values) >= 0.8 else 'text'
    return {
        'mysql': {'boolean': 'BOOLEAN', 'date': 'DATE', 'integer': 'INT', 'float': 'FLOAT', 'text': 'TEXT'},
        'postgres': {'boolean': 'BOOLEAN', 'date': 'DATE', 'integer': 'INTEGER', 'float': 'REAL', 'text': 'TEXT'}
    }.get(sgbd, {}).get(best_type, 'TEXT')

```

Essa função é responsável por determinar o tipo de dados de uma coluna com base nos valores encontrados. Ela classifica os dados como booleano, data, inteiro, flutuante ou texto, dependendo dos padrões de valor encontrados nas células. Conforme já explicado no inicio deste documento.

#### 6. Função `prompt(message)`

```python
def prompt(message):
    return input(message).strip()

```

Aqui, a função `prompt()` exibe uma mensagem para o usuário e captura a entrada fornecida.

#### 7. Função `preview_csv(file_path, delimiter, limit)`

```python
def preview_csv(file_path, delimiter, limit):
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=delimiter)
        print(f"\n\n\033[37;44m PRÉ-VISUALIZAÇÃO DOS DADOS \033[0m (máximo {limit} linhas):")
        for i, row in enumerate(reader):
            print(" | ".join(row))
            if i + 1 >= limit:
                break

```

Essa função exibe uma pré-visualização das primeiras linhas do arquivo CSV. É útil para confirmar se o arquivo foi lido corretamente antes de seguir com a conversão.

#### 8. Função `main()`

```python
def main():
    args = sys.argv
    ...

```

É onde todos os processos são iniciados e encapsulados. Ela começa capturando os argumentos passados pela linha de comando, determinando o nome do arquivo CSV, o nome da tabela e o delimitador. Após isso, a função gera o arquivo SQL de acordo com os tipos de banco de dados especificados e insere os dados no formato correto.

--- 
<br> &nbsp; 

### **Script PHP (`csv_to_sql.php`)**

Contem as funções e a lógica do script, em versão PHP.

#### 1. Definição de Configurações

```php
$SGBD = "postgres";          // "postgres" | "mysql"
$rView = 5;                  // Linhas em preview
$batchSize = 500;            // Blocos de insert
$outpuPath = "/app/backups/"; // pasta de saída

```

Assim como no script Python, o script PHP também define configurações iniciais, como o tipo de SGBD (Postgres ou MySQL), o número de linhas para visualização preliminar (`rView`), o tamanho dos lotes de inserção (`batchSize`) e o diretório de saída dos arquivos gerados (`outputPath`).

#### 2. Função `normalize_name($name)`

```php
function normalize_name($name) {
    $name = strtolower(trim($name));
    $name = preg_replace('/[^\w\s]/', '', $name);
    $name = preg_replace('/[\s\-\.]+/', '_', $name);
    return $name;
}

```

Essa função em PHP tem o mesmo objetivo que a versão Python: normalizar o nome da tabela ou coluna, removendo espaços, acentos e pontuações, e convertendo os caracteres para minúsculas.

#### 3. Função `detect_delimiter($filePath)`

```php
function detect_delimiter($filePath) {
    $delimiters = [",", ";", "\t", "|"];
    $line = fgets(fopen($filePath, 'r'));
    $results = [];
    foreach ($delimiters as $delimiter) {
        $fields = str_getcsv($line, $delimiter);
        $results[$delimiter] = count($fields);
    }
    arsort($results);
    return array_key_first($results);
}

```

No PHP, essa função também detecta automaticamente o delimitador usado no arquivo CSV, assim como o script Python. Ela analisa uma linha do arquivo e escolhe o delimitador mais comum.

#### 4. Função `detect_column_type($values, $SGBD)`

```php
function detect_column_type($values, $SGBD) {
    $isNumeric = true;
    $isText = false;
    foreach ($values as $value){
        if (is_numeric($value)) continue;
        $isNumeric = false;
        if (strlen($value) > 0) $isText = true;
    }
    if ($SGBD == "mysql"){ 
        return $isNumeric ? 'INT' : ($isText ? 'VARCHAR(255)' : 'TEXT'); 
    } elseif ($SGBD == "postgres"){ 
        return $isNumeric ? 'INTEGER' : ($isText ? 'VARCHAR(255)' : 'TEXT');
    } else { 
        return 'TEXT'; 
    }
}

```

A função `detect_column_type` no PHP segue um raciocínio semelhante à versão Python, determinando o tipo de dados da coluna com base nos valores encontrados. A diferença está na lógica de verificação e na escolha do tipo de dados com base no SGBD.

#### 5. Função `prompt($message)`

```php
function prompt($message){
    echo $message;
    return trim(fgets(STDIN));
}

```

Essa função exibe uma mensagem ao usuário e captura sua entrada, assim como o `prompt` do Python.

#### 6. Função `main()`

```php
if ($argc === 1) {
    ...
}

```

Assim como o script Python
, o script PHP possui uma função principal que encapsula os fluxos de execução, captura os parâmetros da linha de comando e chama as funções necessárias para gerar o arquivo SQL de inserção. Embora não obrigatória, esta prática visa facilitar a manutenção, monitoramento de desempenho e edições.

----------

### **Lógica Geral**

Tanto o script Python quanto o PHP seguem uma lógica similar:

1.  O script lê o arquivo CSV e detecta o delimitador utilizado.
    
2.  A primeira linha do CSV é processada para determinar os nomes das colunas.
    
3.  A partir de uma amostra, o script determina o tipo de dado (se é texto, inteiro, etc.) e o classifica em uma coluna.

4.  O script gera uma instrução SQL para a criação da tabela no banco de dados (se ele já não existir), considerando os tipos de dados em uma inserção segura (composta de blocos atômicos).

5. O arquivo SQL gerado é salvo no diretório de saída especificado. 

6. Ao carregar o Script SQL resultante. Os dados serão lidos em blocos/lotes (`batchSize de forma serial`) e inseridos na tabela SQL gerada, conforme regra de TRANSACT, contribuindo com o ACID (se praticado pelo time e aceito pelo SGBD).
    

----------

## Conclusão e observações:

Ambos os scripts têm como objetivo facilitar a integração de dados em bancos de dados MySQL e PostgreSQL, oferecendo ao usuário **uma ferramenta simples, mas poderosa, para automação, auditoria de importação de dados**.
_Este projeto não oferece nenhuma crítica ou restrição ao uso da biblioteca Pandas, quanto a manipulação de dados em ambiente python._ Ao contrário, ele pretende (entre outros objetivos) ilustrar o que muitas vezes ocorre longe dos olhos dos que a utilizam. Se prestando a diversas outras funcionalidades, além do tipo classificação, manipulação e catalogação de dados, descritas aqui. 
Embora ambos os scripts tenham sido criados para **uso em ambientes de produção, permanecem sendo um conceito** particular. O que significa, não ser nenhum exagero recomendar a familiarização, analise e uso prédio em ambientes controlados como os de homologação, desenvolvimento, de estudo e testes. Afim de que o uso adequado, corresponda as expectativas de seus utilizadores. Enfatizando a importância de que, mais do que se utilizar de bibliotecas e scripts práticos, é preciso compreende-los. Com a mesma compreensão e a humildade, de que nem mesmo a matéria sólida, é capaz de chegar ao zero absoluto.



<br> &nbsp; 
## AUTOR:

**Autor:** Sr. Victor Batista  
**GitHub:** [https://github.com/srvictorbatista](https://github.com/srvictorbatista)  
**LinkedIn:** [https://linkedin.com/in/levymac](https://linkedin.com/in/levymac)  
**Contato no Telegram:** [@LevyMac](https://t.me/levymac)

<br> &nbsp;
## REPOSITÓRIO / CONTRIBUIÇÕES / LICENÇA / REPORTs:

- **GitHub:** [https://github.com/srvictorbatista/CSVtoSQL-PNCA](https://github.com/srvictorbatista/CSVtoSQL-PNCA)
- **Licença:** [MIT](https://opensource.org/licenses/MIT) – Uso livre sem limitações no estado em que se encontra, com a preservação da autoria.

Contribuições são bem-vindas! Caso deseje melhorar este projeto, fique à vontade para abrir uma **issue** ou **pull request** no repositório acima.
Para **reportar falhas, melhorias ou observações adicionais**. Por favor, entre em **[contato com o autor](#autor)**. 

---

**Licença MIT para uso livre no estado em que se encontra, preservada a autoria.**
