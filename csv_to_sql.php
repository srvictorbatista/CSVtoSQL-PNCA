<?php

// COMO USAR: 
// source ~/.bashrc; php csv_to_sql.php (de forma infetariva) ou
// source ~/.bashrc; php csv_to_sql.php "seu_arquivo.csv" (conversão direta)


// CONFIGURAÇÕES:
$SGBD=         "postgres";          // "postgres" | "mysql"
$rView=         5;                  // Linhas em preview
$batchSize=     500;                // Blocos de insert
$outpuPath=     "/app/backups/";    // pasta de saída



function normalize_name($name) {
    $name = strtolower(trim($name));
    $name = preg_replace('/[^\w\s]/', '', $name); // Trata acentos e símbolos
    $name = preg_replace('/[\s\-\.]+/', '_', $name); // Trata espaços e pontuações por underline
    return $name;
}

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

function detect_column_type($values, $SGBD) {
    $isNumeric = true;
    $isText = false;
    foreach ($values as $value){
        if (is_numeric($value)) continue;
        $isNumeric = false;
        if (strlen($value) > 0) $isText = true;
    }
    if ($SGBD == "mysql"){ return $isNumeric ? 'INT' : ($isText ? 'VARCHAR(255)' : 'TEXT'); // MySQL
    }elseif($SGBD == "postgres"){ return $isNumeric ? 'INTEGER' : ($isText ? 'VARCHAR(255)' : 'TEXT'); // Postgres
    }else{ return 'TEXT'; // fallback seguro (em ultimo caso, como segurança adicional)
    }
}

function prompt($message){
    echo $message;
    return trim(fgets(STDIN));
}

if ($argc === 1) {
    $csvFile = prompt("Digite o caminho do arquivo CSV: ");
    if (!file_exists($csvFile) || !is_readable($csvFile)) {
        echo "Erro: Arquivo não encontrado ou não legível.\n";
        exit(1);
    }
    $delimiter = detect_delimiter($csvFile);
    $handlePreview = fopen($csvFile, 'r');
    echo "\n\n\033[37;44m PRÉ-VISUALIZAÇÃO DOS DADOS \033[0m (máximo $rView linhas):\n";
    $count = 0;
    while (($line = fgetcsv($handlePreview, 0, $delimiter)) !== false && $count < $rView) {
        echo implode(" | ", $line) . "\n";
        $count++;
    }
    fclose($handlePreview);
    $tableNameInput = prompt("\n\033[37;44m DIGITE O NOME DA TABELA SQL \033[0m \033[37m\n(ou pressione Enter para usar o nome do arquivo):\033[0m ");
    $tableName = $tableNameInput ? normalize_name($tableNameInput) : normalize_name(pathinfo($csvFile, PATHINFO_FILENAME));
} elseif ($argc === 2) {
    $csvFile = $argv[1];
    $tableName = normalize_name(pathinfo($csvFile, PATHINFO_FILENAME));
    $delimiter = detect_delimiter($csvFile);
} elseif ($argc === 3) {
    $csvFile = $argv[1];
    $tableName = normalize_name($argv[2]);
    $delimiter = detect_delimiter($csvFile);
} else {
    echo "USO: php csv_to_sql.php \"caminho/para/arquivo.csv\" \"nome da tabela\"\n";
    echo "OBS: O uso de aspas é obrigatório.\n";
    exit(1);
}
if (!file_exists($csvFile) || !is_readable($csvFile)) {
    echo "Erro: Arquivo não encontrado ou não legível: $csvFile\n";
    exit(1);
}
$start = microtime(true);
$SGBD = mb_strtolower($SGBD, 'UTF-8'); // garante padrão minuscula
$datePrefix = date("d.m.Y");
$outputFile = "{$outpuPath}{$datePrefix}_{$tableName}.sql";
$handle = fopen($csvFile, 'r');
$columns = fgetcsv($handle, 0, $delimiter);
if (!$columns) {
    echo "Erro: Cabeçalho vazio ou inválido.\n";
    exit(1);
}
$columns = array_map('normalize_name', $columns);

// Seleciona tipos de dados/colunas
$previewRows = [];
$count = 0;
while (($row = fgetcsv($handle, 0, $delimiter)) !== false && $count < 10) {
    $previewRows[] = $row;
    $count++;
}
fclose($handle);
$columnTypes = [];
foreach ($columns as $index => $col) {
    $columnValues = array_column($previewRows, $index);
    $columnTypes[$col] = detect_column_type($columnValues, $SGBD);
}

$out = fopen($outputFile, 'w');
fwrite($out, "-- Gerado a partir de $csvFile \n-- Padrão: {$SGBD}\n\n");

// CREATE TABLE (se não existir)
switch($SGBD){
    case "mysql":
        $tableSQL = "CREATE TABLE IF NOT EXISTS `$tableName` (\n";
        $tableSQL .= "  `id` INT AUTO_INCREMENT PRIMARY KEY,\n";
        foreach ($columns as $col){
            if ($col !== 'id'){
                $type = $columnTypes[$col] ?? 'VARCHAR(255)';
                $tableSQL .= "  `$col` $type,\n";
            }
        }
    break;
    case "postgres":
        $tableSQL = "CREATE TABLE IF NOT EXISTS \"$tableName\" (\n";
        $tableSQL .= "  \"id\" SERIAL PRIMARY KEY,\n";
        foreach ($columns as $col){
            if ($col !== 'id'){
                $type = $columnTypes[$col] ?? 'VARCHAR(255)';
                $tableSQL .= "  \"$col\" $type,\n";
            }
        }
    break;
}
$tableSQL = rtrim($tableSQL, ",\n") . "\n);\n\n";
fwrite($out, $tableSQL);

// INSERT INTO
$handle = fopen($csvFile, 'r');
$columns = array_map('normalize_name', fgetcsv($handle, 0, $delimiter));
$recordCount = 0;
$insertBatch = [];
switch($SGBD){
    case "mysql":
        while (($row = fgetcsv($handle, 0, $delimiter)) !== false){
            if (count($row) !== count($columns)) continue;
            $escaped = array_map(function ($v) {
                return is_numeric($v) ? $v : "'" . str_replace("'", "\'", trim($v)) . "'";
            }, $row);
            $insertBatch[] = "(" . implode(", ", $escaped) . ")";
            $recordCount++;
            if (count($insertBatch) >= $batchSize) {
                fwrite($out, "BEGIN; \nINSERT INTO `$tableName` (`" . implode("`, `", $columns) . "`) VALUES\n");
                fwrite($out, implode(",\n", $insertBatch) . ";\nCOMMIT; \n\n");
                $insertBatch = [];
            }
        }
        if (!empty($insertBatch)) {
            fwrite($out, "BEGIN; \nINSERT INTO `$tableName` (`" . implode("`, `", $columns) . "`) VALUES\n");
            fwrite($out, implode(",\n", $insertBatch) . ";\nCOMMIT; \n\n");
        }
    break;
    case "postgres":
        while (($row = fgetcsv($handle, 0, $delimiter)) !== false){
            if (count($row) !== count($columns)) continue;
            $escaped = array_map(function ($v) {
                return is_numeric($v) ? $v : "'" . str_replace("'", "''", trim($v)) . "'";
            }, $row);
            $insertBatch[] = "(" . implode(", ", $escaped) . ")";
            $recordCount++;
            if (count($insertBatch) >= $batchSize) {
                fwrite($out, "BEGIN; \nINSERT INTO \"$tableName\" (\"" . implode("\", \"", $columns) . "\") VALUES\n");
                fwrite($out, implode(",\n", $insertBatch) . ";\nCOMMIT; \n\n");
                $insertBatch = [];
            }
        }
        if (!empty($insertBatch)) {
            fwrite($out, "BEGIN; \nINSERT INTO \"$tableName\" (\"" . implode("\", \"", $columns) . "\") VALUES\n");
            fwrite($out, implode(",\n", $insertBatch) . ";\nCOMMIT; \n\n");
        }
    break;
}

fclose($handle);
fclose($out);

$duration = microtime(true) - $start;
echo "\n\033[37;44m ✅ CONVERSÃO CONCLUÍDA \033[0m\n";
echo "Script {$SGBD} gerado em: \033[37m$outputFile\033[0m\n";
echo "Linhas processadas (dados): $recordCount\n";
printf("Todo o processo levou: %s%s\n\n\n", $duration > 59 ? ($f=gmdate("H:i:s",(int)$duration)) . " (" : '', number_format($duration, 2) . " segundos" . ($duration > 59 ? ')' : ''));
