<?php
/**
 * This file contains the Backup_Database function wich performs
 * a partial or complete backup of any given MySQL database
 * @author Aneesh Ajithkumar <dev.aneeshajithkumar@gmail.com>
 * @version 1.0
 */


require 'vendor/autoload.php'; // Include the AWS SDK for PHP

// Load the .env file
$dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
$dotenv->load();

use Aws\S3\S3Client;
use Aws\Exception\AwsException;


/**
 * Database Backup For First DataBase
 */
$host = $_ENV['BACKUP_DB_HOST'];
$username = $_ENV['BACKUP_DB_USERNAME'];
$password = $_ENV['BACKUP_DB_PASSWORD'];
$dbName = $_ENV['BACKUP_DB_NAME'];

$backupDir = 'db-backups';
$tables = '*';
$ignoreTables = ['tbl_token_auth', 'token_auth'];
$charset = 'utf8';
$gzipBackupFile = true;
$disableForeignKeyChecks = true;
$batchSize = 1000;

backupDatabase($host, 
$username, 
$password, 
$dbName,
$backupDir, 
$tables, 
$ignoreTables, 
$charset, 
$gzipBackupFile, 
$disableForeignKeyChecks, 
$batchSize);

/**
 * Logs a message to a log file with a timestamp and type.
 * 
 * @param string $message The message to log.
 * @param string $type The type of the log message (INFO, ERROR, WARNING, etc.).
 * @param string $logFile The file to log the message to.
 */
function logMessage($message, $type = 'INFO', $logFile = 'logs/app.log') {
    // Ensure the log directory exists
    if (!file_exists(dirname($logFile))) {
        mkdir(dirname($logFile), 0777, true);
    }

    // Prepare the log message with a timestamp and type
    $timestamp = date('Y-m-d H:i:s');
    $logEntry = "[$timestamp] [$type] $message" . PHP_EOL;

    // Write the log message to the log file
    file_put_contents($logFile, $logEntry, FILE_APPEND | LOCK_EX);
}

/**
 * Perform a complete or partial backup of a MySQL database
 * 
 * @param string $host
 * @param string $username
 * @param string $password
 * @param string $dbName
 * @param string $backupDir
 * @param string $tables
 * @param array $ignoreTables
 * @param string $charset
 * @param bool $gzipBackupFile
 * @param bool $disableForeignKeyChecks
 * @param int $batchSize
 * @return bool|string
 */

function backupDatabase(
    $host, 
    $username, 
    $password,
    $dbName,
    $backupDir = '.',
    $tables = '*',
    $ignoreTables = [],
    $charset = 'utf8',
    $gzipBackupFile = true, 
    $disableForeignKeyChecks = true, 
    $batchSize = 1000
) {
    $output = '';
    $backupFile = 'db-backup-' . $dbName . '-' . date("Ymd_His", time()) . '.sql';
    function obfPrint($msg, $lineBreaksBefore = 0, $lineBreaksAfter = 1) {
        global $output;
        if (!$msg) return false;
        $msg = date("Y-m-d H:i:s") . ' - ' . $msg;
        $lineBreak = (php_sapi_name() != "cli") ? "<br />" : "\n";
        $output = '';

        for ($i = 0; $i < $lineBreaksBefore; $i++) $output .= $lineBreak;
        $output .= $msg;
        for ($i = 0; $i < $lineBreaksAfter; $i++) $output .= $lineBreak;

        echo $output;
        flush();
    }

    function saveFileToS3($s3Client, $s3Bucket, $backupKey, $sql) {
        try {
            $result = $s3Client->putObject([
                'Bucket' => $s3Bucket,
                'Key'    => $backupKey,
                'Body'   => $sql,
            ]);
            logMessage('Backup file successfully saved to S3 '.$result['ObjectURL'], 'INFO');
            obfPrint('Backup file successfully saved to S3: ' . $result['ObjectURL'], 1, 1);
            return true;
        } catch (AwsException $e) {
            obfPrint('Error uploading backup file to S3: ' . $e->getMessage());
            logMessage('Error uploading backup file to S3: ERROR - '. $e->getMessage(), 'ERROR');
            return false;
        }
    }

    function saveFile($backupDir, $backupFile, &$sql) {
        if (!$sql) return false;
        if (!file_exists($backupDir)) mkdir($backupDir, 0777, true);
        file_put_contents($backupDir . '/' . $backupFile, $sql, FILE_APPEND | LOCK_EX);
        return true;
    }

    function gzipBackupFile($backupDir, $backupFile, $level = 9) {
        $source = $backupDir . '/' . $backupFile;
        $dest =  $source . '.gz';

        obfPrint('Gzipping backup file to ' . $dest . '... ', 1, 0);
        $mode = 'wb' . $level;

        if ($fpOut = gzopen($dest, $mode)) {
            if ($fpIn = fopen($source, 'rb')) {
                while (!feof($fpIn)) gzwrite($fpOut, fread($fpIn, 1024 * 256));
                fclose($fpIn);
            } else return false;
            gzclose($fpOut);
            if (!unlink($source)) return false;
        } else return false;

        obfPrint('OK');
        return $dest;
    }

    try {
        $conn = mysqli_connect($host, $username, $password, $dbName);
        if (mysqli_connect_errno()) {
            logMessage('Error connecting database: ERROR - '. mysqli_connect_error(), 'ERROR');
            throw new Exception('ERROR connecting database: ' . mysqli_connect_error());
        }
        if (!mysqli_set_charset($conn, $charset)) mysqli_query($conn, 'SET NAMES ' . $charset);
    } catch (Exception $e) {
        logMessage('Error backing up database - '. $e->getMessage(), 'ERROR');
        print_r($e->getMessage());
        die();
    }

    try {
        logMessage('Backup cron job started for database '.$dbName, 'INFO');
        if ($tables == '*') {
            $tables = array();
            $result = mysqli_query($conn, 'SHOW TABLES');
            while ($row = mysqli_fetch_row($result)) $tables[] = $row[0];
        } else {
            $tables = is_array($tables) ? $tables : explode(',', str_replace(' ', '', $tables));
        }

        $sql = 'CREATE DATABASE IF NOT EXISTS `' . $dbName . '`' . ";\n\n";
        $sql .= 'USE `' . $dbName . "`;\n\n";
        if ($disableForeignKeyChecks === true) $sql .= "SET foreign_key_checks = 0;\n\n";

        foreach ($tables as $table) {
            if (in_array($table, $ignoreTables)) continue;
            obfPrint("Backing up `" . $table . "` table..." . str_repeat('.', 50 - strlen($table)), 0, 0);

            $sql .= 'DROP TABLE IF EXISTS `' . $table . '`;';
            $row = mysqli_fetch_row(mysqli_query($conn, 'SHOW CREATE TABLE `' . $table . '`'));
            $sql .= "\n\n" . $row[1] . ";\n\n";

            $row = mysqli_fetch_row(mysqli_query($conn, 'SELECT COUNT(*) FROM `' . $table . '`'));
            $numRows = $row[0];
            $numBatches = intval($numRows / $batchSize) + 1;

            for ($b = 1; $b <= $numBatches; $b++) {
                $query = 'SELECT * FROM `' . $table . '` LIMIT ' . ($b * $batchSize - $batchSize) . ',' . $batchSize;
                $result = mysqli_query($conn, $query);
                $realBatchSize = mysqli_num_rows($result);
                $numFields = mysqli_num_fields($result);

                if ($realBatchSize !== 0) {
                    $sql .= 'INSERT INTO `' . $table . '` VALUES ';
                    for ($i = 0; $i < $numFields; $i++) {
                        $rowCount = 1;
                        while ($row = mysqli_fetch_row($result)) {
                            $sql .= '(';
                            for ($j = 0; $j < $numFields; $j++) {
                                if (isset($row[$j])) {
                                    $row[$j] = addslashes($row[$j]);
                                    $row[$j] = str_replace("\n", "\\n", $row[$j]);
                                    $row[$j] = str_replace("\r", "\\r", $row[$j]);
                                    $row[$j] = str_replace("\f", "\\f", $row[$j]);
                                    $row[$j] = str_replace("\t", "\\t", $row[$j]);
                                    $row[$j] = str_replace("\v", "\\v", $row[$j]);
                                    $row[$j] = str_replace("\a", "\\a", $row[$j]);
                                    $row[$j] = str_replace("\b", "\\b", $row[$j]);
                                    if ($row[$j] == 'true' or $row[$j] == 'false' or preg_match('/^-?[1-9][0-9]*$/', $row[$j]) or $row[$j] == 'NULL' or $row[$j] == 'null') {
                                        $sql .= $row[$j];
                                    } else {
                                        $sql .= '"' . $row[$j] . '"';
                                    }
                                } else {
                                    $sql .= 'NULL';
                                }

                                if ($j < ($numFields - 1)) $sql .= ',';
                            }

                            if ($rowCount == $realBatchSize) {
                                $rowCount = 0;
                                $sql .= ");\n";
                            } else {
                                $sql .= "),\n";
                            }

                            $rowCount++;
                        }
                    }

                    saveFile($backupDir, $backupFile, $sql);
                    $sql = '';
                }
            }

            $sql .= "\n\n";
            obfPrint('OK');
        }
        logMessage('Backup cron job completed for database '.$dbName, 'INFO');

        if ($disableForeignKeyChecks === true) $sql .= "SET foreign_key_checks = 1;\n";
        saveFile($backupDir, $backupFile, $sql);

        // if ($gzipBackupFile) {
        //     gzipBackupFile($backupDir, $backupFile);
        // } else {
        //     obfPrint('Backup file successfully saved to ' . $backupDir . '/' . $backupFile, 1, 1);
        // }
        // $s3Client = new S3Client([
        //     'version' => 'latest',
        //     'region'  => 'ap-south-1',
        //     'credentials' => [
        //         'key'    => 'AKIAVRUVQ6DE6QCLZUZM',
        //         'secret' => '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',
        //     ],
        // ]);

        // $s3Bucket = 'db-backup-manager';
        // $s3Key = $backupDir . '/' . $backupFile;

        // // Upload the backup file to S3
        // saveFileToS3($s3Client, $s3Bucket, $s3Key, $sql);


        // If gzipBackupFile is enabled, gzip the backup file before uploading to S3
        if ($gzipBackupFile) {
            $result = $gzippedFile = gzipBackupFile($backupDir, $backupFile);
            if (!$gzippedFile) {
                obfPrint('Error gzipping backup file.');
                logMessage('Error gzipping backup file for database '.$dbName, 'ERROR');
                return false;
            }
        }

        // Initialize the S3 client
        $s3Client = new S3Client([
            'version' => 'latest',
            'region'  => $_ENV['AWS_DEFAULT_REGION'],
            'credentials' => [
                'key'    => $_ENV['AWS_ACCESS_KEY_ID'],
                'secret' => $_ENV['AWS_SECRET_ACCESS_KEY'],
            ],
        ]);

        $s3Bucket = $_ENV['AWS_BUCKET'];

        // Upload the backup file to S3
        if ($gzipBackupFile) {
            saveFileToS3($s3Client, $s3Bucket, $result, file_get_contents($gzippedFile));
            unlink($gzippedFile); // Remove the local gzipped file after upload
        } else {
            saveFileToS3($s3Client, $s3Bucket, $s3Key, $sql);
        }

    } catch (Exception $e) {
        logMessage('Backup cron job encountered an error for database '.$dbName. 'ERROR - '. $e->getMessage(), 'ERROR');
        print_r($e->getMessage());
        return false;
    }

    return true;
}
?>
