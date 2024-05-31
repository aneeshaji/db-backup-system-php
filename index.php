<?php

require 'vendor/autoload.php'; // Include the AWS SDK for PHP

use Aws\S3\S3Client;
use Aws\Exception\AwsException;

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
    
            obfPrint('Backup file successfully saved to S3: ' . $result['ObjectURL'], 1, 1);
            return true;
        } catch (AwsException $e) {
            obfPrint('Error uploading backup file to S3: ' . $e->getMessage());
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
        if (mysqli_connect_errno()) throw new Exception('ERROR connecting database: ' . mysqli_connect_error());
        if (!mysqli_set_charset($conn, $charset)) mysqli_query($conn, 'SET NAMES ' . $charset);
    } catch (Exception $e) {
        print_r($e->getMessage());
        die();
    }

    try {
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
                return false;
            }
            //$backupKey .= '.gz'; // Update the backup key to include the .gz extension
        }

        // Initialize the S3 client
        $s3Client = new S3Client([
            'version' => 'latest',
            'region'  => 'ap-south-1',
            'credentials' => [
                'key'    => 'AKIAVRUVQ6DE6QCLZUZM',
                'secret' => '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',
            ],
        ]);

        $s3Bucket = 'db-backup-manager';
        //$s3Key = $backupDir . '/' . $backupFile;

        //print_r($result); die;

        // Upload the backup file to S3
        if ($gzipBackupFile) {
            saveFileToS3($s3Client, $s3Bucket, $result, file_get_contents($gzippedFile));
            unlink($gzippedFile); // Remove the local gzipped file after upload
        } else {
            saveFileToS3($s3Client, $s3Bucket, $s3Key, $sql);
        }

    } catch (Exception $e) {
        print_r($e->getMessage());
        return false;
    }

    return true;
}

/**
 * Example usage of the function
 */
$host = 'localhost';
$username = 'root';
$password = '';
$dbName = 'jqrms';
$backupDir = 'myphp-backup-files';
$tables = '*';
$ignoreTables = ['tbl_token_auth', 'token_auth'];
$charset = 'utf8';
$gzipBackupFile = true;
$disableForeignKeyChecks = true;
$batchSize = 1000;

backupDatabase($host, $username, $password, $dbName, $backupDir, $tables, $ignoreTables, $charset, $gzipBackupFile, $disableForeignKeyChecks, $batchSize);
?>
