<?php


header("Content-Type:application/json");

try {
    $network = (strtolower($_GET['network']) == "testnet") ? "testnet" : "mainnet";
    $url = $_GET['url'] ?? null;
    $nodeHash = $_GET['nodeHash'] ?? null;
    $columns = $_GET['values'] ? explode(';', $_GET['values']) : null;

    $valid_columns = [
        "nodeHash",
        "ip",
        "geo",
        "url",
        "trustScore",
        "version",
        "feeData",
        "creationTime",
        "sync",
        "http_code",
        "ssl_exp",
        "status",
        "last_seen",
        "stats",
        "displayInfo",
        "last_updated"
    ];
    $json_columns = [
        "geo",
        "feeData",
        "stats",
        "displayInfo"
    ];

    $database_name     = 'coti_nodes';
    $database_user     = 'DB_USERNAME';
    $database_password = 'DB_PASSWORD';
    $database_host     = 'localhost';

    $pdo = new PDO('mysql:host=' . $database_host . '; dbname=' . $database_name, $database_user, $database_password);
    $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);  
    $pdo->setAttribute(PDO::ATTR_EMULATE_PREPARES,false);                    

    $sql = "SELECT UNIX_TIMESTAMP(update_time) FROM information_schema.tables WHERE table_schema = 'coti_nodes' AND table_name = '$network'";
    $stmt = $pdo->prepare($sql);
    $stmt->execute();

    if ($stmt) {
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $ts = $row['UNIX_TIMESTAMP(update_time)'];
    } else {
        $ts = null;
    }

    $sql = "select * from $network";

    $stmt = $pdo->prepare($sql);
    $stmt->execute();

    $data = [];

    while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {    
        if ($nodeHash) {
            if ($row['nodeHash'] == $nodeHash) {
                $response = [];
                $response['status'] = 'success';
                $response['data'] = [];
                if ($columns) {
                    foreach ($columns as $column) {
                        if (in_array($column, $valid_columns)) {
                            $response['data'][$column] = (in_array($column, $json_columns)) ? json_decode($row[$column]) : $row[$column];
                        }
                    }
                } else {
                    $response['data'] = array(
                        "ip" => $row["ip"],
                        "geo" => json_decode($row["geo"], true),
                        "url" => $row["url"],
                        "trustScore" => $row["trustScore"],
                        "version" => $row["version"],
                        "feeData" => json_decode($row["feeData"], true),
                        "creationTime" => $row["creationTime"],
                        "sync" => $row["sync"],
                        "http_code" => $row["http_code"],
                        "http_msg" => $row["http_msg"],
                        "ssl_exp" => $row["ssl_exp"],
                        "status" => $row["status"],
                        "last_seen" => $row["last_seen"],
                        "stats" => json_decode($row["stats"], true),
                        "displayInfo" => json_decode($row["displayInfo"], true),
                        'last_updated' => $row["last_updated"]
                    );
                }
                echo json_encode($response, JSON_PRETTY_PRINT);
                die();
            }
        } elseif ($url) {
            $pattern = '/\A(https:\/\/){0,1}([a-zA-Z\.\d\-]+)(\/*)\z/i';
            $formated_url = strtolower(preg_replace($pattern, 'https://$2', $url));
            
            if ($row['url'] == $formated_url) {
                $response = [];
                $response['status'] = 'success';
                $response['data'] = [];
                if ($columns) {
                    foreach ($columns as $column) {
                        if (in_array($column, $valid_columns)) {
                            $response['data'][$column] = (in_array($column, $json_columns)) ? json_decode($row[$column]) : $row[$column];
                        }
                    }
                } else {
                    $response['data'] = array(
                        "ip" => $row["ip"],
                        "geo" => json_decode($row["geo"], true),
                        "url" => $row["url"],
                        "trustScore" => $row["trustScore"],
                        "version" => $row["version"],
                        "feeData" => json_decode($row["feeData"], true),
                        "creationTime" => $row["creationTime"],
                        "sync" => $row["sync"],
                        "http_code" => $row["http_code"],
                        "http_msg" => $row["http_msg"],
                        "ssl_exp" => $row["ssl_exp"],
                        "status" => $row["status"],
                        "last_seen" => $row["last_seen"],
                        "stats" => json_decode($row["stats"], true),
                        "displayInfo" => json_decode($row["displayInfo"], true),
                        'last_updated' => $row["last_updated"]
                    );
                }
                echo json_encode($response, JSON_PRETTY_PRINT);
                die();
            }
        } else {
            $data[$row['nodeHash']] = array(
                "ip" => $row["ip"],
                "geo" => json_decode($row["geo"], true),
                "url" => $row["url"],
                "trustScore" => $row["trustScore"],
                "version" => $row["version"],
                "feeData" => json_decode($row["feeData"], true),
                "creationTime" => $row["creationTime"],
                "sync" => $row["sync"],
                "http_code" => $row["http_code"],
                "http_msg" => $row["http_msg"],
                "ssl_exp" => $row["ssl_exp"],
                "status" => $row["status"],
                "last_seen" => $row["last_seen"],
                "stats" => json_decode($row["stats"], true),
                "displayInfo" => json_decode($row["displayInfo"], true)
            );
        }
    } 

    if ($nodeHash || $url) {
        $response = [];
        $response['status'] = 'failed';
        echo json_encode($response, JSON_PRETTY_PRINT);
        die();
    }

    $response         = [];
    $response['FullNodes'] =  $data;
    $response['last_updated'] = $ts;

    echo json_encode($response, JSON_PRETTY_PRINT);

} catch (PDOException $e) {
    echo 'Database error. ' . $e->getMessage();
}        