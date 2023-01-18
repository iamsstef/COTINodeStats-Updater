import mariadb, sys, traceback, json, logging
from datetime import datetime

class DBHelper:
    def __init__(self, username, password, dbname, host):
        self.dbname = dbname
        try:
            self.conn = mariadb.connect(
                user = username,
                password = password,
                host = host,
                port = 3306,
                database = dbname

            )
            self.conn.autocommit = True
            self.conn.auto_reconnect = True
            self.cur = self.conn.cursor()
        except mariadb.Error as e:
            print(f"Error connecting to MariaDB Platform: {e}")
            sys.exit(1)

    def setup(self):
        #stmt = "CREATE TABLE IF NOT EXISTS items (description text)"
        stmt = '''CREATE TABLE IF NOT EXISTS mainnet
        (
            id INTEGER NOT NULL PRIMARY KEY UNIQUE AUTO_INCREMENT,
            nodeHash TEXT NOT NULL ,
            ip TEXT NOT NULL,
            geo JSON,
            url TEXT NOT NULL,
            trustScore DOUBLE NOT NULL,
            version TEXT NOT NULL,
            feeData JSON NOT NULL,
            creationTime BIGINT NOT NULL,
            sync INT NOT NULL,
            http_code INT NULL,
            http_msg TEXT NULL,
            ssl_exp BIGINT,
            status BOOL NOT NULL,
            last_seen BIGINT,
            stats JSON,
            displayInfo JSON,
            last_updated BIGINT NULL
        );'''
        self.cur.execute(stmt)
        stmt = '''CREATE TABLE IF NOT EXISTS testnet
        (
            id INTEGER NOT NULL PRIMARY KEY UNIQUE AUTO_INCREMENT,
            nodeHash TEXT NOT NULL UNIQUE,
            ip TEXT NOT NULL,
            geo JSON,
            url TEXT NOT NULL,
            trustScore DOUBLE NOT NULL,
            version TEXT NOT NULL,
            feeData JSON NOT NULL,
            creationTime BIGINT NOT NULL,
            sync INT NOT NULL,
            http_code INT NULL,
            http_msg TEXT NULL,
            ssl_exp BIGINT,
            status BOOL NOT NULL,
            last_seen BIGINT,
            stats JSON,
            displayInfo JSON,
            last_updated BIGINT NULL
        );'''
        self.cur.execute(stmt)

        self.conn.commit()

    def coti_node(self, network):
        _network = str(network).lower() #"mainnet" if network == "mainnet" else "testnet"
        return DBHelper.coti_node_(self, _network)

    class coti_node_():
        def __init__(self, parent_instance, network):
            self.parent_instance = parent_instance
            self.conn = parent_instance.conn
            self.cur = parent_instance.cur
            self.network = network

        def last_updated(self, human = False):
            stmt = f'''SELECT update_time
                FROM   information_schema.tables
                WHERE  table_schema = '{self.parent_instance.dbname}'
                    AND table_name = '{self.network}'
            '''
            self.cur.execute(stmt)
            if self.cur:
                row = self.cur.fetchone()
                try:
                    ts = row[0].timestamp()
                    if human:
                        readable = datetime.fromtimestamp(ts).strftime('%d-%m-%y at %H:%M UTC')
                        return readable
                    return ts
                except:
                    return None

        def add_node(self, nodeHash,ip,geo,url,trustScore,version,feeData,creationTime,sync,http_code,http_msg,ssl_exp,status,last_seen, displayInfo,last_updated):
            stmt = f'''INSERT INTO {self.network}
            (nodeHash,ip,geo,url,trustScore,version,feeData,creationTime,sync,http_code,http_msg,ssl_exp,status,last_seen, displayInfo,last_updated)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
            args = (nodeHash,ip,geo,url,trustScore,version,feeData,creationTime,sync,http_code,http_msg,ssl_exp,status,last_seen, displayInfo,last_updated)
            self.cur.execute(stmt, args)
            self.conn.commit()

        def update_node(self, nodeHash,ip,geo,url,version,feeData,creationTime: int,sync: int, http_code: int, http_msg: str, ssl_exp, status: int, last_seen: int, displayInfo, last_updated: int):
            stmt = f'''UPDATE {self.network}
            SET ip = ?,
            geo = ?,
            url = ?,
            version = ?,
            feeData = ?,
            creationTime = ?,
            sync = ?,
            http_code = ?,
            http_msg = ?,
            ssl_exp = ?,
            status = ?,
            last_seen = ?,
            displayInfo = ?,
            last_updated = ?
            WHERE nodeHash = (?)'''
            args = (ip,geo,url,version,feeData,creationTime,sync,http_code,http_msg,ssl_exp,status,last_seen,displayInfo,last_updated, nodeHash)
            self.cur.execute(stmt, args)
            self.conn.commit()

        def update_node_stats(self, nodeHash,stats):
            stmt = f'''UPDATE {self.network}
            SET stats = ?
            WHERE nodeHash = (?)'''
            args = (stats, nodeHash)
            self.cur.execute(stmt, args)
            self.conn.commit()
        
        def update_node_trustScore(self, nodeHash,trustScore):
            stmt = f'''UPDATE {self.network}
            SET trustScore = ?
            WHERE nodeHash = (?)'''
            args = (trustScore, nodeHash)
            self.cur.execute(stmt, args)
            self.conn.commit()

        def delete_item(self, chat_id):
            stmt = "DELETE FROM joined_chats WHERE chat_id = (?)"
            args = (chat_id, )
            self.conn.execute(stmt, args)
            self.conn.commit()

        def delete_item_of(self, _id, chat_id):
            stmt = "DELETE FROM joined_chats WHERE id = (?) AND chat_id = (?)"
            args = (_id, chat_id)
            self.conn.execute(stmt, args)
            self.conn.commit()

        def get_nodes(self):
            stmt = f"SELECT nodeHash, ip, url, trustScore, version, feeData, creationTime, sync, http_code, ssl_exp, status, last_seen, stats, geo, displayInfo, http_msg FROM {self.network}"
            self.cur.execute(stmt)
            if self.cur:
                return {
                    x[0]: {
                        'ip': x[1],
                        'url': x[2],
                        'trustScore': x[3],
                        'version': x[4],
                        'feeData': json.loads(x[5]),
                        'creationTime': x[6],
                        'sync': x[7],
                        'http_code': x[8],
                        'http_msg': x[15],
                        'ssl_exp': x[9],
                        'status': x[10],
                        'last_seen': x[11],
                        'stats': json.loads(x[12]) if x[12] else None,
                        'geo': json.loads(x[13]) if x[13] else None,
                        'displayInfo': json.loads(x[14]) if x[14] else None
                    } for x in self.cur
                }
            else: return None

        def get_node(self, nodeHash):
            stmt = f"SELECT ip, url, trustScore, version, feeData, creationTime, sync, http_code, ssl_exp, status, last_seen, stats, geo, displayInfo, http_msg FROM {self.network} WHERE nodeHash = (?) LIMIT 1"
            args = (nodeHash, )
            self.cur.execute(stmt, args)
            if self.cur:
                row = self.cur.fetchone()
                if row:
                    return {
                        'ip': row[0],
                        'url': row[1],
                        'trustScore': row[2],
                        'version': row[3],
                        'feeData': json.loads(row[4]),
                        'creationTime': row[5],
                        'sync': row[6],
                        'http_code': row[7],
                        'http_msg': row[14],
                        'ssl_exp': row[8],
                        'status': row[9],
                        'last_seen': row[10],
                        'stats': json.loads(row[11]) if row[11] else None,
                        'geo': json.loads(row[12]) if row[12] else None,
                        'displayInfo': json.loads(row[13]) if row[13] else None
                    }
            return None
            
        def get_item_count(self, chat_type):
            stmt = "SELECT * FROM joined_chats WHERE chat_type = (?)"
            args = (chat_type, )
            count = [x for x in self.conn.execute(stmt, args)]
            count = len(count)
            return count