import sys
try:
    import aiomysql
    import json
    from datetime import datetime
except ImportError as e:
    package_name = e.name
    print(f"Please install required package {package_name} using -> {sys.executable} -m pip install {package_name}")
    sys.exit()

class DBHelper:
    
    def __init__(self, username, password,  dbname, host, loop):
        self.dbname = dbname
        self.username = username
        self.password = password
        self.host = host
        self.loop = loop

    def __await__(self):
        async def closure():
            try:
                self.pool = await aiomysql.create_pool(
                    host=self.host,
                    port=3306,
                    user=self.username,
                    password=self.password,
                    db=self.dbname,
                    loop = self.loop
                )
            except aiomysql.Error as e:
                print(f"Error connecting to MariaDB Platform: {e}")
                sys.exit(1)
            return self
        return closure().__await__()

    async def setup(self):
        async with self.pool.acquire() as conn:
            conn.autocommit = True
            #await conn.ping(reconnect=True)
            async with conn.cursor() as cur:
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
                await cur.execute(stmt)
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
                await cur.execute(stmt)
                await conn.commit()

    def coti_node(self, network):
        _network = str(network).lower()
        return DBHelper.coti_node_(self, _network)

    class coti_node_():
        def __init__(self, parent_instance, network):
            self.parent_instance = parent_instance
            self.network = network

        def __await__(self):
            async def closure():
                self.pool = self.parent_instance.pool
                return self
            return closure().__await__()

        async def last_updated(self, human = False):
            async with self.pool.acquire() as conn:
                conn.autocommit = True
                async with conn.cursor() as cur:
                    stmt = f'''SELECT update_time
                        FROM   information_schema.tables
                        WHERE  table_schema = '{self.parent_instance.dbname}'
                            AND table_name = '{self.network}'
                    '''
                    await cur.execute(stmt)
                    if cur:
                        row = await cur.fetchone()
                        try:
                            ts = row[0].timestamp()
                            if human:
                                readable = datetime.fromtimestamp(ts).strftime('%d-%m-%y at %H:%M UTC')
                                return readable
                            return ts
                        except:
                            return None

        async def add_node(self, nodeHash,ip,geo,url,trustScore,version,feeData,creationTime,sync,http_code,http_msg,ssl_exp,status,last_seen, displayInfo,last_updated):
            async with self.pool.acquire() as conn:
                conn.autocommit = True
                async with conn.cursor() as cur:
                    stmt = f'''INSERT INTO {self.network}
                    (nodeHash,ip,geo,url,trustScore,version,feeData,creationTime,sync,http_code,http_msg,ssl_exp,status,last_seen, displayInfo,last_updated)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                    args = (nodeHash,ip,geo,url,trustScore,version,feeData,creationTime,sync,http_code,http_msg,ssl_exp,status,last_seen, displayInfo,last_updated)
                    await cur.execute(stmt, args)
                    await conn.commit()

        async def update_node(self, nodeHash,ip,geo,url,version,feeData,creationTime: int,sync: int, http_code: int, http_msg: str, ssl_exp, status: int, last_seen: int, displayInfo, last_updated: int):
            async with self.pool.acquire() as conn:
                conn.autocommit = True
                async with conn.cursor() as cur:
                    stmt = f'''UPDATE {self.network}
                    SET ip = %s,
                    geo = %s,
                    url = %s,
                    version = %s,
                    feeData = %s,
                    creationTime = %s,
                    sync = %s,
                    http_code = %s,
                    http_msg = %s,
                    ssl_exp = %s,
                    status = %s,
                    last_seen = %s,
                    displayInfo = %s,
                    last_updated = %s
                    WHERE nodeHash = %s'''
                    args = (ip,geo,url,version,feeData,creationTime,sync,http_code,http_msg,ssl_exp,status,last_seen,displayInfo,last_updated, nodeHash)
                    await cur.execute(stmt, args)
                    await conn.commit()

        async def update_node_stats(self, nodeHash,stats):
            async with self.pool.acquire() as conn:
                conn.autocommit = True                
                async with conn.cursor() as cur:
                    stmt = f'''UPDATE {self.network}
                    SET stats = %s
                    WHERE nodeHash = %s'''
                    args = (stats, nodeHash)
                    await cur.execute(stmt, args)
                    await conn.commit()
        
        async def update_node_trustScore(self, nodeHash,trustScore):
            async with self.pool.acquire() as conn:
                conn.autocommit = True            
                async with conn.cursor() as cur:
                    stmt = f'''UPDATE {self.network}
                    SET trustScore = %s
                    WHERE nodeHash = %s'''
                    args = (trustScore, nodeHash)
                    await cur.execute(stmt, args)
                    await conn.commit()

        async def get_nodes(self):
            async with self.pool.acquire() as conn:
                conn.autocommit = True            
                async with conn.cursor() as cur:
                    stmt = f"SELECT nodeHash, ip, url, trustScore, version, feeData, creationTime, sync, http_code, ssl_exp, status, last_seen, stats, geo, displayInfo, http_msg FROM {self.network}"
                    await cur.execute(stmt)
                    if cur:
                        result = {
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
                            } async for x in cur
                        }
                        return result
                    else: return None

        async def get_node(self, nodeHash):
            async with self.pool.acquire() as conn:
                conn.autocommit = True            
                async with conn.cursor() as cur:
                    stmt = f"SELECT ip, url, trustScore, version, feeData, creationTime, sync, http_code, ssl_exp, status, last_seen, stats, geo, displayInfo, http_msg FROM {self.network} WHERE nodeHash = %s LIMIT 1"
                    args = (nodeHash, )
                    await cur.execute(stmt, args)
                    if cur:
                        row = await cur.fetchone()
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
            