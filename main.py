#!/usr/bin/env python
# pylint: disable=C0116,W0613
# coding=utf8
import sys

try:
    import asyncio
    import logging, time, pytz, os, json, re
    import aiohttp, aioping, ipinfo
    import ssl, OpenSSL, socket
    import urllib.parse, http.client
    from datetime import datetime, timedelta
    from random import randrange
except ImportError as e:
    package_name = e.name
    print(f"Please install required package {package_name} using -> {sys.executable} -m pip install {package_name}")
    sys.exit()

from custom_data_types import *
COTI = COTI()

from async_dbhelper import DBHelper
from aiohttp_trace import request_tracer

debug = geo_enabled = force_geo = run_once = False
connection_timeout = 10
db_name = db_user = db_pass = db_host = ipinfo_token = ""
ignore_list = verify_exempt_list = []
enabled_networks = ["MAINNET", "TESTNET"]
indexGap = 5
rate_limit = 90
rate_limit_interval = 60
max_concurrent_requests = 10
TrustScoreInterval = 1800
SSLGradesCheckInterval = (60 * 60) * 0.25#hours
SSLGradesUpdateInterval = (60 * 60 * 24) * 7#days
display_info_interval = 3600
StatsUpdateInterval = 1800
update_interval = 50
masterNodeRegEx = r"^(https:\/\/(?:mainnet-fullnode\d*|testnet-fullnode\d*).coti.io)$"

required_settings = [
    "db_name",
    "db_user",
    "db_pass",
    "db_host"
]
settings_keys = {
    "debug": bool,
    "enabled_networks": list,
    "geo_enabled": bool,
    "force_geo": bool,
    "run_once": bool,
    "connection_timeout": int,
    "db_name": str,
    "db_user": str,
    "db_pass": str,
    "db_host": str,
    "ipinfo_token": str,
    "indexGap": int,
    "rate_limit": int,
    "rate_limit_interval": int,
    "max_concurrent_requests": int,
    "TrustScoreInterval": int,
    "SSLGradesCheckInterval": int,
    "SSLGradesUpdateInterval": int,
    "display_info_interval": int,
    "StatsUpdateInterval": int,
    "update_interval": int,
    "ignore_list": list,
    "verify_exempt_list": list
}

try:
    with open("config.json") as json_data_file:
        config_file = json.load(json_data_file)
    for setting_key, setting_type in settings_keys.items():
        try:
            setting = config_file[setting_key]
            if setting_type == type(setting):
                if type(setting) == str:
                    setting = f'"{setting}"'
                exec(f"{setting_key} = {setting}")
            else:
                print(f"'{setting_key}' setting must be of type {setting_type} but got {type(setting)}. Please check your config file.")
                sys.exit()
        except KeyError:
            if setting_key in required_settings:
                print(f"'{setting_key}' setting is required but it's not found in the config file.")
                sys.exit()
        except Exception as e:
            print(e)
            sys.exit()
except Exception as err:
    err_text = f"Error reading config file. Exiting..\r\n{err}"
    print(err_text)
    sys.exit()

rate_limits = {}
cached_NodeDisplayInfo = {
    'timestamp': 0,
    'data': None
}
FinancialServers = {
    COTI.MAINNET: str(),
    COTI.TESTNET: str()
}
masterNodes = {
    COTI.MAINNET: [],
    COTI.TESTNET: []
}
TrustScoreNodeURLs = {
    COTI.MAINNET: None,
    COTI.TESTNET: None
}
httpCodeDesc = http.client.responses
http_ok_codes = [200, 201]

# Enable logging
logFormater = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

log_level = logging.DEBUG if debug else logging.INFO
logSH = logging.StreamHandler(sys.stdout)
logSH.setFormatter(logFormater)
logSH.setLevel(log_level)

logFH = logging.FileHandler("MainAPP.log")
logFH.setFormatter(logFormater)
logFH.setLevel(logging.ERROR)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logSH)
logger.addHandler(logFH)

logger.info(f"{bcolors.OKCYAN}Logger started{bcolors.ENDC} - Log Level: {log_level}")

def requestsExceptionDesc(e):
    try:
        error = e.args[0]
        try:
            while error.reason:
                error = error.reason
                if type(error) == type(str()):
                    break
                else:
                    error = error.args[0]
        except AttributeError:
            pass
        return error
    except Exception as e:
        logger.error(f"{bcolors.FAIL}SYSTEM -> {e}{bcolors.ENDC}")
        return None


async def as_completed_async(futures):
    loop = asyncio.get_event_loop()
    wrappers = []
    for fut in futures:
        assert isinstance(fut, asyncio.Future)  # we need Future or Task
        # Wrap the future in one that completes when the original does,
        # and whose result is the original future object.
        wrapper = loop.create_future()
        fut.add_done_callback(wrapper.set_result)
        wrappers.append(wrapper)

    for next_completed in asyncio.as_completed(wrappers):
        # awaiting next_completed will dereference the wrapper and get
        # the original future (which we know has completed), so we can
        # just yield that
        yield await next_completed

def chunks(list_in, n):
    # For item i in a range that is a length of l,
    for i in range(0, len(list_in), n):
        # Create an index range for l of n items:
        yield list_in[i:i+n]

async def rateLimitCheck(URL, concurrent: int = None, rl = rate_limit, rli = rate_limit_interval):
    global rate_limits
    
    if URL not in rate_limits:
        rate_limits[URL] = {
            'api_call_count': 0,
            'first_api_call_timestamp': None
        }

    if rate_limits[URL]['api_call_count'] == 0:
        logger.info(f"{bcolors.OKCYAN}[Rate-Limiter] ({URL}) First API CALL{bcolors.ENDC}")
        rate_limits[URL]['first_api_call_timestamp'] = loop.time()
    elif rate_limits[URL]['api_call_count'] >= rl:
        logger.info(f"{bcolors.WARNING}[Rate-Limiter] ({URL}) Rate limit of {rl} api calls per {rli} second/s has been reached, waiting...{bcolors.ENDC}")
        #wait until rate_limit_interval has passed since rate limit has been reached
        while loop.time() < (rate_limits[URL]['first_api_call_timestamp'] + rli):
            await asyncio.sleep(0)
        #reset counters
        logger.info(f"{bcolors.OKCYAN}[Rate-Limiter] ({URL}) Rate limit lifted!{bcolors.ENDC}")
        if rate_limits[URL]['api_call_count'] >= rl:
            rate_limits[URL]['api_call_count'] = 0
            rate_limits[URL]['first_api_call_timestamp'] = loop.time()
    elif rate_limits[URL]['api_call_count'] < rl and loop.time() > (rate_limits[URL]['first_api_call_timestamp'] + 61):
            #reset counters if rate_limit_interval has passed before hitting rate limits
            rate_limits[URL]['api_call_count'] = 0
            rate_limits[URL]['first_api_call_timestamp'] = loop.time()

    if concurrent:
        available = (rl - rate_limits[URL]['api_call_count'])
        if concurrent > available:
            rate_limits[URL]['api_call_count'] += available
            return available
        else:
            rate_limits[URL]['api_call_count'] += concurrent
            return concurrent
    else:
        rate_limits[URL]['api_call_count'] += 1


async def rateLimitFor(URL: str, list):
    elist = {x: y for x,y in enumerate(list)}
    position = 0
    remaining = len(list)
       
    while remaining > 0:
        rate = await rateLimitCheck(URL, remaining)
        
        logger.debug(f"[{URL}] Current rate = {rate} | Remaining = {remaining} | API Rate {rate_limits[URL]['api_call_count']}") 
        for i in range(rate):
            currentIndex = (position + i)
            yield currentIndex, elist[currentIndex]
        position += (i + 1)
        remaining = len(list) - position
        logger.debug(f"currentIndex {currentIndex} | len(list) {len(list)} | position {position}")
    return


async def SSLLabsRateLimitFor(list):
    elist = {x: y for x,y in enumerate(list)}
    position = 0
    remaining = len(list)
    
    async with aiohttp.ClientSession() as session:
        while remaining > 0:
            try:
                async with session.get(f'https://api.ssllabs.com/api/v3/info') as response:
                    response.raise_for_status()
                    json_response = await response.json()
                    json_response = dict(json_response)
                    maxAssessments = json_response.get('maxAssessments')
                    currentAssessments = json_response.get('currentAssessments')
                    newAssessmentCoolOff = json_response.get('newAssessmentCoolOff')
            except KeyError as key_error:
                logger.error(f"KeyError: Malformed JSON response from the server. -> {key_error}")
            except (aiohttp.ClientResponseError) as e: #HTTPError
                logger.error(f'HTTP error occurred: {e}')
                desc = f"{e.status} {e.message}"
            except ssl.SSLCertVerificationError as e: #SSLError
                logger.error(f'SSL Verification Error occurred: {e}')
                desc = e.__cause__
            except (aiohttp.ClientError) as e: #GENERIC aiohttp Exception
                logger.error(f'An error occurred: {e}')
                desc = e.__cause__
            except json.JSONDecodeError as invalid_json:
                logger.error(f"InvalidJson: Invalid JSON response from the server. -> {invalid_json}")
            except Exception as e:
                logger.error(f'An error occurred: {e}, in Line: {e.__traceback__.tb_lineno}')

            try:
                maxAssessments = maxAssessments if maxAssessments >= 1 else maxAssessments
                available = maxAssessments - currentAssessments
                if remaining > available:
                    rate = available
                else:
                    rate = remaining
                logger.debug(f"rate =  {rate}, current = {currentAssessments}")
                if rate < 1:
                    await asyncio.sleep(newAssessmentCoolOff/1000)
                    continue
                for i in range(rate):
                    currentIndex = (position + i)
                    yield currentIndex, elist[currentIndex]
                    await asyncio.sleep(newAssessmentCoolOff/1000)
                position += (i + 1)
                remaining = len(list) - position

                await asyncio.sleep(newAssessmentCoolOff/1000)
                continue
            except Exception as e:
                logger.error(f'An error occurred: {e}, in Line: {e.__traceback__.tb_lineno}')

            await asyncio.sleep(1)
        return


async def geoData(ip):
    if geo_enabled:
        try:
            details = await ipinfo_handler.getDetails(ip)
            if 'bogon' in details.all or 'anycast' in details.all:
                return None
            return details.all
        except Exception as e:
            logger.error(f"An error has occured: {e}")
            return None
    else:
        return None

async def getNodeDisplayInfo(nodeHash):
    global cached_NodeDisplayInfo

    try:
        if time.time() <= (cached_NodeDisplayInfo['timestamp'] + display_info_interval) and cached_NodeDisplayInfo['timestamp'] != 0:
            #logger.debug(f"{bcolors.WARNING}({nodeHash}) Using cached NodeDisplayData...{bcolors.ENDC}")
            json_response = cached_NodeDisplayInfo['data']
        else:
            logger.info(f"{bcolors.WARNING}({nodeHash}) Renewing NodeDisplayData...{bcolors.ENDC}")
            endpoint = "https://pay.coti.io"
            await rateLimitCheck(endpoint)
            URL = endpoint + '/nodes.json'
            async with aiohttp.ClientSession() as session:
                async with session.get(URL) as response:
                    response.raise_for_status()
                    json_response = await response.json()

            cached_NodeDisplayInfo['timestamp'] = time.time()
            cached_NodeDisplayInfo['data'] = json_response

        if nodeHash in json_response:
            display_info = json_response.get(nodeHash)

            name = display_info.get('name')

            if image := display_info.get('image'):
                image = f"https://pay.coti.io/nodes/{image}" if not image.endswith('.svg') else None
            
            return name, image
        else: return None, None
    except aiohttp.ClientResponseError as http_error:
        pass
        return None, None
    except Exception as e:
        logger.error(f"{bcolors.FAIL}{e}{bcolors.ENDC}")
        return None, None

def checkSSL(URL: str):
    try:
        socket.setdefaulttimeout(connection_timeout)
        domain = urllib.parse.urlparse(URL).netloc
        cert = ssl.get_server_certificate((domain, 443))
        x509 = OpenSSL.crypto.load_certificate(OpenSSL.crypto.FILETYPE_PEM, cert)
        bytes=x509.get_notAfter()
        timestamp = bytes.decode('utf-8')
        unix = time.mktime(datetime.strptime(timestamp, '%Y%m%d%H%M%S%z').timetuple())
        return unix
    except ssl.SSLCertVerificationError as e:
        logger.warning(f"{bcolors.FAIL}SSL Error{bcolors.ENDC} - {e} ({URL})")
        return f"SSL Error - {e}"
    except Exception as e:
        logger.warning(f"{bcolors.FAIL}{e}{bcolors.ENDC} ({URL})")
        return f"{e}"


async def getLatency(url):
    domain = urllib.parse.urlparse(url).netloc
    try:
        latency = await aioping.ping(domain) * 1000
    except Exception as e:
        logger.warning(f"{bcolors.FAIL}{e} for {domain}{bcolors.ENDC}")
        latency = None
    return latency


async def checkNodeStatus(urls, network):
    try:
        """
        logger.debug(f"{bcolors.OKCYAN}Current Master-Nodes -> {masterNodes[network]}{bcolors.ENDC}")
        for masterNodeUrl in masterNodes[network]:
            if not masterNodeUrl:
                masterNodeStatus = None
                break
            try:
                await rateLimitCheck(masterNodeUrl)
                masterNodeStatus = None
                async with aiohttp.ClientSession() as session:
                    async with session.get(masterNodeUrl + "/transaction/index") as masterNode_response:
                        masterNodeStatus = masterNode_response.status
                        masterNode_response.raise_for_status()
                        masterNode_jsonResponse = await masterNode_response.json()
                        masterNode_jsonResponse = dict(masterNode_jsonResponse)
                        if masterNode_jsonResponse.get('status') == 'Error':
                            raise Exception(f'data -> {str(masterNode_jsonResponse)}')
                        else:
                            index = masterNode_jsonResponse.get('index')
                            if not index: raise Exception(f'data -> {str(masterNode_jsonResponse)}')
                break
            except (aiohttp.ClientResponseError) as http_err:#except HTTPError as http_err:
                logger.error(f'{bcolors.FAIL}[Master-Node]{bcolors.ENDC} ({masterNodeUrl}) HTTP error occurred: {http_err}')
                continue
            except (ssl.SSLCertVerificationError) as ssl_err:#except requests.exceptions.SSLError as ssl_err:
                logger.error(f'{bcolors.FAIL}[Master-Node]{bcolors.ENDC} ({masterNodeUrl}) SSL Error occurred: {ssl_err}')
                continue
            except (aiohttp.ClientError) as e:#except requests.RequestException as e:
                logger.error(f'{bcolors.FAIL}[Master-Node]{bcolors.ENDC} ({masterNodeUrl}) A Request error occurred: {e}')
                continue
            except (json.JSONDecodeError) as e:
                logger.error(f'{bcolors.FAIL}[Master-Node]{bcolors.ENDC} ({masterNodeUrl}) Custom JsonError occurred: {e}')
                continue
            except KeyError as e:
                logger.error(f'{bcolors.FAIL}[Master-Node]{bcolors.ENDC} ({masterNodeUrl}) KeyError occurred: {e}')
                continue
            except Exception as e:
                logger.error(f'{bcolors.FAIL}[Master-Node]{bcolors.ENDC} ({masterNodeUrl}) An error occurred: {e}')
                continue
        logger.debug(f"{bcolors.OKCYAN}Using ({masterNodeUrl}) as Master-Node{bcolors.ENDC}")
        """

        logger.debug(f"{bcolors.OKCYAN}Current Financial-Server -> {FinancialServers[network]}{bcolors.ENDC}")
        FinancialServerUrl = FinancialServers[network]
        if not FinancialServerUrl:
            FinancialServerStatus = None
        index = None
        try:
            await rateLimitCheck(FinancialServerUrl)
            FinancialServerStatus = None
            async with aiohttp.ClientSession() as session:
                async with session.get(FinancialServerUrl + "/transaction/lastIndex") as FinancialServer_response:
                    FinancialServerStatus = FinancialServer_response.status
                    FinancialServer_response.raise_for_status()
                    FinancialServer_jsonResponse = await FinancialServer_response.json()
                    FinancialServer_jsonResponse = dict(FinancialServer_jsonResponse)
                    if FinancialServer_jsonResponse.get('status') == 'Error':
                        raise Exception(f'data -> {str(FinancialServer_jsonResponse)}')
                    else:
                        index = FinancialServer_jsonResponse.get('lastIndex')
                        if not index: raise Exception(f'data -> {str(FinancialServer_jsonResponse)}')
        except (aiohttp.ClientResponseError) as http_err:#except HTTPError as http_err:
            logger.error(f'{bcolors.FAIL}[Financial-Server]{bcolors.ENDC} ({FinancialServerUrl}) HTTP error occurred: {http_err}')
        except (ssl.SSLCertVerificationError) as ssl_err:#except requests.exceptions.SSLError as ssl_err:
            logger.error(f'{bcolors.FAIL}[Financial-Server]{bcolors.ENDC} ({FinancialServerUrl}) SSL Error occurred: {ssl_err}')
        except (aiohttp.ClientError) as e:#except requests.RequestException as e:
            logger.error(f'{bcolors.FAIL}[Financial-Server]{bcolors.ENDC} ({FinancialServerUrl}) A Request error occurred: {e}')
        except (json.JSONDecodeError) as e:
            logger.error(f'{bcolors.FAIL}[Financial-Server]{bcolors.ENDC} ({FinancialServerUrl}) Custom JsonError occurred: {e}')
        except KeyError as e:
            logger.error(f'{bcolors.FAIL}[Financial-Server]{bcolors.ENDC} ({FinancialServerUrl}) KeyError occurred: {e}')
        except Exception as e:
            logger.error(f'{bcolors.FAIL}[Financial-Server]{bcolors.ENDC} ({FinancialServerUrl}) An error occurred: {e}')

        logger.debug(f"{bcolors.OKCYAN}Using ({FinancialServerUrl}) as Master-Node{bcolors.ENDC}")
            
        unix_timestamp = time.time()

        checkSSL_threads = []
        checkSSL_data = {}
        for nodeHash, URL in urls.items():
            if URL in ignore_list:
                logger.info(f"{bcolors.OKBLUE}URL [{URL}] IGNORING...{bcolors.ENDC}")
                yield nodeHash, None #if in ignore list, Skip checkNodeStatus() but update the rest of the data async
                continue
            loop = asyncio.get_event_loop()
            thread = loop.run_in_executor(None, checkSSL, URL)
            checkSSL_data[thread]  = {
                'nodeHash': nodeHash,
                'url': URL
            }
            checkSSL_threads.append(thread)

        syncCheckNodes = []
        async for thread in as_completed_async(checkSSL_threads):
            SSLResult = await thread
            url = checkSSL_data[thread]['url']
            nodeHash = checkSSL_data[thread]['nodeHash']
            try:
                if type(SSLResult) == type(str()):
                    SSLExpDate = None
                    raise CustomSSLError(f"{SSLResult}")
                else:
                    SSLExpDate = SSLResult
                    if unix_timestamp > SSLExpDate:
                        if url in verify_exempt_list:
                            syncCheckNodes.append({
                                'nodeHash': nodeHash,
                                'url': url,
                                'SSLExpDate': SSLExpDate,
                                'http_msg': "SSL Cert. Expired",
                                "verify": False
                            })
                        else:
                            raise CustomSSLError(f'SSL Cert. Expired')
                    else:
                        syncCheckNodes.append({
                            'nodeHash': nodeHash,
                            'url': url,
                            'SSLExpDate': SSLExpDate,
                            'http_msg': None,
                            'verify': True if url not in verify_exempt_list else False
                        })
            except CustomSSLError as e:
                logger.debug(f'Custom SSL Error: {e} ({url})')
                #latency = await getLatency(url)
                data = (None, f"{e}", Status.Unchanged, None, SyncStatus.Unkown, SSLExpDate)
                proccesed_req = nodeHash, data
                yield proccesed_req

        trace = {}
        async def proccess_req(session: aiohttp.ClientSession, url, data, verify = True):
            nonlocal trace
            nodeHash = data.nodeHash
            SSLExpDate = data.SSLExpDate

            try:
                #start = time.perf_counter()#loop.time()
                if verify:
                    req = session.get(url, ssl = True)
                else:
                    req = session.get(url, ssl = False)

                async with req as node_response:
                    latency = trace[url]['transfer']
                    RTT = trace[url]['total']
                    del trace[url]
                    #RTT = (time.perf_counter() - start) * 1000#ms
                    httpCode = node_response.status
                    http_msg = data.http_msg if data.http_msg else httpCodeDesc[httpCode]
                    node_response.raise_for_status()
                    if FinancialServerStatus not in http_ok_codes:
                        return nodeHash, (httpCode, http_msg, nodeIndex, latency, SyncStatus.Unchanged, SSLExpDate)
                    try:
                        if httpCode in http_ok_codes:
                            node_json_response = await node_response.json()
                            node_json_response = dict(node_json_response)
                            
                            if nodeIndex := node_json_response.get('index'):
                                if index:
                                    if nodeIndex >= (index - indexGap):
                                        return nodeHash, (httpCode, http_msg, nodeIndex, latency, SyncStatus.Sync, SSLExpDate)
                                    else:
                                        return nodeHash, (httpCode, http_msg, nodeIndex,  latency, SyncStatus.Unsync, SSLExpDate)
                                else:
                                    return nodeHash, (httpCode, http_msg, nodeIndex,  latency, SyncStatus.Unchanged, SSLExpDate)
                            else:
                                return nodeHash, (httpCode, "Malfomed Json Data Received", Status.Unchanged, latency, SyncStatus.Unsync, SSLExpDate)
                        else:
                            return nodeHash, (httpCode, http_msg, Status.Unchanged, latency, SyncStatus.Unkown, SSLExpDate)
                    except TypeError as type_err:
                        logger.error(f'({url}) TypeError occurred: {type_err}')
                        return nodeHash, (httpCode, http_msg, Status.Unchanged, latency, SyncStatus.Unsync, SSLExpDate)
                    except KeyError as key_err:
                        logger.error(f'({url}) KeyError occurred: {key_err}')
                        return nodeHash, (httpCode, http_msg, Status.Unchanged, latency, SyncStatus.Unsync, SSLExpDate)
                    except json.JSONDecodeError as json_err:
                        logger.error(f'({url}) Invalid JSON data error occurred: {json_err}')
                        return nodeHash, (httpCode, "Malformed Json Data Received", Status.Unchanged, latency, SyncStatus.Unsync, SSLExpDate)
            except (aiohttp.ClientResponseError) as e: #HTTPError
                logger.warning(f'({url}) HTTP error occurred: {e}')
                return nodeHash, (httpCode, http_msg, Status.Unchanged, latency, SyncStatus.Unkown, SSLExpDate)
            except ssl.SSLCertVerificationError as e: #SSLError
                logger.warning(f'({url}) SSL Verification Error occurred: {e}')
                desc = e.__cause__
                return nodeHash, (None, f"{desc}", Status.Unchanged, latency, SyncStatus.Unkown, SSLExpDate)
            except (aiohttp.ClientError) as e: #GENERIC aiohttp Exception
                logger.warning(f'({url}) An error occurred: {e}')
                desc = e.__cause__
                return nodeHash, (None, f"{desc}", Status.Unchanged, None, SyncStatus.Unkown, SSLExpDate)
            except (asyncio.TimeoutError) as e:
                logger.warning(f'({url}) An error occurred: {bcolors.FAIL}Timeout Error{bcolors.ENDC}')
                desc = "Connection Timeout"
                return nodeHash, (None, f"{desc}", Status.Unchanged, None, SyncStatus.Unkown, SSLExpDate)
            except Exception as e:
                logger.warning(f'({url}) An error occurred: {e.__traceback__.tb_frame}, in Line {e.__traceback__.tb_lineno}')
                desc = e.__cause__
                return None

        async with aiohttp.ClientSession(trace_configs=[request_tracer(trace)], timeout=aiohttp.ClientTimeout(total=connection_timeout)) as session:
            tasks = []
            for syncCheckNode in syncCheckNodes:
                url = syncCheckNode['url'] + "/transaction/index"
                ssl_verification = syncCheckNode['verify']
                class data:
                    nodeHash = syncCheckNode['nodeHash']
                    SSLExpDate = syncCheckNode['SSLExpDate']
                    http_msg = syncCheckNode['http_msg']
                task = asyncio.create_task(proccess_req(session, url, data, verify=ssl_verification))
                tasks.append(task)
                del data, task
            
            async for proccesed_req in as_completed_async(tasks): #async
                if proccesed_req:
                    yield await proccesed_req
            #del trace

    except Exception as e:
        logger.error(f"{bcolors.FAIL}{e}{bcolors.ENDC} in Line {e.__traceback__.tb_lineno}")

async def getLastEvent(url, nodeHash):
    await rateLimitCheck(url)
    async with aiohttp.ClientSession() as session:
        data = {
            'nodeHash': nodeHash
        }
        try:
            async with session.post(url + '/statistics/nodeLastEvent', json = data) as response:
                response.raise_for_status()
                json_response = await response.json()
                json_response = dict(json_response)
                if json_response.get('status') == "Success":
                    if _lastEvent := json_response.get('lastEvent'):
                        lastEvent = {
                            "recordTime": _lastEvent.get('recordTime'),
                            "nodeStatus": _lastEvent.get('nodeStatus')
                        }
                        rdata = lastEvent
                        return rdata
        except KeyError as key_error:
            logger.error(f"KeyError: Malformed JSON response from the server. -> {key_error}")
        except (aiohttp.ClientResponseError) as e: #HTTPError
            logger.error(f'HTTP error occurred: {e} ({nodeHash})')
            desc = f"{e.status} {e.message}"
        except ssl.SSLCertVerificationError as e: #SSLError
            logger.error(f'SSL Verification Error occurred: {e}')
            desc = e.__cause__
        except (aiohttp.ClientError) as e: #GENERIC aiohttp Exception
            logger.error(f'An error occurred: {e}')
            desc = e.__cause__
        except json.JSONDecodeError as invalid_json:
            logger.error(f"InvalidJson: Invalid JSON response from the server. -> {invalid_json}")
        except Exception as e:
            logger.error(f'An error occurred: {e}')
    rdata = {
        "recordTime": None,
        "nodeStatus": None
    }
    return rdata  


async def getSSLGrade(rdata):
    domain = rdata.domain
    ipAddress = rdata.ipAddress
    network = rdata.network
    nodeHash = rdata.nodeHash
    index = rdata.index

    db = mainnet_db if network == COTI.MAINNET else testnet_db
    db_nodes = await db.get_nodes()
    db_node = db_nodes[nodeHash]

    async with aiohttp.ClientSession() as session:
        while (True):
            try:
                #await rateLimitCheck('https://api.ssllabs.com/api/v3/analyze', concurrent=None, rl = 1, rli=newAssessmentCoolOff)
                async with session.get(f'https://api.ssllabs.com/api/v3/analyze?host={domain}&startNew=on') as response:
                    response.raise_for_status()
                    json_response = await response.json()
                    json_response = dict(json_response)
                    if json_response.get('status') == "READY":
                        endpoints = json_response.get('endpoints')
                        if (endpoints):
                            for endpoint in endpoints:
                                if endpoint.get('ipAddress') == ipAddress:
                                    grade = endpoint.get('grade') if endpoint.get('grade') else None
                                    await db.update_node_SSLGrade(nodeHash, grade)
                                    logger.debug(f"{bcolors.OKCYAN}[{network}] node = ({index}) {db_node['url']}, SSLGrade = {grade}{bcolors.ENDC}")
                                    return
                        else:
                            grade = None
                            await db.update_node_SSLGrade(nodeHash, grade)
                            logger.debug(f"{bcolors.OKCYAN}[{network}] node = ({index}) {db_node['url']}, SSLGrade = {grade}{bcolors.ENDC}")
                            return
                    elif json_response.get('status') == "ERROR":
                        grade = None
                        await db.update_node_SSLGrade(nodeHash, grade)
                        logger.debug(f"{bcolors.OKCYAN}[{network}] node = ({index}) {db_node['url']}, SSLGrade = {grade}{bcolors.ENDC}")
                        return

            except KeyError as key_error:
                logger.error(f"KeyError: Malformed JSON response from the server. -> {key_error}")
            except (aiohttp.ClientResponseError) as e: #HTTPError
                desc = f"{e.status} {e.message}"
                if e.status == 503:
                    st = randrange(60,900)
                    logger.error(f'SSL Labs Service Not Available, retrying in {st/60} min.')
                    await asyncio.sleep(st)
                elif e.status == 529:
                    st = randrange(60,1800)
                    logger.error(f'SSL Labs Service Overloaded, retrying in {st/60} min.')
                    await asyncio.sleep(st)
                else:
                    logger.error(f'HTTP error occurred: {e}')
                    st = randrange(4-10)
                    await asyncio.sleep(st)
                continue
            except ssl.SSLCertVerificationError as e: #SSLError
                logger.error(f'SSL Verification Error occurred: {e}')
                desc = e.__cause__
            except (aiohttp.ClientError) as e: #GENERIC aiohttp Exception
                logger.error(f'An error occurred: {e}')
                desc = e.__cause__
            except json.JSONDecodeError as invalid_json:
                logger.error(f"InvalidJson: Invalid JSON response from the server. -> {invalid_json}")
            except Exception as e:
                logger.error(f'An error occurred: {e}, in Line: {e.__traceback__.tb_lineno}')

            return rdata

async def updateSSLGrades():
    logger.info(f"{bcolors.HEADER}{bcolors.UNDERLINE}SSL Grades Routine Started...{bcolors.ENDC}")
    while True:
        try:
            for network in ( COTI.MAINNET, COTI.TESTNET ):
                if str(network) not in enabled_networks: continue

                logger.info(f"{bcolors.OKCYAN}[{network}] Updating SSL Grades...{bcolors.ENDC}")
                db = mainnet_db if network == COTI.MAINNET else testnet_db
                db_nodes = await db.get_nodes()
                nodeHashes = list((x for x in db_nodes if db_nodes[x]['status'] == 1))

                lastUpdated = await db.get_sslgrades_lu()
                lastUpdated = lastUpdated if lastUpdated else 0

                if (time.time() >= (lastUpdated + SSLGradesUpdateInterval)):
                    logger.info(f"{bcolors.OKCYAN}[{network}] Requesting New SSL Reports...{bcolors.ENDC}")

                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.get(f'https://api.ssllabs.com/api/v3/info') as response:
                                response.raise_for_status()
                                json_response = await response.json()
                                json_response = dict(json_response)
                                #maxAssessments = json_response.get('maxAssessments')
                                #currentAssessments = json_response.get('currentAssessments')
                                newAssessmentCoolOff = json_response.get('newAssessmentCoolOff')
                    except KeyError as key_error:
                        logger.error(f"KeyError: Malformed JSON response from the server. -> {key_error}")
                        await asyncio.sleep(10)
                        continue
                    except (aiohttp.ClientResponseError) as e: #HTTPError
                        desc = f"{e.status} {e.message}"
                        if e.status == 503:
                            st = randrange(60,900)
                            logger.error(f'SSL Labs Service Not Available, retrying in {st/60} min.')
                            await asyncio.sleep(st)
                        elif e.status == 529:
                            st = randrange(60,1800)
                            logger.error(f'SSL Labs Service Overloaded, retrying in {st/60} min.')
                            await asyncio.sleep(st)
                        else:
                            logger.error(f'HTTP error occurred: {e}')
                            st = randrange(4-10)
                            await asyncio.sleep(st)
                        continue
                    except ssl.SSLCertVerificationError as e: #SSLError
                        logger.error(f'SSL Verification Error occurred: {e}')
                        desc = e.__cause__
                        await asyncio.sleep(10)
                        continue
                    except (aiohttp.ClientError) as e: #GENERIC aiohttp Exception
                        logger.error(f'An error occurred: {e}')
                        desc = e.__cause__
                        await asyncio.sleep(10)
                        continue
                    except json.JSONDecodeError as invalid_json:
                        logger.error(f"InvalidJson: Invalid JSON response from the server. -> {invalid_json}")
                        await asyncio.sleep(10)
                        continue
                    except Exception as e:
                        logger.error(f'An error occurred: {e}, in Line: {e.__traceback__.tb_lineno}')
                        await asyncio.sleep(10)
                        continue

                    pending_reports = []
                    async for i, nodeHash in SSLLabsRateLimitFor(nodeHashes):
                        logger.debug(f"Requesting Report -> ({i+1}) {db_nodes[nodeHash]['url']}")
                        class report:
                            pass
                        report.nodeHash = nodeHash
                        report.network = network
                        ipAddress = db_nodes[nodeHash]['ip']
                        url = db_nodes[nodeHash]['url']
                        domain = urllib.parse.urlparse(url).netloc
                        report.domain = domain
                        report.ipAddress = ipAddress
                        report.index = i
                        result = await getSSLGrade(report)
                        if result: pending_reports.append(result)
                        del report, ipAddress, url, domain

                    await db.update_sslgrades_lu()
                else:
                    logger.info(f"{bcolors.OKCYAN}[{network}] Requesting Cached SSL Reports...{bcolors.ENDC}")
                    logger.debug(f"SSLGradesUpdateInterval=[{SSLGradesUpdateInterval / 60 / 60} hours] hasn't passed since last check. Using Cached Reports")
                    pending_reports = []
                    for nodeHash in nodeHashes:
                        class report:
                            pass
                        ipAddress = db_nodes[nodeHash]['ip']
                        url = db_nodes[nodeHash]['url']
                        domain = urllib.parse.urlparse(url).netloc
                        report.nodeHash = nodeHash
                        report.domain = domain
                        report.ipAddress = ipAddress
                        pending_reports.append(report)
                        del report, ipAddress, url, domain

                index = 0
                while (len(pending_reports) >= 1):
                    for report in pending_reports:
                        nodeHash = report.nodeHash
                        db_node = db_nodes[nodeHash]
                        domain = report.domain
                        ipAddress = report.ipAddress
                        index += 1

                        try:
                            async with aiohttp.ClientSession() as session:
                                async with session.get(f'https://api.ssllabs.com/api/v3/getEndpointData?host={domain}&s={ipAddress}&fromCache=on') as response:
                                    response.raise_for_status()
                                    json_response = await response.json()
                                    json_response = dict(json_response)
                                    if json_response.get('statusMessage') == "Ready":
                                        grade = json_response.get('grade')
                                        grade = grade if grade else None
                                        logger.debug(f"{bcolors.OKCYAN}[{network}] node = ({index}) {db_node['url']}, SSLGrade = {grade}{bcolors.ENDC}")
                                        await db.update_node_SSLGrade(nodeHash, grade)
                                        pending_reports.remove(report)
                                    elif json_response.get('statusMessage') == "In progress":
                                        #In Proggress, check again later
                                        logger.debug(f"{bcolors.OKCYAN}[{network}] node = ({index}) {db_node['url']}, In Progress...")
                                        pass
                                    elif json_response.get('errors'):
                                        grade = None
                                        #await db.update_node_SSLGrade(nodeHash, grade)
                                        logger.debug(f"{bcolors.OKCYAN}[{network}] node = ({index}) {db_node['url']}, SSLGrade = {grade}{bcolors.ENDC}, Response: {json_response.get('errors')}")
                                        pending_reports.remove(report)
                                    else:
                                        grade = None
                                        #await db.update_node_SSLGrade(nodeHash, grade)
                                        logger.debug(f"{bcolors.OKCYAN}[{network}] node = ({index}) {db_node['url']}, SSLGrade = {grade}{bcolors.ENDC}, Response: {json_response.get('statusMessage')}")
                                        pending_reports.remove(report)

                        except KeyError as key_error:
                            logger.error(f"KeyError: Malformed JSON response from the server. -> {key_error}")
                        except (aiohttp.ClientResponseError) as e: #HTTPError
                            desc = f"{e.status} {e.message}"
                            if e.status == 503:
                                st = randrange(60,900)
                                logger.error(f'SSL Labs Service Not Available, retrying in {st/60} min.')
                                await asyncio.sleep(st)
                            elif e.status == 529:
                                st = randrange(60,1800)
                                logger.error(f'SSL Labs Service Overloaded, retrying in {st/60} min.')
                                await asyncio.sleep(st)
                            else:
                                logger.error(f'HTTP error occurred: {e}')
                                st = randrange(4-10)
                                await asyncio.sleep(st)
                            continue
                        except ssl.SSLCertVerificationError as e: #SSLError
                            logger.error(f'SSL Verification Error occurred: {e}')
                            desc = e.__cause__
                        except (aiohttp.ClientError) as e: #GENERIC aiohttp Exception
                            logger.error(f'An error occurred: {e}')
                            desc = e.__cause__
                        except json.JSONDecodeError as invalid_json:
                            logger.error(f"InvalidJson: Invalid JSON response from the server. -> {invalid_json}")
                        except Exception as e:
                            logger.error(f'An error occurred: {e}, in Line: {e.__traceback__.tb_lineno}')
                        
                    logger.debug(f"REPORTS LEFT -> {len(pending_reports)}")
                    #continue to next after cool off
                    await asyncio.sleep(1)
                
                #done
                logger.info(f"{bcolors.OKCYAN}[{network}] SSL Grades Updated!{bcolors.ENDC}")
        
            if run_once: break
            await asyncio.sleep(SSLGradesCheckInterval)
        except Exception as e:
            logger.error(f'An error occurred: {e}, in Line: {e.__traceback__.tb_lineno}')


async def getTrustScore(url, rdata):
    userHash = rdata.nodeHash

    async with aiohttp.ClientSession() as session:
        data = {
            'userHash': userHash
        }
        try:
            async with session.post(url + '/usertrustscore', json = data) as response:
                response.raise_for_status()
                json_response = await response.json()
                json_response = dict(json_response)
                if json_response.get('status') == "Success":
                    trustScore = json_response.get('trustScore')
                    trustScore = trustScore if trustScore else 0.0
                    rdata.trustScore = trustScore
                else:
                    rdata.trustScore = 0.0
                return rdata
        except KeyError as key_error:
            logger.error(f"KeyError: Malformed JSON response from the server. -> {key_error}")
        except (aiohttp.ClientResponseError) as e: #HTTPError
            logger.error(f'HTTP error occurred: {e}')
            desc = f"{e.status} {e.message}"
        except ssl.SSLCertVerificationError as e: #SSLError
            logger.error(f'SSL Verification Error occurred: {e}')
            desc = e.__cause__
        except (aiohttp.ClientError) as e: #GENERIC aiohttp Exception
            logger.error(f'An error occurred: {e}')
            desc = e.__cause__
        except json.JSONDecodeError as invalid_json:
            logger.error(f"InvalidJson: Invalid JSON response from the server. -> {invalid_json}")
        except Exception as e:
            logger.error(f'An error occurred: {e}')
    rdata.trustScore = 0.0
    return rdata


async def updateTrustScores():
    logger.info(f"{bcolors.HEADER}{bcolors.UNDERLINE}TrustScores Routine Started...{bcolors.ENDC}")
    while True:
        for network in TrustScoreNodeURLs:
            if str(network) not in enabled_networks: continue

            while not TrustScoreNodeURLs[network]:
                await asyncio.sleep(0)
            URL = TrustScoreNodeURLs[network]
            logger.debug(f"Current [{network}] -> {URL}")
            logger.info(f"{bcolors.OKCYAN}[{network}] Updating TrustScores...{bcolors.ENDC}")
            db = mainnet_db if network == COTI.MAINNET else testnet_db
            db_nodes = await db.get_nodes()
            nodeHashes = list((x for x in db_nodes))
            nodeHashesChuncks = list(chunks(nodeHashes,max_concurrent_requests))
            
            tasks = []

            for nodeHashesChunck in nodeHashesChuncks:
                async for i, nodeHash in rateLimitFor(URL, nodeHashesChunck):
                        class data:
                            pass
                        data.nodeHash = nodeHash
                        task = asyncio.create_task(getTrustScore(URL, data))
                        tasks.append(task)
                        del data

                async for data in as_completed_async(tasks):
                    data = await data
                    trustScore = data.trustScore
                    nodeHash = data.nodeHash
                    if not trustScore: trustScore = db_nodes[nodeHash]['trustScore']
                    await db.update_node_trustScore(nodeHash, trustScore)

            logger.info(f"{bcolors.OKCYAN}[{network}] TrustScores Updated!{bcolors.ENDC}")
        if run_once: break
        await asyncio.sleep(TrustScoreInterval)


async def updateStats():
    try:
        URLs = {
            COTI.MAINNET: "https://mainnet-nodemanager.coti.io",
            COTI.TESTNET: "https://testnet-nodemanager.coti.io"
        }

        for network, URL in URLs.items():
            if str(network) not in enabled_networks: continue

            db = mainnet_db if network == COTI.MAINNET else testnet_db
            db_nodes = await db.get_nodes()

            logger.info(f"{bcolors.OKCYAN}[{network}] Updating stats...{bcolors.ENDC}")
            stats = {db_nodeHash: {
                'total': None,
                365: None,
                180: None,
                90: None,
                30: None,
                14: None,
                7: None,
                1: None
            } for db_nodeHash in db_nodes}

            if db_nodes:
                days = ['total', 365, 180, 90, 30, 14, 7, 1]

                async def proccess_stat(session, url, data, day):
                    try:
                        async with session.post(url + '/statistics/totalsByPercentageNodes', json = data) as stat_response:
                            stat_response.raise_for_status()
                            jsonResponse = await stat_response.json()
                            if jsonResponse["status"] == "Success":
                                nodeHashToActivityPercentage = jsonResponse["nodeHashToActivityPercentage"]
                                for nodeHash in nodeHashToActivityPercentage:
                                    stats[nodeHash][day] = nodeHashToActivityPercentage[nodeHash]["percentage"]
                                return True
                    except (aiohttp.ClientResponseError) as e:
                        url = e.request_info.url
                        desc = (f"custom HTTP error -> {e.status} {e.message} ({url})")
                        logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                        logger.info(f"DATA Sent during error -> {data}")
                    except (ssl.SSLCertVerificationError) as e:
                        desc = (f"custom SSL Error -> {e.__cause__} ({URL})")
                        logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                    except (aiohttp.ClientError) as e:
                        url = url
                        desc = (f"Connection Error -> {e.__cause__} ({url})")
                        logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                    except (json.JSONDecodeError) as e:
                        desc = (f"Custom JSON Error -> {e} ({URL})")
                        logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                    except Exception as e:
                        logger.error(e)
                    return False

                async with aiohttp.ClientSession() as session:
                    stats_tasks = []
                    nodeHashes = [db_nodeHash for db_nodeHash in db_nodes if db_nodeHash != "b6de2e6a6dab22da37d9e55f8d97ff0ecad1c792019ca6d3a8e0ce35d6f4f022ec48f277edf9912652525ab829380e39c5bd0fba07d0caaea04ea6488eeaf66c"]
                    nodeHashChunks = list(chunks(nodeHashes,max_concurrent_requests))
                    
                    #async for i, day in rateLimitFor(URL, days):
                    for day in days:
                        for nodeHashChunk in nodeHashChunks:
                            #print(f"[{network}] Chunk of {len(nodeHashChunk)}\n")
                            data = {
                                "nodeHashes": nodeHashChunk,
                                "startDate": "1000-01-01T00:00:00.0000" if day == 'total' else (datetime.now() - timedelta(days=day)).strftime("%Y-%m-%dT%H:%M:%S.%f"),
                                "endDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                            }
                            
                            await rateLimitCheck(URL)
                            await proccess_stat(session, URL, data, day)
                            await asyncio.sleep(1)
                            #stats_task = asyncio.ensure_future(proccess_stat(session, URL, data, day))
                            #stats_tasks.append(stats_task)
                        #print(f"[{network}]  --day {day} done--")

                        #await asyncio.gather(*stats_tasks)

                for nodeHash in stats:
                    try:
                        await db.update_node_stats(nodeHash, json.dumps(stats[nodeHash]))
                    except Exception as e:
                        logger.error(f"{bcolors.FAIL}{e}{bcolors.ENDC}",f" on line {e.__traceback__.tb_lineno}")
            await db.update_lu()
            logger.info(f"{bcolors.OKCYAN}[{network}] Stats Updated!{bcolors.ENDC}")
        
        await asyncio.sleep(StatsUpdateInterval)
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logger.error(f"{bcolors.FAIL}{(e, exc_type, exc_tb.tb_lineno)}{bcolors.ENDC}")
        await asyncio.sleep(update_interval)

async def cacheNodes():
    global TrustScoreNodeURLs, FinancialServers
    logger.info(f"{bcolors.HEADER}{bcolors.UNDERLINE}Caching nodes...{bcolors.ENDC}")

    while 1:
        start_time = time.time()
        try:
            URLs = {
                COTI.MAINNET: "https://mainnet-nodemanager.coti.io",
                COTI.TESTNET: "https://testnet-nodemanager.coti.io"
            }
            masterNodes[COTI.MAINNET].clear()
            masterNodes[COTI.TESTNET].clear()
            for network, URL in URLs.items():
                if str(network) not in enabled_networks: continue

                try:
                    db = mainnet_db if network == COTI.MAINNET else testnet_db

                    await rateLimitCheck(URL)
                    async with aiohttp.ClientSession() as session:
                        async with session.get(URL + '/management/full_network/verbose') as response:
                            response.raise_for_status()
                            textResponse = await response.text()
                            jsonResponse = json.loads(textResponse)
                            jsonResponse = dict(jsonResponse)

                    db_nodes = await db.get_nodes()

                    if jsonResponse.get('status') == "Success":
                        if networkData := jsonResponse.get('networkData'):
                            if singleNodeNetworkDataMap := dict(networkData).get('singleNodeNetworkDataMap'):
                                if FinancialServer := dict(singleNodeNetworkDataMap).get('FinancialServer'):
                                    FinancialServers[network] = dict(FinancialServer).get("webServerUrl")
                                else: raise Exception(f"Malformed Data Received from {URL} -> {str(jsonResponse)}")
                            else:
                                raise Exception(f"Malformed Data Received from {URL} -> {str(jsonResponse)}")
                            if multipleNodeMaps := dict(networkData).get('multipleNodeMaps'):
                                if FullNodes := dict(multipleNodeMaps).get('FullNode'):
                                    pass
                                else: raise Exception(f"Malformed Data Received from {URL} -> {str(jsonResponse)}")
                            else:
                                raise Exception(f"Malformed Data Received from {URL} -> {str(jsonResponse)}")
                        else:
                            raise Exception(f"Malformed Data Received from {URL} -> {str(jsonResponse)}")

                        for nodeHash, nodeData in FullNodes.items():
                            if re.match(masterNodeRegEx, dict(nodeData).get('webServerUrl')): masterNodes[network].append(dict(nodeData).get('webServerUrl'))
                        
                        if TrustScoreNodes := multipleNodeMaps.get('TrustScoreNode'):
                            #get the url of the first trustscore node
                            TrustScoreNodeURL = None
                            try:
                                for TrustScoreNode in TrustScoreNodes: TrustScoreNodeURL = TrustScoreNodes[TrustScoreNode]['webServerUrl']; break
                            except Exception as e:
                                logger.error(f"Error Extracting TrustScore Node URL -> {e}")
                            TrustScoreNodeURLs[network] = TrustScoreNodeURL
                        
                        #check whether our nodes in the database exist on the Node Manager List, if not,
                        #we update their status.
                        if db_nodes:
                            urls = {}
                            for db_node, db_node_data in db_nodes.items():
                                if db_node not in FullNodes:
                                    last_seen = db_node_data['last_seen'] if db_node_data['last_seen'] else 0
                                    if datetime.timestamp(datetime.now()) <= (float(last_seen) + 3600):#86400):
                                        urls[db_node] = db_node_data["url"]
                                    else:
                                        logger.debug(f"{bcolors.OKBLUE}URL [{db_node_data['url']}] last seen longer than a hour ago, ignoring...{bcolors.ENDC}")
                                        status = 0
                                        http_code = db_node_data['http_code']
                                        http_msg = db_node_data['http_msg']
                                        last_index = db_node_data['last_index']
                                        last_event = db_node_data['last_event'] if status == db_node_data['status'] else time.time()
                                        latency = db_node_data['latency']
                                        sync = db_node_data['sync']
                                        ssl_exp = db_node_data['ssl_exp']

                                        if force_geo:
                                            try:
                                                geo_data = await geoData(db_node_data['ip'])
                                                geo_data = json.dumps(geo_data)
                                            except Exception as e:
                                                geo_data = None
                                        else:
                                            geo_data = json.dumps(db_node_data['geo'])

                                        name, image = await getNodeDisplayInfo(db_node)
                                        displayInfo = {
                                            'name': name,
                                            'image': image
                                        }

                                        last_updated = int(time.time())
                                        await db.update_node(
                                            db_node, db_node_data["ip"], geo_data, db_node_data["url"], db_node_data["version"],
                                            json.dumps(db_node_data['feeData']), db_node_data["creationTime"],
                                            sync, http_code, http_msg, last_index, latency, ssl_exp, status, db_node_data['last_seen'], last_event, json.dumps(displayInfo),last_updated
                                        )
                                        del last_seen, sync, http_code, ssl_exp, geo_data, displayInfo, db_node, db_node_data
                            if urls:
                                async for proccessed_req in checkNodeStatus(urls, network):
                                    if proccessed_req:
                                        nodeHash, nodeResult = proccessed_req
                                        db_node = nodeHash
                                        db_node_data = db_nodes[db_node]
                                        status = 0
                                        last_event = db_node_data['last_event'] if status == db_node_data['status'] else time.time()
                                        #lastEvent = await getLastEvent(URL, nodeHash)
                                    else:
                                        logger.debug(f"No data receveid from checkNodeStatus() = {proccessed_req}")
                                        break
                                    #update node status in db
                                    if nodeResult:
                                        http_code, http_msg, last_index, latency, sync, ssl_exp = nodeResult
                                    else:
                                        http_code, http_msg, last_index, latency, sync, ssl_exp = (None, 'Skipped: In ignore list', None, None, SyncStatus.Unkown, None)
                                    
                                    if force_geo:
                                        try:
                                            geo_data = await geoData(db_node_data['ip'])
                                            geo_data = json.dumps(geo_data)
                                        except Exception as e:
                                            geo_data = None
                                    else:
                                        geo_data = json.dumps(db_node_data['geo'])

                                    name, image = await getNodeDisplayInfo(db_node)
                                    displayInfo = {
                                        'name': name,
                                        'image': image
                                    }

                                    last_updated = int(time.time())
                                    await db.update_node(
                                        db_node, db_node_data["ip"], geo_data, db_node_data["url"], db_node_data["version"],
                                        json.dumps(db_node_data['feeData']), db_node_data["creationTime"],
                                        sync, http_code, http_msg, last_index, latency, ssl_exp, status, db_node_data['last_seen'], last_event, json.dumps(displayInfo),last_updated
                                    )
                                    del sync, http_code, last_index, ssl_exp, geo_data, displayInfo, db_node, db_node_data
                            del urls

                        urls = {}
                        for nodeHash in FullNodes:
                            node = dict(FullNodes[nodeHash])
                            urls[nodeHash] = node.get("webServerUrl")
                        if urls:
                            async for proccessed_req in checkNodeStatus(urls, network):
                                if proccessed_req:
                                    nodeHash, nodeResult = proccessed_req
                                    status = 1
                                    last_event = time.time()
                                    #lastEvent = await getLastEvent(URL, nodeHash)
                                else:
                                    logger.debug(f"No data receveid from checkNodeStatus() = {proccessed_req}")
                                    break
                                node = dict(FullNodes[nodeHash])
                                last_seen = datetime.timestamp(datetime.now())

                                if nodeResult:
                                    http_code, http_msg, last_index, latency, sync, ssl_exp = nodeResult
                                else:
                                    http_code, http_msg, last_index, latency, sync, ssl_exp = (None, 'Skipped: In ignore list', None, None, SyncStatus.Unkown, None)
                                sync = sync if sync != None else SyncStatus.Unkown

                                name, image = await getNodeDisplayInfo(nodeHash)
                                displayInfo = {
                                    'name': name,
                                    'image': image
                                }

                                #check if a nodeHash exists in our database, if not, we add it
                                if nodeHash not in db_nodes:
                                    try:
                                        geo_data = await geoData(node.get('address'))
                                        geo_data = json.dumps(geo_data)
                                    except:
                                        geo_data = None

                                    trustScore = 0.0

                                    last_updated = int(time.time())
                                    await db.add_node(
                                        nodeHash, node.get("address"), geo_data, node.get("webServerUrl"), trustScore, node.get("version"),
                                        json.dumps(node.get("feeData")), dict(node.get("nodeRegistrationData")).get("creationTime"),
                                        sync, http_code, http_msg, last_index, latency, ssl_exp, status, last_seen, last_event, json.dumps(displayInfo),last_updated
                                    )
                                else:
                                    #update it
                                    db_node = db_nodes[nodeHash]
                                    last_event = db_node['last_event'] if status == db_node['status'] else time.time()
                                    if db_node['ip'] != node["address"] or force_geo:
                                        try:
                                            geo_data = await geoData(node['address'])
                                            geo_data = json.dumps(geo_data)
                                        except:
                                            geo_data = None
                                    else:
                                        geo_data = json.dumps(db_node['geo'])

                                    sync = sync if sync != None else node["sync"]
                                    last_index = last_index if last_index != None else db_node['last_index']

                                    last_updated = int(time.time())
                                    await db.update_node(
                                        nodeHash, node.get("address"), geo_data, node.get("webServerUrl"), node.get("version"),
                                        json.dumps(node.get("feeData")), dict(node.get("nodeRegistrationData")).get("creationTime"),
                                        sync, http_code, http_msg, last_index, latency, ssl_exp, status, last_seen, last_event, json.dumps(displayInfo),last_updated
                                    )
                    else:
                        pass

                    """logger.info(f"{bcolors.OKCYAN}[{network}] Updating stats...{bcolors.ENDC}")
                    stats = {db_nodeHash: {
                        'total': None,
                        365: None,
                        180: None,
                        90: None,
                        30: None,
                        14: None,
                        7: None,
                        1: None
                    } for db_nodeHash in db_nodes}

                    if db_nodes:
                        days = ['total', 365, 180, 90, 30, 14, 7, 1]

                        async def proccess_stat(session, url, data, day):
                            try:
                                async with session.post(url + '/statistics/totalsByPercentageNodes', json = data) as stat_response:
                                    stat_response.raise_for_status()
                                    jsonResponse = await stat_response.json()
                                    if jsonResponse["status"] == "Success":
                                        nodeHashToActivityPercentage = jsonResponse["nodeHashToActivityPercentage"]
                                        for nodeHash in nodeHashToActivityPercentage:
                                            stats[nodeHash][day] = nodeHashToActivityPercentage[nodeHash]["percentage"]
                                        return True
                            except (aiohttp.ClientResponseError) as e:
                                url = e.request_info.url
                                desc = (f"custom HTTP error -> {e.status} {e.message} ({url})")
                                logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                            except (ssl.SSLCertVerificationError) as e:
                                desc = (f"custom SSL Error -> {e.__cause__} ({URL})")
                                logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                            except (aiohttp.ClientError) as e:
                                url = url
                                desc = (f"Connection Error -> {e.__cause__} ({url})")
                                logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                            except (json.JSONDecodeError) as e:
                                desc = (f"Custom JSON Error -> {e} ({URL})")
                                logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                            except Exception as e:
                                logger.error(e)
                            return False

                        async with aiohttp.ClientSession() as session:
                            stats_tasks = []
                            nodeHashes = [db_nodeHash for db_nodeHash in db_nodes]
                            nodeHashChunks = list(chunks(nodeHashes,50))
                            
                            #async for i, day in rateLimitFor(URL, days):
                            for day in days:
                                for nodeHashChunk in nodeHashChunks:
                                    print(f"Chunk of {len(nodeHashChunk)} -> {nodeHashChunk}\n")
                                    data = {
                                        "nodeHashes": nodeHashChunk,
                                        "startDate": "1000-01-01T00:00:00.0000" if day == 'total' else (datetime.now() - timedelta(days=day)).strftime("%Y-%m-%dT%H:%M:%S.%f"),
                                        "endDate": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")
                                    }
                                    
                                    await rateLimitCheck(URL)
                                    await proccess_stat(session, URL, data, day)
                                    await asyncio.sleep(1)
                                    #stats_task = asyncio.ensure_future(proccess_stat(session, URL, data, day))
                                    #stats_tasks.append(stats_task)

                                #await asyncio.gather(*stats_tasks)

                        for nodeHash in stats:
                            try:
                                await db.update_node_stats(nodeHash, json.dumps(stats[nodeHash]))
                            except Exception as e:
                                logger.error(f"{bcolors.FAIL}{e}{bcolors.ENDC}",f" on line {e.__traceback__.tb_lineno}")
                    logger.info(f"{bcolors.OKCYAN}[{network}] Stats Updated!{bcolors.ENDC}")"""
                    await db.update_lu()
                except (aiohttp.ClientResponseError) as e:
                    url = e.request_info.url
                    desc = (f"custom HTTP error -> {e.status} {e.message} ({url})")
                    logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                except (ssl.SSLCertVerificationError) as e:
                    desc = (f"custom SSL Error -> {e.__cause__} ({URL})")
                    logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                except (aiohttp.ClientError) as e:
                    url = URL
                    desc = (f"Connection Error -> {e.__cause__} ({url})")
                    logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                except (json.JSONDecodeError) as e:
                    desc = (f"Custom JSON Error -> {e} ({URL})")
                    logger.error(f"{bcolors.FAIL}{desc}{bcolors.ENDC}")
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    logger.error(f"{bcolors.FAIL}{(e, exc_type, exc_tb.tb_lineno)}{bcolors.ENDC}")

            last_updated = datetime.now(tz=pytz.UTC).strftime("%d-%m-%y, %H:%M")
            logger.info(f"{bcolors.OKGREEN}Last Updated: {last_updated} | Time Elapsed: {(time.time() - start_time)} seconds.{bcolors.ENDC}")
            if run_once: break

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            logger.error(f"{bcolors.FAIL}{(e, exc_type, exc_tb.tb_lineno)}{bcolors.ENDC}")
            
        await asyncio.sleep(update_interval)
        logger.info(f"{bcolors.HEADER}{bcolors.UNDERLINE}Starting over...{bcolors.ENDC}")

async def main():
    global mainnet_db, testnet_db, ipinfo_handler, loop

    loop = asyncio.get_event_loop()

    db = DBHelper(db_user,db_pass, db_name, db_host, loop)
    await db
    await db.setup()

    mainnet_db = db.coti_node(COTI.MAINNET)
    testnet_db = db.coti_node(COTI.TESTNET)
    await mainnet_db
    await testnet_db

    if geo_enabled:
        ipinfo_handler = ipinfo.getHandlerAsync(ipinfo_token)

    tasks = []
    tasks.append(loop.create_task(cacheNodes()))
    tasks.append(loop.create_task(updateStats()))
    tasks.append(loop.create_task(updateTrustScores()))
    tasks.append(loop.create_task(updateSSLGrades()))

    await asyncio.gather(*tasks)
    loop.stop()
    logger.info("All tasks have closed. Exiting...")

try:
    asyncio.run(main())
    
except Exception as e:
    logger.error(e, e.__traceback__.tb_lineno)