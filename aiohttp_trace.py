import aiohttp


def request_tracer(results_collector):

    async def on_request_start(session, context, params):
        context.on_request_start = session.loop.time()
        context.is_redirect = False

    async def on_connection_create_start(session, context, params):
        since_start = session.loop.time() - context.on_request_start
        context.on_connection_create_start = since_start

    async def on_request_redirect(session, context, params):
        since_start = session.loop.time() - context.on_request_start
        context.on_request_redirect = since_start
        context.is_redirect = True

    async def on_dns_resolvehost_start(session, context, params):
        since_start = session.loop.time() - context.on_request_start
        context.on_dns_resolvehost_start = since_start

    async def on_dns_resolvehost_end(session, context, params):
        since_start = session.loop.time() - context.on_request_start
        context.on_dns_resolvehost_end = since_start

    async def on_connection_create_end(session, context, params):
        since_start = session.loop.time() - context.on_request_start
        context.on_connection_create_end = since_start
    
    async def on_request_chunk_sent(session, context, params):
        since_start = session.loop.time() - context.on_request_start
        context.on_request_chunk_sent = since_start

    async def on_request_end(session, context, params):
        total = session.loop.time() - context.on_request_start
        context.on_request_end = total

        dns_lookup_and_dial = context.on_dns_resolvehost_end - context.on_dns_resolvehost_start
        connect = context.on_connection_create_end - dns_lookup_and_dial
        transfer = total - context.on_connection_create_end
        is_redirect = context.is_redirect

        url = str(params.url)
        results_collector[url] = {}
        results_collector[url]['dns_lookup_and_dial'] = round(dns_lookup_and_dial * 1000, 2)
        results_collector[url]['connect'] = round(connect * 1000, 2)
        results_collector[url]['transfer'] = round(transfer * 1000, 2)
        results_collector[url]['total'] = round(total * 1000, 2)
        results_collector[url]['is_redirect'] = is_redirect

    trace_config = aiohttp.TraceConfig()

    trace_config.on_request_start.append(on_request_start)
    trace_config.on_request_redirect.append(on_request_redirect)
    trace_config.on_dns_resolvehost_start.append(on_dns_resolvehost_start)
    trace_config.on_dns_resolvehost_end.append(on_dns_resolvehost_end)
    trace_config.on_connection_create_start.append(on_connection_create_start)
    trace_config.on_connection_create_end.append(on_connection_create_end)
    trace_config.on_request_end.append(on_request_end)
    trace_config.on_request_chunk_sent.append(on_request_chunk_sent)

    return trace_config