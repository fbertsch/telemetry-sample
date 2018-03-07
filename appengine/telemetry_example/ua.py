import re


def ua_basic(ua):
    parts = ua.rsplit('/', 1)
    if len(parts) == 2:
        match = re.match('^(\d+)', parts[1])
        if match:
            return int(match.group()), None, None
    return None, None, None


def ua_keyword(kw, browser=None, os=None):
    def f(ua):
        i = ua.find(kw)
        if i > -1:
            i += len(kw)
            match = re.match('^(\d+)', ua[i:])
            if match:
                return int(match.group()), browser, os
        return None, browser, os
    return f


ua_os_matchers = [
    ('iPod', 'iPod'),
    ('iPad', 'iPad'),
    ('iPhone', 'iPhone'),
    ('Android', 'Android'),
    ('BlackBerry', 'BlackBerry'),
    ('Linux', 'Linux'),
    ('Macintosh', 'Macintosh'),
    ('Mozilla/5.0 (Mobile;', 'FirefoxOS'),
    ('Windows NT 10.0', 'Windows 10'),
    ('Windows NT 6.3', 'Windows 8.1'),
    ('Windows NT 6.2', 'Windows 8'),
    ('Windows NT 6.2', 'Windows 7'),
    ('Windows NT 6.0', 'Windows Vista'),
    ('Windows NT 5.1', 'Windows XP'),
    ('Windows NT 5.0', 'Windows 2000'),
]

ua_browser_matchers = [
    ('Edge', ua_keyword('Edge/')),
    ('Chrome', ua_keyword('Chrome/')),
    ('Opera Mini', ua_basic),
    ('Opera Mobi', ua_basic),
    ('Opera', ua_basic),
    ('MSIE', ua_keyword('MSIE ')),
    ('Trident/7.0', lambda ua: (11, 'MSIE', None)),
    ('Safari', ua_basic),
    (
        'Firefox AndroidSync',
        ua_keyword('Firefox AndroidSync ', 'FxSync', 'Android')
    ),
    ('Firefox-iOS-Sync', ua_keyword('Firefox-iOS-Sync/', 'FxSync', 'iOS')),
    ('Firefox', ua_keyword('Firefox/')),
]


def ua_parse(
    log,
    field,
    prefix='user_agent_',
    ua_os_matchers=ua_os_matchers,
    ua_browser_matchers=ua_browser_matchers,
):
    if field not in log:
        return
    ua, browser, version, os = log[field], None, None, None
    for key, f in ua_browser_matchers:
        if key in ua:
            version, browser, os = f(ua)
            if browser is None:
                browser = key
            break
    if os is None:
        for key, value in ua_os_matchers:
            if key in ua:
                os = value
                break
    if browser is not None:
        del log[field]
        log[prefix+'browser'] = browser
    if version is not None:
        log[prefix+'version'] = version
    if os is not None:
        log[prefix+'os'] = os
