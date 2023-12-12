''' Global constants '''

ACCEPTED_DOC_TYPES = (
    '7z', 'doc', 'docx', 'pdf',
    'tcq', 'dwg', 'odg', 'odt',
    'rar', 'rtf', 'tcq', 'txt',
    'xls', 'xlsm', 'xlsx', 'zip'
)

TIMEOUT = 10

REDIRECT_CODES = (301, 302, 303, 307, 308)
MAX_REDIRECTS = 30

# EXIT_CODES
SKIPPED = 1
UNWANTED_TYPE = 2
STORE_OK = 200
SSL_ERROR = 3
ERROR = -1

# MIN_ORDER
MIN_ORDER = {
    'insiders': 0,
    'outsiders': 0,
    'minors': 10000000
}