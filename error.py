import logging
import traceback


def server_error(e):
    logging.exception('Uncaught Exception')
    return traceback.format_exc(), 500
