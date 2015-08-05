import os
import logging
import logging.handlers

def init_logs(name):
    if not os.path.exists('logs'):
        os.makedirs('logs')
    logger = logging.getLogger(name)
    logger.propagate = False
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if os.environ.get('DEBUG_MODE') == 'true':
        ch = logging.StreamHandler()
	ch.setLevel(logging.INFO)
	ch.setFormatter(formatter)
	logger.addHandler(ch)
	return logger
    else:
	log_file = 'logs/{}.log'.format(name)
	file_handler = logging.handlers.RotatingFileHandler(
              log_file, maxBytes=1000000, backupCount=2)
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
	logger.addHandler(file_handler)
	return logger
