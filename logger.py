import logging

logging.basicConfig(filename='parsewiki/out.log', 
                    filemode='w', 
                    format= '[%(filename)s:%(lineno)d] %(levelname)s: %(message)s', #[%(asctime)s]
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger=logging.getLogger()
logger.addHandler(logging.StreamHandler())