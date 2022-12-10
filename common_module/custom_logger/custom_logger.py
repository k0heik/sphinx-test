import os
import json

from logging import config, getLogger, getLogRecordFactory, setLogRecordFactory


def update_record_factory(**kwargs):
    """Updating logging record factory"""

    new_attrs = kwargs
    old_factory = getLogRecordFactory()

    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        for key in new_attrs:
            setattr(record, key, new_attrs.get(key))
        return record

    # append new attributes to each message
    setLogRecordFactory(record_factory)


def init_logging(logname):
    """Initialise app logging"""

    conf_file = os.path.join(os.path.dirname(__file__), 'logger_config.json')
    with open(conf_file, 'r') as c_f:
        logname = logname if logname is not None else '__main__'
        logger = getLogger(logname)
        config.dictConfig(json.load(c_f))
        return logger
