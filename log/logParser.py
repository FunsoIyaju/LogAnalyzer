import re
from pyspark.sql import Row

LOG_PATTERN = '^(\S+) (\S+) (\S+)'


def parse_log(log_line):
    match = re.search(LOG_PATTERN, log_line)
    if match is None:
        print("Invalid log line: %s" % log_line)
    return Row(ip_address=match.group(1),
               client=match.group(2),
               user_id=match.group(3))
