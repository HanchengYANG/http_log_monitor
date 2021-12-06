import sys
from datetime import datetime
import re
import bisect
from argparse import ArgumentParser
from collections import deque

DEBUG = False
# Statistics size
STATS_SIZE = 10
# Traffic monitor window size in seconds
TRAFFIC_MON_SIZE = 120
# Critical number of hits in traffic monitor window
TRAFFIC_HITS_CRITICAL = 10 * TRAFFIC_MON_SIZE
# Traffic monitor OOO(out of order) buffer size
TRAFFIC_OOO_BUFFER_SIZE = 3
# Separator of each log line
LOG_LINE_SEP = ","
# "Request" column pattern: [http method] [/section(/maybe/other/subsections)] [PROTO]
REQ_PATTERN = re.compile(r'\w+ (/\w+)\S* \S*')


class PrintColors:
    OK = '\033[92m'
    WARN = '\033[93m'
    ERR = '\033[91m'
    ENDC = '\033[0m'


def print_dbg(msg: str):
    if DEBUG:
        print(msg)


def print_warn(msg: str):
    print(f'{PrintColors.WARN}{msg}{PrintColors.ENDC}')


def print_ok(msg: str):
    print(f'{PrintColors.OK}{msg}{PrintColors.ENDC}')


def print_err(msg: str):
    print(f'{PrintColors.ERR}{msg}{PrintColors.ENDC}')


def print_msg(msg: str):
    print(msg)


class HttpLogItem:

    def __init__(self, log_items: dict):
        # Regex match of log_item column 'request', only take section
        # 'request' format: '[http method] [/section(/maybe/other/subsections)] [PROTO]'
        self.section = REQ_PATTERN.match(log_items['request']).group(1)


class Statistics:

    def __init__(self):
        self._d = dict()  # statistic dictionary: {[section]: [hits]}
        self._ts_set = set()  # unique list of timestamp that consists statistics sample
        self.overall_hits = 0  # debug variable, overall hits of every report, should equal to the number of log lines

    def add(self, ts: int, section: str):
        self._ts_set.add(ts)
        # update "section - hits" pair
        if section in self._d.keys():
            self._d[section] += 1
        else:
            self._d[section] = 1

    def ts_exists(self, ts: int):
        return ts in self._ts_set

    def ts_count(self):
        # seconds of log lines sampled
        return len(self._ts_set)

    def reset(self):
        # remove statistics sample and remove timestamp set
        self._d = dict()
        self._ts_set = set()

    def report(self):
        count = 0
        for section, hits in self._d.items():
            if DEBUG:
                count += hits
            print_msg(f'Section: {section} hits: {hits}')
        if DEBUG:
            self.overall_hits += count
            print_dbg(f'Total hits: {count}')
            print_dbg(f'Overall hits: {self.overall_hits}')


class TrafficMonitor:

    """
    Traffic monitor receives timestamp that one hits happens (method add()).
    It creates a sliding window of configurable size in seconds. Every time
    after reception of timestamp, it will check and adjust the sliding window
    size.
    """

    def __init__(self, window_size_sec, max_hits):
        self._w_size = window_size_sec  # Sliding window size
        self._ts_list = list()          # Ordered, unique list of timestamp of sliding window
        self._d = dict()                # Dictionary {[timestamp]: [hits]}
        self._hits = 0                  # Number of hits in sliding window
        self._max = max_hits            # Critical hits in sliding window, if _hits bigger than it, we should send an alert
        self._warn = False              # If the monitor is in warning state
        # FIFO Buffer to store ts-hits pair that pops from _ts_list and _d
        self._ooo_buffer = deque(TRAFFIC_OOO_BUFFER_SIZE * [[0, 0]], TRAFFIC_OOO_BUFFER_SIZE)

    def window_size(self):
        return self._ts_list[-1] - self._ts_list[0]

    def add(self, ts: int):
        if ts not in self._ts_list:
            bisect.insort(self._ts_list, ts)  # Insert in order
        # Update hits at "ts"
        if ts in self._d.keys():
            self._d[ts] += 1
        else:
            self._d[ts] = 1
        self._hits += 1
        self.check(ts)

    def ts_exists(self, ts: int):
        return ts in self._ts_list

    def ts_count(self):
        return len(self._ts_list)

    def check(self, ts):
        # While sliding windows size, the most recent timestamp - the earliest timestamp,
        # is bigger than configured size, pop the earliest timestamp, reduce hits at
        # earliest timestamp from total hits of sliding window.
        while self.window_size() > self._w_size - 1:
            self._ooo_buffer.append([self._ts_list[0], self._d[self._ts_list[0]]])
            self._hits -= self._d[self._ts_list[0]]
            self._d.pop(self._ts_list[0])
            self._ts_list.pop(0)
        if not self._warn and ts != self._ts_list[-1]:
            # if ts is not the last (latest), it's out of order
            # if currently we are already in warn state, adding hits in the past
            # shall not affect current state
            for t in self._ts_list[self._ts_list.index(ts):-1]:
                self.disorder_check(t)
        self.check_hits(self._ts_list[-1], self._hits)

    def disorder_check(self, ts):
        for i in range(TRAFFIC_OOO_BUFFER_SIZE):
            # ooo_buffer is append at right side, so first element is the oldest
            # element in ooo_buffer is list [ts, hits]
            if 0 < ts - self._ooo_buffer[i][0] < self._w_size:
                hits_after_ts = sum([self._d[x] for x in self._ts_list[self._ts_list.index(ts) + 1:]])
                hits_ooo_buffer = 0
                for j in range(i, TRAFFIC_OOO_BUFFER_SIZE):
                    hits_ooo_buffer += self._ooo_buffer[j][1]
                hits = self._hits - hits_after_ts + hits_ooo_buffer
                self.check_hits(ts, hits)
                break

    def check_hits(self, ts, hits):
        # Check if hits larger than configured value
        if not self._warn and hits > self._max:
            self._warn = True
            print_warn(f'High traffic hits {hits} at {datetime.fromtimestamp(ts)}')
        if self._warn and hits <= self._max:
            self._warn = False
            print_warn(f'Traffic drops to {hits} at {datetime.fromtimestamp(ts)}')


class HttpLogMonitor:

    def __init__(self):
        self.col_list = list()      # CSV Column name list, line[0] of csv
        self.ts_now = None          # Timestamp of current time, always moves forward
        self.statis = Statistics()
        self.monitor = TrafficMonitor(TRAFFIC_MON_SIZE, TRAFFIC_HITS_CRITICAL)
        self.log_count = 0

    def time_update(self, ts: int):
        # Make sure time always moves forward
        if self.ts_now is None:
            self.ts_now = ts
        else:
            if ts > self.ts_now:
                self.ts_now = ts

    def feed_line(self, log_line: str):
        if len(self.col_list) == 0:  # Suppose that the first line is column names
            for i in log_line.split(LOG_LINE_SEP):
                i = i[1:-1]
                self.col_list.append(i)
            print_dbg('Log monitor initialized.')
        else:
            # Start to decode log line
            if DEBUG:
                self.log_count += 1
            d = dict()
            log_line = log_line.split(LOG_LINE_SEP)
            if len(log_line) != len(self.col_list):
                print_err('Corrupted log line, ignore')
                return
            # Read line into dictionary with corresponding column name as key
            for item_name, item_val in zip(self.col_list, log_line):
                d[item_name] = item_val.replace('"', '')
            ts = int(d['date'])
            # Decode log line into log item
            log_item = HttpLogItem(d)
            self.time_update(ts)
            # Always do size check before add new log item into statistics.
            # if current timestamp is not in statistic sample and current statis
            # size equals report size, that is to say, statis size is about to be
            # larger than configured value, do report and reset.
            if not self.statis.ts_exists(ts) and self.statis.ts_count() == STATS_SIZE:
                print_ok(f'====Statistics report at {datetime.fromtimestamp(self.ts_now)}====')
                print_dbg(f'Location: log[{self.log_count}]')
                self.statis.report()
                self.statis.reset()
            # Add log to statis
            self.statis.add(ts, log_item.section)
            # Update traffic monitor
            self.monitor.add(ts)

    def instant_statis_report(self):
        # Called when exiting from the script, print statistics even if it's not fully filled
        print_ok(f'====Instant statistics report at {datetime.fromtimestamp(self.ts_now)}====')
        print_dbg(f'Location: log[{self.log_count}]')
        self.statis.report()


if __name__ == '__main__':
    parser = ArgumentParser(description='Reads http log file from stdin.\n'
                                        'Print statistics if reads [STATIS_SIZE] seconds of log.\n'
                                        '[STATIS_SIZE] = 10 by default.\n'
                                        'Print alert if total traffic for the past [win_size](default 120) seconds\n'
                                        'exceeds [WIN_CRT_HITS](default 10) per second on average. CAUTION: First\n'
                                        'input line should be column names')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', default=False,
                        help='Print debug message')
    parser.add_argument('-s', '--stats-size', dest='statis_size', type=int, default=10,
                        help='Every [STATIS_SIZE] seconds of log lines print statis')
    parser.add_argument('-w', '--window-size', dest='win_size', type=int, default=120,
                        help='Traffic monitoring window size in seconds')
    parser.add_argument('-c', '--critical-hits', dest='win_crt_hits', type=int, default=10,
                        help='Critical hits per second for traffic monitoring')
    args = parser.parse_args()
    DEBUG = args.debug
    STATS_SIZE = args.statis_size
    TRAFFIC_MON_SIZE = args.win_size
    TRAFFIC_HITS_CRITICAL = args.win_crt_hits * TRAFFIC_MON_SIZE
    print_msg(f'Statistics size: {STATS_SIZE} seconds')
    print_msg(f'Traffic monitoring size: {TRAFFIC_MON_SIZE} seconds')
    print_msg(f'Critical traffic monitoring hits: {args.win_crt_hits} hits/sec')
    mon = HttpLogMonitor()
    try:
        for line in sys.stdin:
            mon.feed_line(line)
    except KeyboardInterrupt:
        print_warn("Quitting...")
    finally:
        if DEBUG:
            mon.instant_statis_report()
    print_dbg(f'Total log count: {mon.log_count}')
    print_dbg('Finished')
