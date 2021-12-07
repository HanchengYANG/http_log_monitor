import sys
from datetime import datetime
import re
import bisect
from argparse import ArgumentParser
from collections import deque


class HttpLogMonitor:
    class HttpLogItem:
        """
        Class to parse log line for convince.
        """
        def __init__(self, monitor, log_items: dict):
            # Regex match of log_item column 'request', only take section
            # 'request' format: '[http method] [/section(/maybe/other/subsections)] [PROTO]'
            self.section = monitor.req_pattern.match(log_items['request']).group(1)

    class Statistics:
        """
        Class records timestamp and section of a hit, and print statistic
        report when necessary.
        """
        def __init__(self, monitor, period, report_handler=None):
            self._d = dict()  # statistic dictionary: {[section]: [hits]}
            self._ts_set = set()  # unique list of timestamp that consists statistics sample
            self._period = period
            self._mon = monitor
            self._debug = monitor.debug
            self.overall_hits = 0  # debug variable, overall hits of every report
            self.report_handler = report_handler

        def add(self, ts: int, section: str):
            # Always do size check before add new log item into statistics.
            # if current timestamp is not in statistic sample and current statis
            # size equals report size, that is to say, statis size is about to be
            # larger than configured value, do report and reset.
            if not self.ts_exists(ts) and self.ts_count() == self._period:
                self._mon.print_ok(f'====Statistics report at {datetime.fromtimestamp(self._mon.ts_now)}====')
                self.report()
                self.reset()
            self._ts_set.add(ts)
            # update "section - hits" pair
            if self._d.get(section) is not None:
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
                if self._debug:
                    count += hits
                self._mon.print_msg(f'Section: {section} hits: {hits}')
            if callable(self.report_handler):
                self.report_handler(self._d)
            if self._debug:
                self.overall_hits += count
                self._mon.print_dbg(f'Total hits: {count}')
                self._mon.print_dbg(f'Overall hits: {self.overall_hits}')

    class TrafficMonitor:
        """
        Traffic monitor receives timestamp that one hits happens (method add()).
        It creates a sliding window of configurable size in seconds. Every time
        after reception of timestamp, it will check and adjust the sliding window
        size. Additionally, there's a buffer for logs out of order (OOO), the earlier
        log which is popped out from sliding windows will be pushed in this buffer,
        in case that OOO log arrives and requires sliding window to reverse.
        """

        def __init__(self, monitor, window_size_sec, critical_rate, ooo_buffer_size, alert_handler=None):
            self._w_size = window_size_sec  # Sliding window size
            self._ts_list = list()  # Ordered, unique list of timestamp of sliding window
            self._d = dict()  # Dictionary {[timestamp]: [hits]}
            self._hits = 0  # Number of hits in sliding window
            self._max = critical_rate * window_size_sec  # Critical hits in sliding window
            self._warn = False  # If the monitor is in warning state
            self.alert_handler = alert_handler
            # FIFO Buffer to store ts-hits pair that pops from _ts_list and _d
            self._ooo_buffer_size = ooo_buffer_size
            self._ooo_buffer = deque(ooo_buffer_size * [[0, 0]], ooo_buffer_size)
            # Upper class
            self._mon = monitor

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
            return self._d.get(ts) is not None

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
            for i in range(self._ooo_buffer_size):
                # ooo_buffer is append at right side, so first element is the oldest
                # element in ooo_buffer is list [ts, hits]
                if 0 < ts - self._ooo_buffer[i][0] < self._w_size:
                    hits_after_ts = sum([self._d[x] for x in self._ts_list[self._ts_list.index(ts) + 1:]])
                    hits_ooo_buffer = 0
                    for j in range(i, self._ooo_buffer_size):
                        hits_ooo_buffer += self._ooo_buffer[j][1]
                    hits = self._hits - hits_after_ts + hits_ooo_buffer
                    self.check_hits(ts, hits)
                    break

        def check_hits(self, ts, hits):
            # Check if hits larger than configured value
            if not self._warn and hits > self._max:
                self._warn = True
                self._mon.print_warn(f'High traffic hits {hits} at {datetime.fromtimestamp(ts)}')
                if callable(self.alert_handler):
                    self.alert_handler(True, ts, hits)
            if self._warn and hits <= self._max:
                self._warn = False
                self._mon.print_warn(f'Traffic drops to {hits} at {datetime.fromtimestamp(ts)}')
                if callable(self.alert_handler):
                    self.alert_handler(False, ts, hits)

    class PrintColors:
        """For output color in cmd"""
        OK = '\033[92m'
        WARN = '\033[93m'
        ERR = '\033[91m'
        ENDC = '\033[0m'

    def __init__(self, statis_period=10, traffic_mon_size=120, critical_rate=10, ooo_buffer_size=3, debug=False,
                 statis_rep_handler=None, alert_handler=None):
        self.ts_now = None  # Timestamp of current time, always moves forward
        '''CSV related vairable'''
        self.col_list = list()  # CSV Column name list, line[0] of csv
        self.csv_sep = ','
        self.req_pattern = re.compile(r'\w+ (/\w+)\S* \S*')
        '''Debug'''
        self.log_count = 0
        self.debug = debug
        if self.debug:
            self.print_dbg = print
        else:
            self.print_dbg = lambda *x: None  # Define debug print as dummy function
        '''Periodic statistics'''
        self.statis = HttpLogMonitor.Statistics(self, statis_period, report_handler=statis_rep_handler)
        '''Traffic monitoring'''
        self.monitor = HttpLogMonitor.TrafficMonitor(self,
                                                     traffic_mon_size,
                                                     critical_rate,
                                                     ooo_buffer_size, alert_handler=alert_handler)

    @staticmethod
    def print_warn(msg: str):
        print(f'{HttpLogMonitor.PrintColors.WARN}{msg}{HttpLogMonitor.PrintColors.ENDC}')

    @staticmethod
    def print_ok(msg: str):
        print(f'{HttpLogMonitor.PrintColors.OK}{msg}{HttpLogMonitor.PrintColors.ENDC}')

    @staticmethod
    def print_err(msg: str):
        print(f'{HttpLogMonitor.PrintColors.ERR}{msg}{HttpLogMonitor.PrintColors.ENDC}')

    @staticmethod
    def print_msg(msg: str):
        print(msg)

    def time_update(self, ts: int):
        # Make sure time always moves forward
        if self.ts_now is None:
            self.ts_now = ts
        else:
            if ts > self.ts_now:
                self.ts_now = ts

    def feed_line(self, log_line: str):
        if len(self.col_list) == 0:  # Suppose that the first line is column names
            for i in log_line.split(self.csv_sep):
                i = i[1:-1]
                self.col_list.append(i)
            self.print_dbg('Log monitor initialized.')
        else:
            # Start to decode log line
            if self.debug:
                self.log_count += 1
            d = dict()
            log_line = log_line.split(self.csv_sep)
            if len(log_line) != len(self.col_list):
                self.print_err('Corrupted log line, ignore')
                return
            # Read line into dictionary with corresponding column name as key
            for item_name, item_val in zip(self.col_list, log_line):
                d[item_name] = item_val.replace('"', '')
            ts = int(d['date'])
            # Decode log line into log item
            log_item = HttpLogMonitor.HttpLogItem(self, d)
            self.time_update(ts)
            # Add log to statis
            self.statis.add(ts, log_item.section)
            # Update traffic monitor
            self.monitor.add(ts)

    def instant_statis_report(self):
        if self.debug:
            # Called when exiting from the script, print statistics even if it's not fully filled
            self.print_ok(f'====Instant statistics report at {datetime.fromtimestamp(self.ts_now)}====')
            self.statis.report()
        else:
            self.print_err('Debug not activated, instant_statis_report is not usable.')


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
    statis_size = args.statis_size
    win_size = args.win_size
    win_max_rate = args.win_crt_hits
    print(f'Statistics size: {statis_size} seconds')
    print(f'Traffic monitoring size: {win_size} seconds')
    print(f'Critical traffic monitoring hits: {win_max_rate} hits/sec')
    mon = HttpLogMonitor(statis_period=statis_size, traffic_mon_size=win_size, critical_rate=win_max_rate,
                         debug=args.debug)
    try:
        for line in sys.stdin:
            mon.feed_line(line)
    except KeyboardInterrupt:
        print("Quitting...")
    finally:
        if args.debug:
            mon.instant_statis_report()
    if args.debug:
        print(f'Total log count: {mon.log_count}')
        print('Finished')
