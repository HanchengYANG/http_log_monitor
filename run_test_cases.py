from log_monitor import HttpLogMonitor
import csv


class LogGenerator:

    class Config:
        def __init__(self, dt, hits, remote_host='10.0.0.1', rfc931='-', authuser='apache', method='GET',
                     url='/api/help', proto='HTTP/1.0', status='200', content_len='1234'):
            self.log_line = f'"{remote_host}","{rfc931}","{authuser}",{dt},"{method} {url} {proto}",{status},{content_len}'
            self.hits = hits

    def __init__(self, config_list: list):
        self.log = ['"remotehost","rfc931","authuser","date","request","status","bytes"']
        for config in config_list:
            for i in range(config.hits):
                self.log += [config.log_line]

    def output(self):
        return self.log


def run_test(log_list: iter):
    print('====Http log monitor output start====')
    mon = HttpLogMonitor(debug=True)
    for line in log_list:
        mon.feed_line(line)
    mon.instant_statis_report()
    print('====Http log monitor output end====\n')


if __name__ == "__main__":
    print(r'''
TEST 1: generate 1200 hits every 120s, should not trigger the alert''')
    log_config = [LogGenerator.Config(0, 1200), LogGenerator.Config(120, 1200)]
    log = LogGenerator(log_config).output()
    run_test(log)
    print(r'''
TEST 2: generate 1201 hits every 120s, alert should be triggered twice
and alert should be disarmed when the first log of the second timestamp.
time[0-119] = 1201 hits 
time[1-120] = 1201 hits (At the very beginning of 120s, when monitor reads the first
                        hit, time[1-120] = 1hit, alert should be disarmed''')
    log_config = [LogGenerator.Config(0, 1201), LogGenerator.Config(120, 1201)]
    log = LogGenerator(log_config).output()
    run_test(log)
    print(r'''
TEST 3: Disordered log
One alert should be expected
t[0] = 1200 hits                        t[0-119] = 1201 hits (alert)
t[120] = 1hits                  =>      t[1-120] = 1 hits (disarm alert)
t[119] = 1hits (Disordered)''')
    log_config = [
        LogGenerator.Config(0, 1200),  # 0 - 119 = 1200 hits
        LogGenerator.Config(120, 1),   # 1 - 120 = 1hits
        LogGenerator.Config(119, 1)    # Disordered 1hits at 119, makes t[0 - 119] = 1201 hits
    ]
    run_test(LogGenerator(log_config).output())
    print(r'''
TEST 4: Disordered log (OOO size test)
The time of this hit tests OOO(out-of-order) buffering)
122 = winows size(120) - 1 + OOO size(3) which is the edge case
if place this hits later than [winows size - 1 + OOO size] will
cause missed alert.

t[0] = 1 hit                
t[1] = 1 hit
t[2] = 1 hit
t[3] = 1 hit
t[4] = 1 hit         =>
t[119] = 1195 hits          t[0-119] = 1200 hits (no alert)
t[122] = 1 hit              t[3-122] = 1198 hits (no alert)
t[119] = 1 hit              makes t[0-119] = 1201 hits (alert)
                            then  t[3-122] = 1199 hits (alert disarmed)
    ''')
    log_config = [
        LogGenerator.Config(0, 1),
        LogGenerator.Config(1, 1),
        LogGenerator.Config(2, 1),
        LogGenerator.Config(3, 1),
        LogGenerator.Config(4, 1),
        LogGenerator.Config(119, 1195),
        LogGenerator.Config(122, 1),
        LogGenerator.Config(119, 1),
    ]
    run_test(LogGenerator(log_config).output())
    print(r'''
TEST 5: Log line too late
Expect: a warning tells log line at timestamp 0 is ignored.
    ''')
    log_config = [
        LogGenerator.Config(0, 1),
        LogGenerator.Config(1, 1),
        LogGenerator.Config(2, 1),
        LogGenerator.Config(3, 1),
        LogGenerator.Config(4, 1),
        LogGenerator.Config(119, 1),
        LogGenerator.Config(120, 1),
        LogGenerator.Config(0, 1),
    ]
    run_test(LogGenerator(log_config).output())
    print(r'''
TEST 6: Sorted vs disordered
Run monitor sample_csv.txt disordered(original) and sorted, compare alert output
    ''')
    # Prepare special monitor that only print alert
    def alert_print(t, ts, hits): print(f'Triggered {t} at {ts} hits {hits}')
    mon = HttpLogMonitor(debug=False, alert_handler=alert_print)
    def dummy(*args): pass
    mon.print_warn = dummy
    mon.print_err = dummy
    mon.print_ok = dummy
    mon.print_msg = dummy
    # Original file
    print("====Output of original log file====")
    csv_file = open("sample_csv.txt")
    for line in csv_file:
        mon.feed_line(line)
    # Sort csv
    csv_file = open("sample_csv.txt")
    csv_reader = csv.reader(csv_file)
    next(csv_reader)
    sorted_log_list = sorted(csv_reader, key=lambda row: row[3])
    log = list()
    for line in sorted_log_list:
        str_l = ','.join(line)
        log.append(str_l)
    h = '"remotehost","rfc931","authuser","date","request","status","bytes"'
    # Prepare special monitor that only print alert, since it's sorted, don't need ooo_buffer
    mon = HttpLogMonitor(debug=False, alert_handler=alert_print, ooo_buffer_size=0)
    mon.print_warn = dummy
    mon.print_err = dummy
    mon.print_ok = dummy
    mon.print_msg = dummy
    print("====Output of sorted log file====")
    for line in [h, *log]:
        mon.feed_line(line)

