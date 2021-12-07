# http_log_monitor

## Environment
* The script log_monitor.py has been tested in Ubuntu 18.04/20.04 with python3.8.
* All packages used in the script are python3.8 built-in

## How to use
1. To shou help info in console:

```python3 log_monitor.py -h```

2. There are 4 options:

| Option          | Explain                                               | Default value |
|-----------------|-------------------------------------------------------|---------------|
| -s STATIS_SIZE  | Every [STATIS_SIZE] seconds of log lines print statis | 10            |
| -w WIN_SIZE     | Traffic monitoring window size in seconds             | 120           |
| -c WIN_CRT_HITS | Critical hits per second for traffic monitoring       | 10            |
| -d              | Enable debug output                                   | False         |

3. The script reads stdin, so monitoring on a historic CSV file:

```cat sample_csv.txt | python3 log_monitor.py```

4. For moniotring on a growing log file:

```tail -f -n +1 sample_csv.txt | python3 log_monitor.py```

## Test case
### To run test cases: 

```python3 run_test_cases.py```

In this script class "LogGenerator" is used for generating test log, then pass the log to log_monitor.
Currently, there are 5 test cases in run_test_cases.py. Each test case will firstly print what output
to expect, then run the monitor and finally print the monitor output.

### To add test case:

```
log_config = [
    LogGenerator.Config(0, 1),
    LogGenerator.Config(1, 1),
    ...
]
run_test(LogGenerator(log_config).output())
```

"log_config" is a list for log generator whose element "LogGenerator.Config".
"LogGenerator.Config" is initialized by 2 parameters: timestamp, number of hits happened at this moment

## Scalable
To use log monitor in other python scripts (like run_test_cases.py)

```
from log_monitor import HttpLogMonitor
mon = HttpLogMonitor(...parameters...)
for log_line in log_file:
    mon.feed_line(line)
```

We can also pass custom handler to monitor

```
HttpLogMonitor(statis_rep_handler=your_statis_report_hanlder, 
                alert_handler=your_alert_handler)
```

* "statis_rep_handler" will be called with 1 parameter of type "dict", whose key is "section" 
and value is hits
* "alert_handler" will be called with 3 parameters:
  1. True: alert has been triggered, False: alert has been disarmed
  2. Timestamp of the alert
  3. Number of hits that triggered this alert

## Potential bug/problem
If a log line arrives really delayed, like 2 minutes ago in default config of monitor, it will be ignored.

## To improve
0. There no handling for corrupted log file.

1. I have been troubled to find a datatype like a dictionary with ordered/sorted keys.
Since the incoming log is not necessarily in order, it's timestamp may be in the past,
I need to update/insert the {timestamp, hits} pair at a specific position of a dictionary.
For now, I have used 1 sorted-list to record the timestamp and another ordinary dict
to record corresponding hits.

2. To keep a sorted list, I have called "bisect.insort", which is not efficient enough,
because firstly, the timestamps of log are not totally disordered, secondly, when the sliding
window moves it calls frequently pop, which is not a good idea for a large list.
Instead, replace this list by a deque, replace bisect.insort by comparing from the end 
and insert at a given position.

3. String.replace is called very frequently, as for every element of each log line, I called
it to remove quotes("), I don't know if a recompiled regex match will quicker or not.

4. Re-write in C/C++ will significantly improve efficiency (I suppose).
