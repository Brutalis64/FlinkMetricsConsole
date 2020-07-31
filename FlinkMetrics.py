    import urllib3
    import json
    import time
    import argparse
    import datetime
    from collections import defaultdict
    import sys

    MAX_METRICS_REQUEST = 50

    BCOLORS = {
        'HEADER': '\033[95m',
        'OKBLUE': '\033[94m',
        'OKGREEN': '\033[92m',
        'WARNING': '\033[93m',
        'FAIL': '\033[91m',
        'ENDC': '\033[0m',
        'BOLD': '\033[1m',
        'UNDERLINE': '\033[4m'
    }


    class FlinkAPI(object):
        def __init__(self, host, port):
            self.http_pool = urllib3.PoolManager()
            self.flink_base_url = 'http://{}:{}/'.format(host, port)

        def _http_get_json(self, url):
            r = self.http_pool.request('GET', url)
            return json.loads(r.data.decode('utf-8'))

        def get_all_running_jobs(self):
            jobs_url = self.flink_base_url + 'jobs'
            jobs = self._http_get_json(jobs_url)['jobs']
            return jobs

        def _get_job_details(self):
            jobs = self.get_all_running_jobs()
            result = []
            for job in jobs:
                if job['status'] == 'RUNNING':
                    detailed_info_url = self.flink_base_url + 'jobs/' + job['id']
                    result.append(self._http_get_json(detailed_info_url))
            return result

        @staticmethod
        def _get_dup(jobs):
            dup = defaultdict(int)
            for job in jobs:
                dup[job['name']] += 1
            return dup

        @staticmethod
        def _check_job_duplicates(dup, job, job_name):
            if dup[job_name] != 1:
                print(BCOLORS['FAIL'] +
                      'Warning! It seems that there ara {} instances of "{}" job. Printing info about "{}".'.format(
                          dup[job_name], job_name, job['jid']) + BCOLORS['ENDC'],
                      file=sys.stderr)

        def print_all_running_jobs(self):
            data = [['dup', 'jid', 'name', 'duration', 'state']]
            jobs = self._get_job_details()
            dup = self._get_dup(jobs)

            for job in jobs:
                if dup[job['name']] == 1:
                    d = ''
                else:
                    d = '*'

                data.append([d,
                             job['jid'],
                             job['name'],
                             str(datetime.timedelta(
                                 seconds=int(job['duration']) / 1000)),
                             job['state']])
            pretty_print(data)

        def print_all_job_tasks(self, job_name):
            jobs = self._get_job_details()
            dup = self._get_dup(jobs)
            data = None
            if job_name not in dup:
                print(BCOLORS['FAIL'] +
                      'Provided job name {} doesn\'t exist or not in running state. Please check arguments.'.format(
                          job_name) + BCOLORS['ENDC'],
                      file=sys.stderr)
                sys.exit(1)

            for job in jobs:
                if job['name'] == job_name:
                    self._check_job_duplicates(dup, job, job_name)
                    time.sleep(0.1)
                    data = [['id', 'parallelism', 'status', 'name']]
                    for task in job['vertices']:
                        data.append([task['id'],
                                     task['parallelism'],
                                     task['status'],
                                     task['name']])
                    break
            pretty_print(data)

        def _get_task_metrics_ids(self, job_name, task_name):
            job_id = None
            task_id = None
            jobs = self._get_job_details()
            dup = self._get_dup(jobs)
            metrics = None
            for job in jobs:
                if job['name'] == job_name:
                    self._check_job_duplicates(dup, job, job_name)
                    time.sleep(0.1)
                    job_id = job['jid']
                    for task in job['vertices']:
                        if task['name'] == task_name:
                            task_id = task['id']
                            metrics = self._http_get_json(
                                self.flink_base_url + 'jobs/' + job_id + '/vertices/' + task_id + '/metrics')
                    break

            if not job_id:
                print(BCOLORS[
                          'FAIL'] + 'Provided job name "{}" doesn\'t exist or not in running state. '
                                    'Please check arguments.'.format(job_name + BCOLORS['ENDC']),
                      file=sys.stderr)
                sys.exit(1)
            if not task_id:
                print(
                    BCOLORS['FAIL'] + 'Provided task name "{}" doesn\'t exist. Please check arguments.'.format(task_name) +
                    BCOLORS['ENDC'],
                    file=sys.stderr)
                sys.exit(1)
            return job_id, task_id, [x['id'] for x in metrics]

        def print_task_metrics(self, job_name, task_name, exact_metrics=None, like_metrics=None, ignore_case=False):
            job_id, task_id, metrics = self._get_task_metrics_ids(job_name, task_name)
            new_metrics = set()
            if exact_metrics:
                for m in metrics:
                    for e in exact_metrics.split(','):
                        if e == '.'.join(m.split('.')[1:]):
                            new_metrics.add(m)

            if like_metrics:
                for m in metrics:
                    for e in like_metrics.split(','):
                        if ignore_case and str(e).lower() in str(m).lower():
                            new_metrics.add(m)
                        elif e in m:
                            new_metrics.add(m)

            if new_metrics or exact_metrics or like_metrics:
                metrics = list(new_metrics)

            values = []
            for i in range(0, len(metrics), MAX_METRICS_REQUEST):
                values_url = self.flink_base_url + 'jobs/' + job_id + '/vertices/' + task_id + '/metrics?get={}'.format(
                    ','.join([x for x in metrics[i:i + MAX_METRICS_REQUEST]]))
                values += self._http_get_json(values_url)

            result = defaultdict(float)

            data = [['metric', 'value']]
            for metric in values:
                if not metric['value'].isalpha():
                    result['.'.join(metric['id'].split('.')[1:])] += float(metric['value'])

            for k in result.keys():
                data.append([k, result[k]])
            pretty_print(data)


    def pretty_print(data):
        max_lengths = []
        for i in range(0, len(data[0])):
            max_len = 10
            for j in data:
                max_len = max(max_len, len(str(j[i])))
            max_lengths.append(max_len)

        result_formatter = str()
        for f in max_lengths:
            result_formatter += '{:<' + str(f) + '} | '

        for value in data:
            if data.index(value) == 0 or data.index(value) == 1:
                print('-' * sum(max_lengths) + '-' * len(max_lengths) * 3)
            print(result_formatter.format(*value))


    if __name__ == '__main__':
        parser = argparse.ArgumentParser()
        parser.add_argument('--host', help='JobManager HOST to connect', required=True)
        parser.add_argument('--port', help='JobManager PORT to connect', required=True)
        action_group = parser.add_mutually_exclusive_group()
        action_group.add_argument('--jobs', help='print all running jobs with id\'s', action='store_true')
        action_group.add_argument('--tasks', help='Print details for certain job', action='store_true')
        action_group.add_argument('--metrics', help='Print metrics for certain task', action='store_true')
        parser.add_argument('--exact_metrics', help='Comma separated list of metrics to exact match. '
                                                    'Provided without leading thread number')
        parser.add_argument('--like_metrics', help='Comma separated list of metrics to like match. '
                                                   'Provided without leading thread number')
        parser.add_argument('--ignore_case', help='Ignore case for --like_metrics option', action='store_true')
        # parser.add_argument('--task_id', help='Provide task id for using --metrics option')
        # parser.add_argument('--job_id', help='Provide job id for using --metrics  or --tasks option')
        parser.add_argument('--task_name', help='Provide task id for using --metrics option')
        parser.add_argument('--job_name', help='Provide job id for using --metrics  or --tasks option')
        args = parser.parse_args()

        api = FlinkAPI(args.host, args.port)
        if args.jobs:
            api.print_all_running_jobs()

        elif args.tasks:
            if not args.job_name:
                print(BCOLORS['WARNING'] + 'Please provide --job_name option' + BCOLORS['ENDC'])
                sys.exit(0)
            api.print_all_job_tasks(args.job_name)

        elif args.metrics:
            if not args.job_name or not args.task_name:
                print(BCOLORS['WARNING'] + 'Please provide --job_name and --task_name options' + BCOLORS['ENDC'])
                sys.exit(0)

            if args.exact_metrics and args.like_metrics:
                api.print_task_metrics(args.job_name, args.task_name,
                                       exact_metrics=args.exact_metrics,
                                       like_metrics=args.like_metrics,
                                       ignore_case=args.ignore_case)
            elif args.exact_metrics:
                api.print_task_metrics(args.job_name, args.task_name,
                                       exact_metrics=args.exact_metrics, ignore_case=args.ignore_case)
            elif args.like_metrics:
                api.print_task_metrics(args.job_name, args.task_name,
                                       like_metrics=args.like_metrics, ignore_case=args.ignore_case)
            else:
                api.print_task_metrics(args.job_name, args.task_name)
