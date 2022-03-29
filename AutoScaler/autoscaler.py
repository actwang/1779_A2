# import socketserver
# import schedule
# from threading import Thread
import cgi
import threading
from datetime import datetime, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler, ThreadingHTTPServer
import boto3
import time
from botocore.exceptions import ClientError
from config import Config
import requests
from apscheduler.schedulers.background import BackgroundScheduler
import pymysql

MAX_POOL_SIZE = 8
MIN_POOL_SIZE = 1


# global avg_overall_misRate
# global instances_running
# global settings

def connect_to_database():
    try:
        return pymysql.connect(
            host=Config.RDS_CONFIG['host'],
            port=Config.RDS_CONFIG['port'],
            user=Config.RDS_CONFIG['user'],
            password=Config.RDS_CONFIG['password'],
            database=Config.RDS_CONFIG['database']
        )
    except pymysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)


def pick_instances(num_ins, open):
    """ Pick num_ins number of instances (for outside func to open or close)
        Open = 1 means to open instances, 0 means to close
        Return an array of instance IDs

        int, int --> list
    """
    if num_ins == 0:
        return []
    ec2 = boto3.resource('ec2')
    instances = ec2.instances.all()
    if open:
        STATE = 'stopped'
    else:
        STATE = 'running'
        
    id_arr = [ins.id for ins in instances if ins.instance_type == 't2.micro'
                                 and ins.state['Name'] == STATE]
    
    print('ID array = ')
    print(id_arr)
    print('num_ins = ' + str(num_ins))
    if open:
        picked_arr = id_arr[:num_ins]
        # for i in range(num_ins):
        #     picked_arr.append(id_arr[i])

    else:
        picked_arr = id_arr[-num_ins:]
        # for i in reversed(range(num_ins)):
        #     picked_arr.append(id_arr[i])

    print('Picked_arr = ')
    print(picked_arr)

    return picked_arr



# Start a EC2 instance
def start_an_ec2_instance():
    ec2 = boto3.resource('ec2')
    instances = ec2.instances.all() # Collection of EC2 Objects

    # Get a stopped instance's ID
    stopped_instance_id_array = [ins.id for ins in instances if ins.instance_type == 't2.micro'
                                 and ins.state['Name'] == 'stopped']

    if not stopped_instance_id_array:
        print('Error in start_ec2_instance: No available instances to start.')
        return -1

    start_id = stopped_instance_id_array[0]
    ec2_start(start_id)
    print('Starting instance ID = ' + str(start_id))
    return start_id


# Start the EC2 instance by ID
def ec2_start(ins_id):
    ec2 = boto3.client('ec2')
    try:
        ec2.start_instances(InstanceIds=[ins_id], DryRun=True)
    except ClientError as e:
        if 'DryRunOperation' not in str(e):
            raise

    # Dry run succeeded, run start_instances without dryrun
    try:
        response = ec2.start_instances(InstanceIds=[ins_id], DryRun=False)
        print(response)
    except ClientError as e:
        print(e)

    return


# Stop a running EC2 instance
def stop_an_ec2_instance():
    """Choose an EC2 instance to stop from running instances"""
    ec2 = boto3.resource('ec2')
    instances = ec2.instances.all()

    # Get a running instance's ID
    running_ins_id_array = [ins.id for ins in instances if (ins.instance_type == 't2.micro'
                                                            and ins.state['Name'] == 'running')]
    if not running_ins_id_array:
        print('Error in start_ec2_instance: No available instances to start.')
        return -1

    stop_id = running_ins_id_array[-1]
    ec2_stop(stop_id)
    print('Stopping instance ID = ' + str(stop_id))
    return stop_id


# Stop the EC2 instance by ID
def ec2_stop(ins_id):
    ec2 = boto3.resource('ec2')

    ec2.instances.filter(InstanceIds=[ins_id]).stop()

    return



def get_avg_missRate():
    # Read instance IDs dynamically from aws
    ec2 = boto3.resource('ec2')
    instances = ec2.instances.all()

    client = boto3.client('cloudwatch', region_name=Config.AWS_CONFIG['REGION'],
                          aws_access_key_id=Config.AWS_CONFIG['AWS_ACCESS_KEY_ID'],
                          aws_secret_access_key=Config.AWS_CONFIG['SECRET_ACCESS_KEY'])

    # Check running instance and put their id in an array
    id_array = [instance.id for instance in instances if (instance.instance_type == 't2.micro'
                                                          and instance.state['Name'] == 'running')]

    sum_avg_misRate = 0
    # Pull the 1-minute avg miss rate of each memcache instance
    for ins_id in id_array:
        print('Getting miss rate datapoints of instance ' + str(ins_id))
        miss_rate = client.get_metric_statistics(
            Period=60,
            Namespace='MemCache',
            MetricName='MissRate',
            Dimensions=[{'Name': 'InstanceId', 'Value': Config.id_lookup[ins_id]}],
            StartTime=datetime.utcnow() - timedelta(seconds=1 * 60),
            EndTime=datetime.utcnow(),
            Unit='Count',
            Statistics=['Average']
        )

        # A datapoint looks like below
        # [{'Timestamp': datetime.datetime(2022, 3, 13, 15, 13, 59, tzinfo=tzutc()), 'Average': 0.0, 'Unit': 'Count'},...]

        # Computer the average miss rate over the last minute
        # misRate_sum = 0
        # ct = 0
        # for Dpoints in miss_rate['Datapoints']:
        #     misRate_sum += Dpoints['Average']
        #     ct += 1
        print(miss_rate['Datapoints'])
        if not miss_rate['Datapoints']:
            print('No data points yet.')
            continue
        avg_misRate = miss_rate['Datapoints'][0]['Average']  # Past 1-minute avg
        # print(avg_misRate)
        sum_avg_misRate += avg_misRate
    try:
        # Update the average miss rate of all running instance
        Config.avg_missRate = sum_avg_misRate / len(id_array)
    except ZeroDivisionError:
        print('Run at least one memcache instance please. ')
    print('AVG overall miss rate updated. MissRate = ', str(Config.avg_missRate))
    print('id_array = ' + str(id_array))
    # print('sum_avg_missRate = ' + str(sum_avg_misRate))


class managerHandler(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/html')
        self.end_headers()

    def do_GET(self):
        _set_response()

        # Receive settings from manager: requests.post("http://localhost", {'max_miss': 1,
        #                                               'min_miss': 0, 'expand_ratio': 2, 'shrink_ratio': 0.5})

    def do_POST(self):
        ctype, pdict = cgi.parse_header(self.headers.get('content-type'))
        pdict['boundary'] = bytes(pdict['boundary'], "utf-8")
        pdict['CONTENT-LENGTH'] = int(self.headers.get('Content-length'))
        if ctype == 'multipart/form-data':
            fields = cgi.parse_multipart(self.rfile, pdict)
        print(fields)
        self._set_response()

        # POST's fields looks like
        # {'mode':'manual', 'add_drop': 1} add=1; drop=-1; change_mode=0
        # {'mode':'auto', 'max_miss': ['1'], 'shrink_ratio': ['0.5'], 'expand_ratio': ['2'], 'min_miss': ['0']}

        mode = fields['mode'][0]
        if mode == 'Manual':
            Config.settings = {'mode': mode}  # Manual mode doesn't need mode settings
            add_drop = int(fields['add_drop'][0])
            manual_mode(add_drop)

            # update settings in Config so the main loop can read it and enter manual mode(which is nothing in main)

        elif mode == 'Automatic':
            max_miss = float(fields['max_miss'][0])
            min_miss = float(fields['min_miss'][0])
            expand_ratio = float(fields['expand_ratio'][0])
            shrink_ratio = float(fields['shrink_ratio'][0])
            Config.settings = {'mode': mode, 'max_miss': max_miss, 'min_miss': min_miss, 'expand_ratio': expand_ratio,
                               'shrink_ratio': shrink_ratio}

        else:
            print('Illegal mode in manager POST request!! Must be manual or auto, but got: ', mode)
            raise TypeError

        # Optional response, maybe the reason that causes code requests to fail?
        # self.send_response(200)



def auto_mode(settings, missRate):
    """
    Checks the auto mode settings to see if pool needs resize.
    Called upon by __main__ function every second.

    dict, float -> None
    """
    pool_size_changed = False
    target_pool_size = -1

    # settings = {'mode': auto, 'max_miss': 0.5, 'min_miss': 0.0, 'expand_ratio': 2.0, 'shrink_ratio': 0.5}
    ec_2 = boto3.resource('ec2')
    all_instances = ec_2.instances.all()

    id_arr = [ins.id for ins in all_instances if (ins.instance_type == 't2.micro'
                                                  and ins.state['Name'] == 'running')]
    instances_running = len(id_arr)

    # Grow the pool when miss rate is too high
    if missRate > settings['max_miss']:
        print('Increasing pool size! Miss Rate too high!')
        target_pool_size = int(instances_running * Config.settings['expand_ratio'])
        if target_pool_size > MAX_POOL_SIZE:
            target_pool_size = MAX_POOL_SIZE

        num_ins_to_open = target_pool_size - instances_running
        # randomly pick this number of instances beforehand to avoid for loop picking the same
        #       instances due to delay in between stopped and pending status
        open_id_arr = pick_instances(num_ins_to_open, 1)
        print(num_ins_to_open)
        for open_id in open_id_arr:
            pool_size_changed = True
            ec2_start(open_id)

        if pool_size_changed:
            new_running_ins = len([ins.id for ins in ec_2.instances.all() if (ins.instance_type == 't2.micro'
                                                                              and ins.state['Name'] == 'running')])
            while new_running_ins != target_pool_size:
                # target_pool_size won't be -1 because that means pool_size_changed = False
                new_running_ins = len([ins.id for ins in ec_2.instances.all() if (ins.instance_type == 't2.micro'
                                                                                  and ins.state['Name'] == 'running')])
                time.sleep(1)

            new_id_arr = [ins.id for ins in ec_2.instances.all() if (ins.instance_type == 't2.micro'
                                                                     and ins.state['Name'] == 'running')]

            cnx = connect_to_database()
            cursor = cnx.cursor()
            query1 = "UPDATE ECE1779.cachepool_stats SET num_instances = %s WHERE id = 0;"
            cursor.execute(query1, (target_pool_size,))

            diff = set(new_id_arr).difference(id_arr)
            query2 = "UPDATE ECE1779.cache_instances SET is_avail = 1 WHERE ec2_id = %s;"
            for ins_id in diff:
                cursor.execute(query2, (ins_id,))

            cnx.commit()
            cursor.close()
            cnx.close()
            # notify front end
            address = Config.FRONTEND_URL+ '/pool/resize'
            requests.post(address)

    # Shrink the pool when miss rate is too low
    elif missRate < settings['min_miss']:
        print('Reducing Pool Size! Miss Rate too low! ')
        # int() is for rounding down to ensure integer number of target pool size
        target_pool_size = int(instances_running * Config.settings['shrink_ratio'])
        if target_pool_size < MIN_POOL_SIZE:
            target_pool_size = MIN_POOL_SIZE

        print('instances running = '+ str(instances_running))
        print('taget pool size = '+ str(target_pool_size))
        num_ins_to_stop = instances_running - target_pool_size
        stop_id_arr = pick_instances(num_ins_to_stop, 0)
        print('stop_id_arr = ')
        print(stop_id_arr)
        print('\n\n')
        for stop_id in stop_id_arr:
            pool_size_changed = True
            ec2_stop(stop_id)

        if pool_size_changed:
            new_running_ins = len([ins.id for ins in ec_2.instances.all() if (ins.instance_type == 't2.micro'
                                                                              and ins.state['Name'] == 'running')])
            while new_running_ins != target_pool_size:
                # target_pool_size won't be -1 because that means pool_size_changed = False
                new_running_ins = len([ins.id for ins in ec_2.instances.all() if (ins.instance_type == 't2.micro'
                                                                                  and ins.state['Name'] == 'running')])
                time.sleep(1)

            new_id_arr = [ins.id for ins in ec_2.instances.all() if (ins.instance_type == 't2.micro'
                                                                     and ins.state['Name'] == 'running')]

            cnx = connect_to_database()
            cursor = cnx.cursor()
            query1 = "UPDATE ECE1779.cachepool_stats SET num_instances = %s WHERE id = 0;"
            cursor.execute(query1, (target_pool_size,))

            query2 = "UPDATE ECE1779.cache_instances SET is_avail = 1 WHERE ec2_id = %s;"
            diff = set(id_arr).difference(new_id_arr)
            for ins_id in diff:
                cursor.execute(query2, (ins_id,))

            cnx.commit()
            cursor.close()
            cnx.close()

            # notify front end
            address = Config.FRONTEND_URL + '/pool/resize'
            requests.post(address)

    return


def manual_mode(add_drop):
    """
    Manual mode is at any time subject to +1 or -1 pool resize requests from the manager.
    Manual mode function is not called by main as it will be called upon and execute
    the job of adding or dropping an EC2 instance as soon as the Manager POST request comes in.

    dict -> None
    """
    if add_drop == 0:
        return

    ec_2 = boto3.resource('ec2')
    all_instances = ec_2.instances.all()
    # print([inst.id for inst in all_instances])

    pool_size_changed = False
    new_ins_id = -1

    id_arr = [ins.id for ins in all_instances if (ins.instance_type == 't2.micro'
                                                  and ins.state['Name'] == 'running')]
    instances_running = len(id_arr)

    # Return if already at MAX or MIN pool size
    if instances_running + add_drop < MIN_POOL_SIZE:
        print('Manual mode drop_instance_error: At least one instance must be running.')
        return
    if instances_running + add_drop > MAX_POOL_SIZE:
        print('Manual mode start_instance_error: At most eight instances can be running.')
        return

    # if resizing pool
    if add_drop == 1 or add_drop == -1:
        pool_size_changed = True
        if add_drop == 1:
            new_ins_id = start_an_ec2_instance()
        elif add_drop == -1:
            new_ins_id = stop_an_ec2_instance()

    elif add_drop != 0:
        print('Invalid add_drop. Must be 0, 1 or -1. ')

    # After adjusting Pool Size, update the RDS and notify frontend
    if pool_size_changed:
        new_running_ins = len([ins.id for ins in ec_2.instances.all() if (ins.instance_type == 't2.micro'
                                                                          and ins.state['Name'] == 'running')])
        # Wait for expected number of instances to be running
        while new_running_ins != instances_running + add_drop:
            new_running_ins = len([ins.id for ins in ec_2.instances.all() if (ins.instance_type == 't2.micro'
                                                                              and ins.state['Name'] == 'running')])
            print('Waiting for instance to be ready')
            time.sleep(1)

        cnx = connect_to_database()
        cursor = cnx.cursor()
        if add_drop == 1:
            query1 = "UPDATE ECE1779.cachepool_stats SET num_instances = %s WHERE id = 0;"
            query2 = "UPDATE ECE1779.cache_instances SET is_avail = 1 WHERE ec2_id = %s;"
            cursor.execute(query1, (new_running_ins,))
            cursor.execute(query2, (new_ins_id,))
            print('Executed is_avail =1 Script id = '+str(new_ins_id))
        elif add_drop == -1:
            query1 = "UPDATE ECE1779.cachepool_stats SET num_instances = %s WHERE id = 0;"
            query2 = "UPDATE ECE1779.cache_instances SET is_avail = 0 WHERE ec2_id = %s;"
            print('DROPPINGGGGG')
            cursor.execute(query1, (new_running_ins,))
            print("Executing query2 set is_avail = 0, ec2_id = ",str(new_ins_id))
            cursor.execute(query2, (new_ins_id,))
        cnx.commit()            
        cursor.close()
        cnx.close()

        requests.post("http://localhost:5000/api/memcache/resize")

    return


# TODO: Main Function Starts
if __name__ == '__main__':
    # instances_running = 0  # Number of instances currently running
    ec2 = boto3.resource('ec2')
    instances = ec2.instances.all()

    # Start the server that listens to manager app
    server = ThreadingHTTPServer(('localhost', 8000), managerHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    # Don't need server_thread.join() here because don't wait for server to finish before continuing main
    #   otherwise run_server() will block main thread
    server_thread.daemon = True
    server_thread.start()
    print('AutoScaler Server running on port 8000! ')

    # Get the miss rate every minute (always, so that avg_overall_misRate is always available and up-to-date)
    get_avg_missRate()
    scheduler = BackgroundScheduler({'apscheduler.timezone': 'UTC'})
    scheduler.add_job(get_avg_missRate, 'interval', seconds=60)
    scheduler.start()

    while True:
        print('settings = ' + str(Config.settings))
        print('miss rate = ' + str(Config.avg_missRate))

        if 'mode' in Config.settings:
            # Check running instances, update RDS and send post request to frontend if changed

            # Auto mode
            if Config.settings['mode'] == 'Automatic':
                auto_mode(Config.settings, Config.avg_missRate)

            # Manual mode
            elif Config.settings['mode'] == 'Manual':
                pass

            else:
                print("Error in Autoscaler Main: Operation mode in settings is set illegally: "
                      "Must be 'auto' nor 'manual'. ")

        time.sleep(1)
