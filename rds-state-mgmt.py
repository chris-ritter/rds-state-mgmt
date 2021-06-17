from datetime import datetime
import enum
import logging
import re
from os import environ

import boto3
import pytz
from pythonjsonlogger import jsonlogger



logger = logging.getLogger()
logger.setLevel(logging.DEBUG if str(environ.get('DEBUG_MODE', 'false')).lower() == 'true' else logging.INFO)
logging.getLogger('botocore').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

class CustomJsonFormatter(jsonlogger.JsonFormatter): # pragma: no cover
    ''' Customizations for structured logging. '''
    def add_fields(self, log_record, record, message_dict):
        ''' Add level field for our use. '''
        super().add_fields(log_record, record, message_dict)

        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname

if environ.get('CI') != 'true':
    for handler in logger.handlers:
        handler.setFormatter(CustomJsonFormatter(timestamp=True))
instances = []
client = boto3.client('rds')
rds = client.describe_db_instances()
for instance in range(len(rds['DBInstances'])):
    instances.append(rds['DBInstances'][instance]['DBInstanceIdentifier'])



START_TAG_NAME = environ.get('START_TAG_NAME') or 'uncomn:rds:schedule:start'
STOP_TAG_NAME = environ.get('STOP_TAG_NAME') or 'uncomn:rds:schedule:stop'

# note: time should be specified in 24hr time and according to the configured STATE_MGMT_TIMEZONE
HARDCODED_START = '06:00'.split(':')[0]
HARDCODED_STOP = '18:00'.split(':')[0]
TIME_PATTERN = re.compile('^([01][0-9]|2[0-3]):[0-5][0-9]$')

class StateManagementPhase(enum.Enum):
    ''' Helper enum. '''
    PHASE_ONE = enum.auto()   # :00 - :14
    PHASE_TWO = enum.auto()   # :15 - :29
    PHASE_THREE = enum.auto() # :30 - :44
    PHASE_FOUR = enum.auto()  # :45 - :59

class StateManagementDayOfWeek(enum.Enum):
    ''' Helper enum. '''
    MON = 0
    TUE = 1
    WED = 2
    THU = 3
    FRI = 4
    SAT = 5
    SUN = 6

class RecoveredError(Exception):
    ''' Helper class. '''

def get_invoke_time(now):
    ''' Get the invocation time. '''
    return [str(now.weekday())] + now.strftime('%H:%M').split(':')

def get_hour_phase(current_minute):
    ''' Identifies which segment of the hour the current invocation falls into. '''
    if 0 <= int(current_minute) < 15:
        return StateManagementPhase.PHASE_ONE

    if 15 <= int(current_minute) < 30:
        return StateManagementPhase.PHASE_TWO

    if 30 <= int(current_minute) < 45:
        return StateManagementPhase.PHASE_THREE

    return StateManagementPhase.PHASE_FOUR

timezone = pytz.timezone(environ.get('STATE_MGMT_TIMEZONE') or 'UTC')
utc_now = datetime.utcnow()
local_now = utc_now + timezone.utcoffset(utc_now)

current_day_of_week, current_hour, current_minute = get_invoke_time(local_now)
hour_phase = get_hour_phase(current_minute)

def get_day_of_week_offset(day_name):
    ''' Gets the .weekday() value of a given day of the week given only the day name. '''
    if day_name.upper() not in StateManagementDayOfWeek.__members__.keys():
        return False

    return StateManagementDayOfWeek[day_name.upper()]


def check_tag_time_format(instance, time_type, time_value):
    ''' Check to see if the time specified for a start or stop tag value is correctly formatted. '''
    if not re.match(TIME_PATTERN, time_value):

        return False

    return True

def tag_list_to_dict(tags):
    ''' Collapses the list of tags that AWS provides for EC2s down into a simple dict. '''
    return {t['Key']:t['Value'] for t in tags}

def check_configured_time(instance, time_type, minute_value):
    ''' Messages user-specified minute value for a start or stop tag value to a sane value. '''
    if int(minute_value) in [0, 15, 30, 45]:
        return minute_value

    if 0 < int(minute_value) < 15:
        logger.warning('Invalid minute specifier for instance', extra={
            'instance_id': instance.id,
            'time_type': time_type,
            'specified_time': ':' + minute_value,
            'assumed_time': ':00'
        })
        return '00'

    if 15 < int(minute_value) < 30:
        logger.warning('Invalid minute specifier for instance', extra={
            'instance_id': instance.id,
            'time_type': time_type,
            'specified_time': ':' + minute_value,
            'assumed_time': ':15'
        })
        return '15'

    if 30 < int(minute_value) < 45:
        logger.warning('Invalid minute specifier for instance', extra={
            'instance_id': instance.id,
            'time_type': time_type,
            'specified_time': ':' + minute_value,
            'assumed_time': ':30'
        })
        return '30'

    if 45 < int(minute_value): # pylint: disable=misplaced-comparison-constant
        logger.warning('Invalid minute specifier for instance', extra={
            'instance_id': instance.id,
            'time_type': time_type,
            'specified_time': ':' + minute_value,
            'assumed_time': ':45'
        })
        return '45'

    return minute_value
# This takes all the instances and filters them to see if they fit the requirement to be started. 
def start_db():
    start_dbs = set({})
    for node in instances:
        nodeinfo = client.describe_db_instances(
            DBInstanceIdentifier= node
      )
        status = nodeinfo['DBInstances'][0]['DBInstanceStatus']
        arn = nodeinfo['DBInstances'][0]['DBInstanceArn']
        dbtags = client.list_tags_for_resource(
            ResourceName= arn
        )
        tag = tag_list_to_dict(dbtags['TagList'])
        if status == 'stopped':
            if START_TAG_NAME in tag:
                for start_event in tag[START_TAG_NAME].split('/'):
                    if (check_event_time(instance, current_day_of_week, current_hour, hour_phase, start_event, 'start')) == True:
                        start_dbs.add(node)
    return start_dbs


## This takes all the instances and filters them to see if it checks all the requirements. If it passes it adds the instances to a list to be stopped
def stop_db():
    stop_dbs = set({})
    for node in instances:
        nodeinfo = client.describe_db_instances(
            DBInstanceIdentifier= node
      )
        status = nodeinfo['DBInstances'][0]['DBInstanceStatus']
        arn = nodeinfo['DBInstances'][0]['DBInstanceArn']
        dbtags = client.list_tags_for_resource(
            ResourceName= arn
        )
        tag = tag_list_to_dict(dbtags['TagList'])
        if status == 'available':
            if STOP_TAG_NAME in tag:
                for start_event in tag[STOP_TAG_NAME].split('/'):
                    if (check_event_time(instance, current_day_of_week, current_hour, hour_phase, start_event, 'stop')) == True:
                        stop_dbs.add(node)
    return stop_dbs

##Check to see if the event matches the scheduled time
def check_event_time(instance, current_day_of_week, current_hour, hour_phase, time_chunk, time_type): # pylint: disable=too-many-arguments
    Check to see if the event matches the scheduled time
    time_split = time_chunk.strip().split(' ')
    if len(time_split) > 1:
        

        target_day = get_day_of_week_offset(time_split[0])
        event_time = time_split[1]

        if target_day is False:
            
            return False

        if int(current_day_of_week) != target_day.value:
            return False
    else:
        event_time = time_split[0]

    if not check_tag_time_format(instance, time_type, event_time):
        return False

    tag_hour, tag_minute = event_time.split(':')
    if current_hour == tag_hour:
        tag_minute = check_configured_time(instance, time_type, tag_minute)

        if ((hour_phase == StateManagementPhase.PHASE_ONE and tag_minute == '00') # pylint: disable=too-many-boolean-expressions
            or (hour_phase == StateManagementPhase.PHASE_TWO and tag_minute == '15')
            or (hour_phase == StateManagementPhase.PHASE_THREE and tag_minute == '30')
            or (hour_phase == StateManagementPhase.PHASE_FOUR and tag_minute == '45')
        ):
            return True

    return False

##Handler for the lambda to call the script
def lambda_handler(event, context): # pragma: no cover
    ''' Lambda handler '''

    timezone = pytz.timezone(environ.get('STATE_MGMT_TIMEZONE') or 'UTC')
    utc_now = datetime.utcnow()
    local_now = utc_now + timezone.utcoffset(utc_now)

    current_day_of_week, current_hour, current_minute = get_invoke_time(local_now)
    hour_phase = get_hour_phase(current_minute)

    logger.debug('Got current invoke time details', extra={
        'day_of_week': current_day_of_week,
        'hour': current_hour,
        'minute': current_minute,
    })

    
    start_db()
    stop_db()

    stop_databases = stop_db()
    start_databases = start_db()
    failure_count = 0 
    
    if (len(start_databases)) > 0:
        for database in start_databases:
            try:
                logger.info('Stopping instance', extra={
                'Database_id': database
                })
                client.start_db_instance(
                    DBInstanceIdentifier=instance
                    )
            except:
                logger.error('Failed to stop DataBase', extra={
                    'DataBase_id': database
                }, exc_info=ex)
                failure_count += 1
                

            
    if (len(stop_databases)) > 0:
        for database in stop_databases:
            try:
                logger.info('Stopping Database', extra={
                'Database_id': database
                })
                client.stop_db_instance(
                    DBInstanceIdentifier=instance
                    )
            except:
                logger.error('Failed to stop DataBase', extra={
                    'DataBase_id': database
                }, exc_info=ex)
                failure_count += 1


    if failure_count:
        raise RecoveredError(f'{failure_count} instance control failures occurred')


