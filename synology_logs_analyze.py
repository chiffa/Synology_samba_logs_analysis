from csv import reader as csv_reader
from collections import defaultdict, Counter
from datetime import datetime

# DEVLOG:
#
# add a mappibl between files and directories accessed and users
# store counts in a database and update based on the latest import date.
# limit report to a single user

total_rw = 0
per_user_rw = defaultdict(float)

multiplier_table = {
    'Bytes': 1.0/1024.0**3,
    'KB': 1.0/1024.0**2,
    'MB': 1.0/1024.0,
    'GB': 1}

first = None
last = None

syspath_counter = Counter()
syspath_dir_counter = Counter()

syspath_traffic_counter = defaultdict(float)

special_file = "/common/Common/Lab Meetings & Journal Club/Lab Business/2015 Lab Business"
special_file_counter = Counter()

with open("C:\Users\Andrei\Downloads\smbxferlog_2017-2-22-18-15-2.csv", 'r') as source:
    reader = csv_reader(source)
    header = reader.next()
    header = reader.next()
    print header
    for line in reader:
        date = line[1]
        if not first:
            first = datetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        user = line[3]
        file_size = line[6]
        file_name = line[7]
        base_folder = '/'.join(line[7].split('/')[:-1])
        syspath_counter[file_name] += 1
        syspath_dir_counter[base_folder] += 1
        if file_name == special_file or base_folder == special_file:
            special_file_counter[user] +=1
        if file_size != 'NA':
            file_size = float(file_size.split(' ')[0]) * multiplier_table[file_size.split(' ')[1]]
            per_user_rw[user] += file_size
            total_rw += file_size
            syspath_traffic_counter[file_name] += file_size
    last = datetime.strptime(date, '%Y/%m/%d %H:%M:%S')

print 'total read-writes %.2f GB' % total_rw
print 'logged time:', first-last
log_span = (first-last).days+(first-last).seconds/86399.0
print 'yearly estimated traffic %.2f GB' % (total_rw/log_span*365)

for key, value in per_user_rw.iteritems():
    per_user_rw[key] = float('%.2f' % value)

print '\nper-user report:\n'

for key, value in sorted(per_user_rw.items(), key=lambda x: x[1], reverse=True):
    print key, value

print '\n\nfile access report report:\n'

for key, value in sorted(syspath_dir_counter.items(), key=lambda x: x[1], reverse=True):
    if value > 20*log_span:
        print value, key

print '\n\nfile access report:\n'

for key, value in sorted(syspath_counter.items(), key=lambda x: x[1], reverse=True):
    if value > 2*log_span:
        print value, key

print '\n\nper-file traffic report:\n'

for key, value in sorted(syspath_traffic_counter.items(), key=lambda x: x[1], reverse=True):
    if value > 1*log_span:
        print '%.2f' % value, key

print '\n\nspecial_file access pattern:\n'

for key, value in sorted(special_file_counter.items(), key=lambda x: x[1], reverse=True):
    if value > 1*log_span:
        print '%.2f' % value, key
