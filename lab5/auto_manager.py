from optparse import OptionParser
import subprocess
import time

parser = OptionParser(
    usage="Manager for auto test"
)
parser.add_option(
    "--file",
    metavar="file",
    type="string",
    help="The test file to be read")
parser.add_option(
    "--num",
    metavar="num",
    type=int,
    help="Number of subprocesses")
(options, args) = parser.parse_args()

process_num = options.num
processes = [None for _ in range(process_num)]

print("Creating subprocesses...")
for idx in range(process_num):
    processes[idx] = subprocess.Popen("python auto_client.py --file ./testcases/{}_{}.txt".format(options.file, idx), shell=True)
print("Finish Creating subprocesses...")

start = time.time()
count = process_num
while count:
    for idx in range(process_num):
        if subprocess.Popen.poll(processes[idx]) == 0:
            count -= 1
allTime = time.time() - start
print("==========================>TIME: {}".format(allTime))