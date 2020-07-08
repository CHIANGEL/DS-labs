# Generation a list of commands
# Just use put
import os
import random
import pickle

commands = [
    'get',
    'put',
    'delete',
    'make_persistence',
]

file_num = 500
command_Nums = 10
key = list('abcdefghijklmnopqrstuvwxyz1234567890')
chars = '1234567890qwertyuiopasdfghjklzxcvbnm'
lenchars = len(chars)
low = 20
high = 40
longerkeys = [i + j  + k for i in key for j in key for k in key]

#----------------------
# Pure put operations
#----------------------

for file_idx in range(file_num):
    with open('testcases/random_puts_{}.txt'.format(file_idx), 'w') as f:
        for _ in range(command_Nums):
            length = random.randint(low, high)
            k = longerkeys[random.randint(0, len(longerkeys) - 1)]
            c = ''
            for i in range(length):
                c += chars[random.randint(0, lenchars - 1)]
            f.write("put {} {}\n".format(k, c))
        f.write("#\n")