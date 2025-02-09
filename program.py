from mpi4py import MPI 
import sys
import numpy as np
#Deniz Ulaş Poyraz 2021400270

#This class is for creating machine objects. I created a list of machines to navigate easier.
class Machine():
    def __init__(self, childs, current, parent, wear):
        self.childs = childs
        self.current = current
        self.parent = parent
        self.wear = wear
        self.childless = -1
        self.cost = 0
        


#Function to enhance a string.
def enhance(s):
    ret = ""
    s += s[-1]
    ret = s[0] + s
    return ret

#Function to reverse a string.
def reverse(s):
    return s[::-1]

#Function to chop a string.
def chop(s):
    if(len(s) == 1):
        return s
    return s[0: len(s)-1]

#Function to trim a string.
def trim(s):
    if(len(s) <= 2):
        return s
    return s[1:len(s)-1]

#Function to split a string.
def split(s):
    n = len(s)//2 
    if(len(s) % 2 == 0):        
        return s[0:n]
    return s[0:n+1]

#This function takes a machine a string and a list of integers as parameters. 
#It returns the function that performs the desired operation. 
#Also it subtracts the wear factor from the current machine, calculates the cost and modifies the values.
def selectOp(op, s, wearF):
    if(op.current == "reverse"):
        op.current = "trim"
        op.wear -= wearF[1]
        if(op.wear <=0):
            op.cost = (0-op.wear+1)*wearF[1]
        return reverse(s)
    elif(op.current == "enhance"):
        op.current = "split"
        op.wear -= wearF[0]
        if(op.wear <=0):
            op.cost = (0-op.wear+1)*wearF[0]
        return enhance(s)
    elif(op.current == "chop"):
        op.current = "enhance"
        op.wear -= wearF[2]
        if(op.wear <=0):
            op.cost = (0-op.wear+1)*wearF[2]
        return chop(s)
    elif(op.current == "trim"):
        op.current = "reverse"
        op.wear -= wearF[3]
        if(op.wear <=0):
            op.cost = (0-op.wear+1)*wearF[3]
        return trim(s)
    elif(op.current == "split"):
        op.current = "chop"
        op.wear -= wearF[4]
        if(op.wear <=0):
            op.cost = (0-op.wear+1)*wearF[4]
        return split(s)

comm = MPI.COMM_WORLD


size = comm.Get_size()
rank = comm.Get_rank()

fp_in = sys.argv[1]
fp_out = sys.argv[2]

info = open(fp_in, "r").readlines()

if(rank == 0):
    x =open(fp_out, "w")
    x.close()

machine_count = int(info[0])
prod_cycles = int(info[1])
wear_factors = [int(i) for i in info[2].split()]
treshold = int(info[3])
machines = {i+1: Machine([], "", -1, treshold) for i in range(machine_count)}
ops = []
#Fill the machines list with given information
for i in range(4, 4+machine_count-1):
    lst = info[i].split()
    if(len(lst) == 3):
        machines[int(lst[0])].current = lst[2].strip()
        machines[int(lst[0])].parent = int(lst[1])
        machines[int(lst[1])].childs.append(int(lst[0]))
    
#Fill the operations list.
for i in range(4+machine_count-1, len(info)):
    ops.append(info[i].strip())

#Determine if a child is leaf or not.
c = 0
for i in range(1, machine_count+1):
    m = machines[i]
    if(len(m.childs) == 0):
        m.childless = c
        c +=1


maintenance = []
#Do prod_cycle operations
for p in range(prod_cycles):
    weared_out = []
    for i in range(1,machine_count):
        if(comm.iprobe(source=i, tag=1)):
            weared_out.append(i)
    #Control room, also acts like the last machine.
    if(rank == 0):
        out_f = open(fp_out, "a")
        mac = machines[1]
        final = ""
        for i in mac.childs:
            final += comm.recv(source=i-1, tag=0)
        for i in weared_out:
            msg = bytearray(1024)
            req = comm.irecv(msg, source=i, tag=1)
            req.wait
            maintenance.append(msg.decode("utf-8").rstrip('\0'))
        out_f.write(f"{final}\n")

    else:
        #Loop through all the machines.
        for i in range(1, machine_count+1):
            if(rank == i):
                mac = machines[i+1]
                datas = ""
                send = ""
                #If machine has no child make them perform an operation from ops list.
                if(len(mac.childs) == 0):
                    send = selectOp(mac, ops[mac.childless], wear_factors)
                    if(mac.wear<=0):
                        msg = f"{i+1}-{mac.cost}-{p+1}".encode("utf-8")
                        comm.Isend(msg, dest=0, tag=1)
                        mac.wear = treshold
                    comm.send(send, dest=mac.parent-1, tag=0)
                #Else wait for the children to finish, then perform the operation and send it to parent.
                else:
                    for j in mac.childs:
                        datas += comm.recv(source=j-1, tag=0)
                    send = selectOp(mac, datas, wear_factors)
                    if(mac.wear<=0):
                        msg = f"{i+1}-{mac.cost}-{p+1}".encode("utf-8")
                        comm.Isend(msg, dest=0, tag=1)
                        mac.wear = treshold
                    comm.send(send, dest=mac.parent-1, tag=0)
#Write maintenance values to the file                
if(rank == 0):
    out_f = open(fp_out, "a")
    for i in maintenance:
        out_f.write(f"{i}\n")
    out_f.close()
