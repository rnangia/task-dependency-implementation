from threading import Timer, Thread, enumerate
import datetime
import random

# data-structure tree
class Tree:

    # dat can be of any datatype
    def __init__(self,dat,left=None,right=None):
        self.dat = dat
        self.left = left
        self.right = right

    def __str__(self):
        return str(self.dat)

    def print_preorder(self):
        if self == None:
            return
        if self.left is not None:
            self.left.print_preorder()
        if self.right is not None:
            self.right.print_preorder()

    # return postorder in a list
    def postorder(self):
        data = []

        def recurse(node):
            if not node:
                return
            recurse(node.left)
            recurse(node.right)
            data.append(node.dat)

        recurse(self)
        return data

    # this returns the immediate children i.e. immediate tasks dependent upon
    def has_children(self,val):
        data = []

        def recurse(node):
            if not node:
                return
            recurse(node.left)
            recurse(node.right)
            if node.dat == val:
                if node.left is not None:
                    data.append(node.left.dat)
                if node.right is not None:
                    data.append(node.right.dat)

        recurse(self)
        return data

    # returns parent of the node
    def parent(self,val):
        data = []
        def recurse(node,parent=None):
            if not node:
                return
            recurse(node.left,node.dat)
            recurse(node.right,node.dat)
            if node.dat == val:
                data.append(parent)

        recurse(self)
        return data[0]



# *********************************************************************************************************************************** #
# constructing a Tree & other functions

node1 = Tree(8,None,None)
node2 = Tree(9,node1,None)
node3 = Tree(16,None,None)
node4 = Tree(11,node2,node3)
node5 = Tree(2,None,None)
node6 = Tree(5,node5,node4)
node7 = Tree(29,None,None)
node8 = Tree(38,None,None)
node9 = Tree(35,node7,node8)
root = Tree(17,node6,node9)

#printing the tree in pre-order traversal format
root.print_preorder()

# postorder traversal of the tree in a list
postorderLst = root.postorder()

# checking parent is correctly identified
print("parent of 17 is ",root.parent(17))

# *********************************************************************************************************************************** #
# implementing a queue of lists ( in python it is essentially a list of lists ) : for easy parallelizing of tasks
# the queue contains leaf nodes ( tasks with no dependency) (level 0), tasks which depend on leaf nodes (level 1), tasks depending on those in level 1 etc.

def parallelize_tasks(tree,lst,expected_children=[],queue=[]):
    newlist = []
    for i in lst:
        children = tree.has_children(i)
        if expected_children == [] and children == []:
            newlist.append(i)
        elif expected_children != []:
            bl = []
            for j in children:
                if j in expected_children:
                    bl.append(True)
                else:
                    bl.append(False)
            if bl != [] and False not in bl:
                newlist.append(i)
    newlist = list(set(newlist)-set(expected_children))
    queue.append(newlist)
    expected_children = list(set(expected_children + newlist))

    # NOTE: This is because the list "lst" was arranged in postorder!
    if (lst[len(lst)-1] == newlist[0]):
        return queue
    else:
        return parallelize_tasks(tree,lst,expected_children,queue)


# dependency structure of tasks
queue = parallelize_tasks(root,postorderLst)


# print("stack is ",stack)
cpQueue = list(queue)
while cpQueue:
    print(cpQueue.pop(0))


# *********************************************************************************************************************************** #
# Python dynamic function with custom names
# this function delays execution of task by random number of seconds

def task(*args):
    if args[1]:
        print("function",args[0],"was delayed by",str((args[1])[0]))
    else:
        print("function was delayed by ",args[0])

def bindFunction(*args):
    def func(*args):
        delay = int(random.random()*10)
        # print("inside function with delay = ", delay)
        if args:
            r = Timer(delay, task, (args[0],[delay]))
        else:
            r = Timer(delay, task, [delay])
        r.start()
        r.join()
    return func

# defining tasks dynamically (closure)
for i in range(0,len(postorderLst)):
    exec("%s = bindFunction()" % ("func"+str(postorderLst[i])))

# making sure that this closure & dynamic definitions work
func38("func38")

# *********************************************************************************************************************************** #
# Worker Function that is run on each thread calling a dynamic function

def worker(st):
    # print("in worker trying to execute func",st)
    exec("func%s(\"%s\")" % (st,st))
    return

# *********************************************************************************************************************************** #
# This piece of code just runs every task on a different thread ( here we understand the basics of the threading library in python )

print("*"*150)

threads = []
for i in range(0,len(postorderLst)):
    name = "func"+ str(postorderLst[i])
    t = Thread(name=name,target=worker,args=(str(postorderLst[i]),))
    threads.append(t)
    t.start()

print("processes running are ",enumerate())

for t in threads:
    t.join()

# *********************************************************************************************************************************** #
# This code implements level by level (i.e. it parallelizes independent tasks (each level) and serializes according to dependency)
# easiest way would be just wait level by level (queue) which is shown below (least complexity)

print("*"*150)

threads = []
while queue:
    tasks = queue.pop(0)
    print("tasks:",tasks,"starting at:",datetime.datetime.now())
    for i in range(0,len(tasks)):
        t = Thread(target=worker,args=(str(tasks[i]),))
        threads.append(t)
        t.start() # now running the thread
    #now wait for each thread to finish
    for t in threads:
        t.join() # adding t to the main thread and will wait till all daemon threads in main complete
    print("tasks:",tasks,"finishing at:",datetime.datetime.now())


# *********************************************************************************************************************************** #
# This following code only seeks counsel of whether its children have finished their tasks, it is brute force ( iterating continuously)
# faster but expensive in terms of computational cycles

# we shall attempt to start every task but the task cannot start unless its children have finished

print("*"*150)

class BreakIt(Exception): pass  # we need to break out of all loops below

try:
    lst = list(postorderLst)
    rootnode = lst[len(lst)-1]
    processesStarted = []
    while 1:
        for i in lst:
            # for each node, find its children
            children = root.has_children(i)
            if children != []:
                # find if child tasks finished!
                flag1 = 0
                flag2 = 0
                for j in children:
                    for tr in enumerate():
                        if tr.getName() == ("func"+str(j)):
                            flag1 += 1

                    if j in processesStarted:
                        flag2 += 1

                if flag1 == 0 and (i not in processesStarted) and flag2 == len(children):
                    print("starting process",i)
                    processesStarted.append(i)
                    name = "func" + str(i)
                    t = Thread(name=name,target=worker,args=(str(i),))
                    t.start()
                    lst.remove(i)

            else:
                print("starting process",i)
                processesStarted.append(i)
                name = "func" + str(i)
                t = Thread(name=name,target=worker,args=(str(i),))
                t.start()
                lst.remove(i)  # shrinking the list so that useless loop iterations are saved

            if (rootnode in processesStarted):
                print("root has started, breaking")
                raise BreakIt # normal break only breaks out of 1 loop, we need to break out of all loops
except BreakIt:
    pass
