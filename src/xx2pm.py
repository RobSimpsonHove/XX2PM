# -*- encoding: utf-8 -*-

## Started qsdbc's
##      can't pick up as external module
## autofix sort for join!

import os
import sys
import re
import time
import codecs


import configparser

c=configparser.ConfigParser()
c.optionxform=str
with codecs.open(sys.argv[1], 'r', encoding='utf-8') as f:
    c.read_file(f)
config=dict(c.items('xx2pm'))
print(config)


now = time.strftime("%Y%m%d-%H%M%S")
rundir = sys.argv[2]
rundirsub=rundir + '//' + now

if not os.path.exists(rundir):
    os.makedirs(rundir)
if not os.path.exists(rundirsub):
    os.makedirs(rundirsub)
rundirsub=os.path.abspath(rundirsub)

#rundirsub='C:/Users/PBDIA00022/PycharmProjects/XX2PM/mango/20160316-135357'
#parser = configparser.ConfigParser()
# Open the properties file with the correct encoding
#with codecs.open(template, 'r', encoding='utf-8') as f:
#    parser.read_file(f)


class main:
    def __init__(self):

        ## Get list of sources, expanding dirs if necessary
        sources = config.get('sources')
        print(sources)
        self.sourcelist = []
        self.itemlist = []
        for item in sources.split(','):
            print(item)
            print('')
            if os.path.isdir(item):
                print('Checking dir')
                for file in glob.glob(item+"\*.sql"):
                    self.sourcelist.append(file)
                    self.itemlist.append(stem(file))
            else:
                self.sourcelist.append(item)
                self.itemlist.append(stem(item))
        print(self.itemlist)
         ## Pull source data into foci in run directory
        getsources(self.sourcelist)

        ## Run through task list
        self.tasklist = config.get('tasklist').split(',')
        print(self.tasklist)
        executetasklist(self)



def get_previous_focus(tasklist, path, name, currentstep):
    print(tasklist[0])
    if currentstep==tasklist[0]:
        return name+".ftr"
    else:
        prior=False
        for step in reversed(tasklist):
            if prior:
                file=glob.glob(path+"//"+name+"*"+step+"."+"*")
                if file:
                    file=glob.glob(path+"//"+name+"*"+step+"."+"*")[0]
                    return os.path.basename(file)
            elif step==currentstep:
                prior=True

        return name+".ftr"


def stem(fullpathfilename):
    base=os.path.basename(fullpathfilename)    # file.txt
    name=os.path.splitext(base)[0]   # file
    return name


def executetasklist(self):

    for step in self.tasklist:
        for name in self.itemlist:
            #print('SOURCE',source)                                 # source: C:/a/b/c/file.txt
            #path=rundirsub    # C:/a/b/c/
            #base=os.path.basename(source)    # file.txt
            #name=os.path.splitext(base)[0]   # file

            print('Calling gpf:',self.tasklist,rundirsub,name,step)
            prevname=get_previous_focus(self.tasklist,rundirsub,name,step)
            print('Building on',prevname)


            value = config.get( name + '.' + step)

            if value==None:
                print('No ',name + '.' + step)
            else:
                print('value::',value)
                print('self.tasklist',self.tasklist)
                print('step',step)
                print('name',name)
                print('prev',prevname)
                inp=prevname
                out=stem(prevname)+'_'+step

                if re.search('^der',step):
                    derproc(self,inp,out,fdl=value)
                elif re.search('^agg',step):
                    aggproc(self,inp,out,tml=value)
                elif re.search('^join',step):
                    joinproc(self,inp,out,rhs=value)
                elif re.search('^fin',step):
                    finproc(self,step)



def derproc(self,inp,out,fdl):

    if not os.path.isfile(rundirsub+'//'+out+'.ftr'):

        print('Really doing2 derivations  ' + fdl + ' for ' + inp +' to ' + out +'...')

        if not os.path.isfile(fdl):
            print('fdl',fdl)
            fdlfile=rundirsub+'//'+out+'_.fdl'
            f = open(fdlfile, 'w')
            f.write(fdl)
            f.close()
        else:
            copyfile(fdl, rundirsub+'//'+out+'_.fdl')

        qsderive(rundirsub+'//'+out+'_.fdl', rundirsub+'//'+inp, rundirsub+'//'+out+'.ftr')

    else:
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')


def aggproc(self,inp,out,tml):
    print('AGGGS:',out)
    if not os.path.isfile(rundirsub+'//'+out+'.ftr'):

        name=os.path.splitext(inp)[0]

        keys = config.get(name + '.keys',config.get('.keys'))

        if keys!=None:

            if not os.path.isfile(tml):
                print('fdl',tml)
                tmlfile=rundirsub+'//'+out+'_.tml'
                f = open(tmlfile, 'w')
                f.write(tml)
                f.close()
            else:
                copyfile(tml, rundirsub+'//'+out+'_.tml')

            qsmeasure(rundirsub+'//'+out+'_.tml', rundirsub+'//'+inp, rundirsub+'//'+out+'.ftr', keys)

        else:
                print('No key set for ',name)

    else:
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')




def joinproc(self,inp,out,rhs):

    if not os.path.isfile(rundirsub+'//'+out+'.ftr'):

        name=os.path.splitext(inp)[0]

        keys = config.get(name + '.keys',config.get('.keys'))

        if keys!=None:

            rhsfocus=rhs.split('.')[0]
            rhsfields=rhs.split('.')[1]
            if rhsfields=='*':
                rhsfields=None

            qsjoin(rundirsub+'//'+inp, keys, rundirsub+'//'+out+'.ftr',rundirsub+'//'+rhsfocus,fields=rhsfields)
        else:
            print('No key set for ',name)
    else:
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')


def getsources(sourcelist):
    for source in sourcelist:
            #source=os.path.splitext(os.path.basename(source))[0]

            name=os.path.splitext(os.path.basename(source))[0]
            suffix=os.path.splitext(os.path.basename(source))[1]

            print('Fetching ', name, suffix, source)

            if suffix == '.sql':
                getsql(source)
            elif suffix == '.ftr':
                getftr(source)
            elif suffix == '.txt':
                gettxt(source)
            else:
                raise Exception('Suffix '+suffix+' for item '+name+' not known!!!')



def getsql(sql):
    name = os.path.splitext(os.path.basename(sql))[0]

    if not os.path.isfile(rundirsub+'//'+name+'.ftr'):

        udc = config.get(name+'.udc',config.get('.udc'))
        print(udc)
        if udc!=None:
            qsimportdb(udc,sql,rundirsub+'//'+name,force='true')
        else:
            print('UDC not found for ',name)
    else:
        print(name+'.ftr'+' already exists...')


def getftr(ftr):
    pass

def gettxt(txt):
    pass


def runqsdb(command, args):

    qshome='C:/PortraitMiner/server/qs7.0B/win64/bin/'
    print('EXEC',qshome+command,[command]+args)
    result = os.spawnv(os.P_WAIT, qshome+command, [command]+args)
    return result

def qsimportdb(udc,sql,output,fields=None,xfields=None,force=None):

    args=[]
    args.extend(['-udc',udc,'-sql',sql,'-output',output])

    for arg in ['fields','xfields']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['force']:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdb('qsimportdb.exe', args)


def qsmeasure(aggregations, input, output, keys, fields=None, xfields=None, force=None):

    args=[]
    args.extend(['-aggregations',aggregations,'-input',input,'-output',output,'-keys',keys])

    for arg in ['fields','xfields']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['force']:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdb("qsmeasure.exe", args)



def qsderive(derivations, input, output=None):

    args=[]
    args.extend(['-derivations',derivations,'-input',input])

    for arg in ['output']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in []:
        if eval(arg):
            args.extend(['-'+arg])


    runqsdb("qsderive.exe", args)


def qsjoin(input, keys, output, join, fields=None, xfields=None, force=None):

    presort=True
    if presort:
        input=qssortfix(input,keys)
        join=qssortfix(join,keys)

    args=[]
    args.extend(['-input',input,'-keys',keys,'-output',output, '-join', join])

    for arg in ['fields','xfields']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['force']:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdb("qsjoin.exe", args)


def qssortfix(input,keys):

        result=runqsdb("qssort.exe", ['-check -input '+input+' -keys '+keys])

        if result==1:
            sorted=os.path.splitext(input)[0]+'__s.ftr'
            runqsdb("qssort.exe", ['-input '+input+' -keys '+keys+' -output '+sorted])
            input=sorted

        return input





main()