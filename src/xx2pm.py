# -*- encoding: utf-8 -*-

##      can't pick up as external module

##  qs: copy, importdb, measure, derive, join, importmetadata,
##  qsjoinplus, qsmeasureplus: do sorts if required
##  qssortfix
##  TODO failing to pick up metadata
    ## *** Error: C:\Users\PBDIA00022\PycharmProjects\XX2PM\mango\20160410-160820\metadata_.qsfm (The system cannot find the file specified)
##  TODO 'score' *.ftr to DB table

## DOCUMENT
## sources= ....  file or dir, expands dir for *.sql
## taskslist: der, agg, join, meta, copy
## meta task: file or dir, expands dir for *.qsfm

## $today YYYYMMDD, $now YYYYMMDD-HHMMSS

import os
import sys
import re
import time
import codecs
import glob
from shutil import copyfile
import configparser
import xml.etree.ElementTree as ET


import argparse

parser = argparse.ArgumentParser(description='This is a Miner databuild template')
parser.add_argument('-d','--dir', help='Run directory',required=False)
parser.add_argument('-p','--param',help='Build parameters', required=True)
args = parser.parse_args()
#args.dir='20160421-161226'

qshome='C:/PortraitMiner7.0B/server/qs7.0B/win32/bin/'

## Set timestamps
now = time.strftime("%Y%m%d-%H%M%S")
today = time.strftime("%Y%m%d")

## Set and create run directories
print(args.dir, args.param)

rundirroot = '../rundirs'
if args.dir:
    rundirsub=os.path.abspath(rundirroot + '/' + args.dir)
else:
    rundirsub=os.path.abspath(rundirroot + '/' + now)

if not os.path.exists(rundirroot):
    os.makedirs(rundirroot)
if not os.path.exists(rundirsub):
    os.makedirs(rundirsub)

## Replace token in tempfile
f = open(args.param,'r')
origtemplate = f.read()
f.close()

template = origtemplate.replace("$today",today)
template = template.replace("$now",now)
print(template)
f = open(rundirsub+'/template.properties','w', encoding='utf-8')
f.write(template)
f.close()


c=configparser.ConfigParser()
c.optionxform=str
with codecs.open(rundirsub+'/template.properties', 'r', encoding='utf-8') as f:
    c.read_file(f)
config=dict(c.items('xx2pm'))
print(config)


#rundirsub='C:/Users/PBDIA00022/PycharmProjects/XX2PM/mango/20160316-135357'
#parser = configparser.ConfigParser()
# Open the properties file with the correct encoding
#with codecs.open(template, 'r', encoding='utf-8') as f:
#    parser.read_file(f)


class main:
    def __init__(self):

        ## Get list of sources, expanding dirs if necessary
        sources = config.get('sources')
        print('sources:',sources)
        self.sourcelist, self.itemlist = get_search_items(sources, ['.sql', '.ftr'])
         ## Pull source data into foci in run directory
        getsources(self.sourcelist)

        ## Run through task list
        self.tasklist = config.get('tasklist').split(',')
        print(self.tasklist)
        executetasklist(self)


def get_search_items(filesOrDirs, extensions):
    ## extensions = (".qsfm","*.jpg","*.jpeg",)
    itemlist = []

    for item in filesOrDirs.split(','):
        print('Search item',item)
        base,_,_=stem(item)

        ## Search locally in rundir if no path given
        if base==item:
            item=rundirsub+'/'+item

        print('Checking dir',item)
        list = []
        _,name,suffix=stem(item)
        for extension in extensions:
            if suffix==extension:
                search=item
            else:
                search=item+extension

            print('looking for pattern:',search)
            list.extend(glob.glob(search))
        itemlist.extend(list)
    print('Found items:',itemlist)

    namelist = []
    for item in itemlist:
        _,name,_=stem(item)

        namelist.append(name)

    return itemlist, namelist


def get_previous_focus(tasklist, path, name, currentstep):

    if currentstep==tasklist[0]:
        return name+".ftr"
    else:
        prior=False
        for step in reversed(tasklist):
            if prior:
                file=glob.glob(path+"//"+name+"_*"+"_"+step+".ftr")
                if file:
                    file=file[0]
                    return os.path.basename(file)
            elif step==currentstep:
                prior=True

        return name+".ftr"


def stem(fullpathfilename):
                                                # C:/mydir/bdir/file.txt
    base=os.path.basename(fullpathfilename)     # file.txt
    name=os.path.splitext(base)[0]              # file
    suffix=os.path.splitext(base)[1]            # txt
    return base,name,suffix


def executetasklist(self):

    for task in self.tasklist:
        print('Task:', task)
        for name in self.itemlist:
            #print('SOURCE',source)                                 # source: C:/a/b/c/file.txt
            #path=rundirsub    # C:/a/b/c/
            #base=os.path.basename(source)    # file.txt
            #name=os.path.splitext(base)[0]   # file

            #print('Calling gpf:',self.tasklist,rundirsub,name,step)
            prevname=get_previous_focus(self.tasklist,rundirsub,name,task)
            #print('Building on',prevname)

            value = config.get( task + '.' + name)
            print(task,name,value)
            if value is None:
                pass
            else:
                print('DOING ',task + '.' + name,'=', value)
                inp=prevname
                _,out,_=stem(prevname)
                out=out+'_'+task

                if re.search('^der',task):
                    derproc(self,inp,out,task,fdl=value)
                elif re.search('^agg',task):
                    aggproc(self,inp,out,tml=value)
                elif re.search('^join',task):
                    joinproc(self,inp,out,rhs=value)
                elif re.search('^metaN',task):
                    metaNproc(self,inp,out,meta=value)
                elif re.search('^metax',task):
                    metaxproc(self,inp,meta=value)
                elif re.search('^meta',task):
                    metaproc(self,inp,out,meta=value)
                elif re.search('^copy',task):
                    copyproc(self,inp,to=value)
                #elif re.search('^cmd',step):
                #    cmdproc(self,inp,out,meta=value)
                elif re.search('^fin',task):
                    finproc(self,task)
                else:
                    raise Exception('Task',task,'not a supported task')


def metaxproc(self,inp,meta):
    _,name,_=stem(inp)
    qsexportmetadata(rundirsub+'//'+inp, rundirsub+'//'+name+'_'+meta+'.qsfm')


def metaNproc(self,inp,out,meta):
    metaproc(self,inp,out,meta,Nway=True)

def metaproc(self,inp,out,meta,Nway=False):

    qsfmlist, namelist = get_search_items(meta, ['.qsfm'])
    lastout=None
    for i in range(len(qsfmlist)):
        qsfm=qsfmlist[i]
        name=namelist[i]
        if os.path.dirname(qsfm) != rundirsub:
                copyfile(qsfm, rundirsub+'//'+name+'.qsfm')

        thisin,thisout=sequence(Nway,inp,out,i,namelist)
        print('this:',thisin,thisout)
        qsimportmetadata(rundirsub+'/'+thisin, rundirsub+'/'+name+'.qsfm', rundirsub+'/'+thisout, warn='true')


def sequence(Nway,inp,out,i,namelist):
    name=namelist[i]
    last=namelist[-1]
    if Nway:    #   One output per parameter: e.g. metadata scoring to multiple files
        infile=inp
        outfile=out+'_'+name+'.ftr'
    else:       #   One output total: e.g. serial applications of metadata
        if i==0:                    ## first input to name
            infile=inp
            outfile=out+'_'+name+'.ftr'
            lastout=outfile
        elif name!=last :           ## middle input from previous to name
            infile=out+'_'+namelist[i-1]
            outfile=out+'_'+name+'.ftr'
        else:                       ## last input from previous to out
            infile=out+'_'+namelist[i-1]
            outfile=out+'.ftr'

    return  infile,outfile





def derproc(self,inp,out,task,fdl):

    if not os.path.isfile(rundirsub+'//'+out+'.ftr'):

        ## Make local copy of fdl
        fdlfile=rundirsub+'//'+out+'_.fdl'
        if not os.path.isfile(fdl):
            print('fdl',fdl)
            f = open(fdlfile, 'w')
            f.write(fdl)
            f.close()
        else:
            copyfile(fdl, fdlfile)

        var = config.get(task+'.var')
        if var is not None:

            trim = config.get(task+'.trim')
            if trim is not None:
                if type(trim) is tuple:
                    print('TUPLE')
                    regex=trim[0]
                    replace=trim[1]
                else:
                    regex=trim
                    replace=''
            else:
                regex='dummy'
                replace='dummy'

            print(regex,replace)

            fields=getmatchingfields(rundirsub+'//'+inp,var)
            print('FIELDS!!:',fields)
            fdlfile_2=rundirsub+'//'+out+'_2.fdl'

            fout = open(fdlfile_2,'w')

            with open(fdlfile, 'rU') as fin:
                for line in fin:
                    if not(re.search('nvl',line)):
                        print('#',line)
                        fout.write(line)
            fout.write('\n')
            for field in fields:
                if trim is not None:
                    field=re.sub(regex,replace,field)

                with open(fdlfile, 'rU') as fin:
                    for line in fin:
                        if re.search('\$var',line):
                            print(re.sub('\$var',field,line))
                            fout.write(re.sub('\$var',field,line))

            fin.close()
            fout.close()
            fdlfile=fdlfile_2


        qsderive(fdlfile, rundirsub+'//'+inp, rundirsub+'//'+out+'.ftr')

    else:
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')


def copyproc(self,ffrom,to):

    qscopy(rundirsub+'//'+ffrom, to+'.ftr')


def aggproc(self,inp,out,tml):

    if not os.path.isfile(rundirsub+'//'+out+'.ftr'):

        name=os.path.splitext(inp)[0]

        keys = config.get(name + '.keys',config.get('.keys'))
        library = config.get(name + '.library',config.get('.library'))

        if keys is not None:

             ## Make local copy of tml
            if not os.path.isfile(tml):
                print('tml',tml)
                tmlfile=rundirsub+'//'+out+'_.tml'
                f = open(tmlfile, 'w')
                f.write(tml)
                f.close()
            else:
                copyfile(tml, rundirsub+'//'+out+'_.tml')

            input=qssortfix(rundirsub+'//'+inp,keys)
            qsmeasure(rundirsub+'//'+out+'_.tml', input, rundirsub+'//'+out+'.ftr', keys)

        else:
                print('No key set for ',name)

    else:
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')



def joinprocOld(self,inp,out,rhs):

    if not os.path.isfile(rundirsub+'//'+out+'.ftr'):

        name=os.path.splitext(inp)[0]

        keys = config.get(name + '.keys',config.get('.keys'))

        if keys is not None:
            raise Exception('No key set for ',name)

        rhsfocus=rhs.split('.')[0]
        if(len(rhs.split('.')))==2:
            rhsfields=rhs.split('.')[1]
        else:
            rhsfields=None

        qsjoinplus(rundirsub+'//'+inp, keys, rundirsub+'//'+out+'.ftr',rundirsub+'//'+rhsfocus,fields=rhsfields)

    else:
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')


def joinproc(self,inp,out,rhs):

    if not os.path.isfile(rundirsub+'//'+out+'.ftr'):

        name=os.path.splitext(inp)[0]

        keys = config.get(name + '.keys',config.get('.keys'))
        print('name is',name)
        if keys is None:
            raise Exception('No key set for ',name)

        rhscount=len(rhs.split(';'))

        for j in rhs.split(';'):

            rhsfocus=j.split('.')[0]
            if(len(j.split('.')))==2: ## fieldnames exist
                rhsfields=j.split('.')[1]
            else:
                rhsfields=None

            if j==rhs.split(';')[0]:
                qsjoinplus(rundirsub+'//'+inp, keys, rundirsub+'//'+out+'.ftr',rundirsub+'//'+rhsfocus,fields=rhsfields)
            else:
                qsjoinplus(rundirsub+'//'+out, keys, rundirsub+'//'+out+'.ftr',rundirsub+'//'+rhsfocus,fields=rhsfields)


#   itemlist, namelist = get_fd_items(meta,['*.qsfm'])
#   lastout=None
#   for i in range(len(itemlist)):
#       qsfm=itemlist[i]
#       name=namelist[i]
#       copyfile(qsfm, rundirsub+'//'+name+'.qsfm')

#       thisin,thisout=sequence(Nway,inp,out,i,namelist)
#       print('this:',thisin,thisout)
#       qsimportmetadata(rundirsub+'/'+thisin, rundirsub+'/'+name+'.qsfm', rundirsub+'/'+thisout, warn='true')



def getsources(sourcelist):
    for source in sourcelist:
            #source=os.path.splitext(os.path.basename(source))[0]

            name=os.path.splitext(os.path.basename(source))[0]
            if '_' in name:
                raise Exception('UNDERSCORE IN SOURCE NAME '+name)
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
        if udc is not None:
            qsimportdb(udc,sql,rundirsub+'//'+name,force='true')
        else:
            print('UDC not found for ',name)
    else:
        print(name+'.ftr'+' already exists...')


def getftr(ftr):
    name = os.path.splitext(os.path.basename(ftr))[0]

    if not os.path.isfile(rundirsub+'//'+name+'.ftr'):
        qscopy(ftr,rundirsub+'//'+name+'.ftr',force='true')
    else:
        print(name+'.ftr'+' already exists...')



def gettxt(txt):
    pass


############### QSDBC's #######################################################

def runqsdbUsual(command, args, failonbad=True):

    print('EXECUTING',qshome+command,[command]+args)
    result = os.spawnv(os.P_WAIT, qshome+command, [command]+args)
    #print('Result',result)
    if result==1 and failonbad:
            raise Exception( command+' failed for ',args)
    return result


def runqsdb(command, arglist, failonbad=True):
    print('args', arglist)

    command2=qshome+command
    for a in arglist:
        command2=command2+' '+a
    print('EXECUTING in os.system:',command2)
    os.system(command2)
    #result = os.spawnv(os.P_WAIT, qshome+command, [command]+args)
    return 0


def qscopy(ffrom,to,force=None):

    args=[]
    args.extend(['-from',ffrom,'-to',to])

    for arg in []:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['force']:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdb('qscopy', args)


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


def qsmeasure(aggregations, input, output, keys, fields=None, xfields=None, force=None, library=None):

    args=[]
    args.extend(['-aggregations',aggregations,'-input',input,'-output',output,'-keys',keys])

    for arg in ['fields','xfields','library']:
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

    args=[]
    args.extend(['-input',input,'-keys',keys,'-output',output, '-join', join])

    for arg in ['fields','xfields']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['force']:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdb("qsjoin.exe", args)


#qsimportmeta(rundirsub+'//'+inp, rundirsub+'//'+out+'_.qsfm', rundirsub+'//'+out+'.ftr',warn='true')

def qsimportmetadata(input, metadata, output=None, fields=None, warn=None):

    args=[]
    args.extend(['-input',input,'-metadata',metadata])

    for arg in ['output','fields']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['warn']:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdb("qsimportmetadata.exe", args)


def qsexportmetadata(input, metadata):

    args=[]
    args.extend(['-input',input,'-output',metadata])

    for arg in []:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in []:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdb("qsexportmetadata.exe", args)


def qssortfix(input,keys):

        sorted=os.path.splitext(input)[0]+'__s.ftr'
        if os.path.isfile(rundirsub+'//'+sorted):
            return sorted

        result=runqsdb("qssort.exe", ['-check -input '+input+' -keys '+keys],False)
        if result==1:
            runqsdb("qssort.exe", ['-input '+input+' -keys '+keys+' -output '+sorted])
            input=sorted

        return input


def qsjoinplus(input, keys, output, join, fields=None, xfields=None, force=None):

    presort=True
    if presort:
        input=qssortfix(input,keys)
        join=qssortfix(join,keys)

    qsjoin(input, keys, output, join, fields, xfields, force)


def qsmeasureplus(aggregations, input, output, keys, fields=None, xfields=None, force=None):

    presort=True
    if presort:
        input=qssortfix(input,keys)

    qsmeasure(aggregations, input, output, keys, fields, xfields, force)


#main()


import csv

def foo(a,b):
    print(a)
    print(b)


import csv

def getfocusfieldsOld(focus):
    _,name,_ = stem(focus)
    runqsdb('qsdescribe',['-input', focus, '-fields', '-output', rundirsub+name+'__fields.txt'])
    fields={}

    reader = csv.reader(open(rundirsub+name+'__fields.txt', 'r'))
    for row in reader:
        try:
            x=row[0].split()
            dummy=int(x[0])   ##  First element is a number
            print(x )
            fields[x[1]] = {'seq':x[0], 'type':x[2], 'interps':x[3]}
        except:
                pass
    return fields

def getfocusfields(focus):
    _,name,_ = stem(focus)
    runqsdb('qsexportmetadata',['-input', focus, '-output', rundirsub+'//'+name+'.qsfm'])
    fields=[]
    fielddict={}


    tree = ET.parse(rundirsub+'//'+name+'.qsfm')
    doc = tree.getroot()
    for a in doc.findall('{http://www.quadstone.com/xml}field'):
        fieldname=a.get('name')
        fieldtype=a.get('type')
        print(fieldname,fieldtype)
        fields.append(fieldname)
        fielddict[fieldname] = {'type':fieldtype}
    return fields, fielddict



def getmatchingfields(focus,patterns):
    fields, fielddict=getfocusfields(focus)
    matchingfields=[]
    for p in patterns.split(','):
        p=p.strip()
        t=None
        if ':' in p:
            t=p.split(':')[1]
            p=p.split(':')[0]

        notregex=len(re.sub("[a-zA-Z0-9_]","",p))==0
        p='^'+p+'$'
        latestmatchingfields=[]
        for f in fields:
            if re.match(p,f,re.IGNORECASE) and (t==None or re.match(t,fielddict[f]['type'],re.IGNORECASE)):
                latestmatchingfields.append(f)
        if latestmatchingfields==[]:
            print("WARNING: field ",p,"not found in",focus)
        matchingfields.extend(latestmatchingfields)
    dedupematchingfields=[]
    for i in matchingfields:
        if i not in dedupematchingfields:
            dedupematchingfields.append(i)
    return matchingfields


main()
#print(re.sub('create *([A-Za-z0-9_()\.\*]*).*','\\1',n))

#var=

