# -*- encoding: utf-8 -*-

##      can't pick up as external module

##  qs: copy, importdb, measure, derive, join, importmetadata,
##  qsjoinplus, qsmeasureplus: do sorts if required
##  qssortfix
##  TODO failing to pick up metadata
    ## *** Error: C:\Users\PBDIA00022\PycharmProjects\XX2PM\mango\20160410-160820\metadata_.qsfm (The system cannot find the file specified)
##  TODO 'score' *.ftr to DB table
## TODO command line command
##  TODO What happens when splitting a thread: B, B_der, B_der_agg,

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
import datetime
from dateutil.relativedelta import *
import argparse
import csv
exec(open('local.py').read())

parser = argparse.ArgumentParser(description='This is a Miner databuild template')
parser.add_argument('-d','--dir', help='Run directory',required=False)
parser.add_argument('-p','--param',help='Build parameters', required=True)
args = parser.parse_args()
#args.dir='20160421-161226'


## Set timestamps
now = time.strftime("%Y%m%d-%H%M%S")
today = time.strftime("%Y%m%d")

## Set and create run directories
print('ARGS.DIR:', args.dir)
print('ARGS.PARAM:', args.param)

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
#print(template)
f = open(rundirsub+'/template.properties','w', encoding='utf-8')
f.write(template)
f.close()


c=configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
c.optionxform=str
with codecs.open(rundirsub+'/template.properties', 'r', encoding='utf-8') as f:
    c.read_file(f)
config=dict(c.items('xx2pm'))
print('Config File:')
print(config)

foo = config.get('foo')
print('FOO',foo)


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


            # Turn regex into glob pattern
            search=search.replace('.*','*')
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
    print('tasklist, path, name, currentstep: ',tasklist, path, name, currentstep)
    input=config.get(currentstep+'.input')
    if input is not None:
        print('AAA')
        return input+'.ftr'
    elif currentstep==tasklist[0]:
        print('BBB')
        return name+'.ftr'
    else:
        prior=False
        for step in reversed(tasklist):
            if prior:
                print('looking for focus',path+"//"+name+"_"+step+".ftr")
                file=glob.glob(path+"//"+name+"_"+step+".ftr")
                if file:
                    file=file[0]
                    print('FOUND PREV:',file)
                    print('CCC')
                    return os.path.basename(file)
            elif step==currentstep:
                print('(Ignoring step',step,')')
                prior=True
        print('previous is:',name)
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
            print('TNV:',task,name,value)
            if value is None:
                pass
            else:
                print('DOING ',task + '.' + name,'=', value)
                inp=prevname
                _,out,_=stem(prevname)
                out=name+'_'+task

                print('DOING task.name=value',task + '.' + name,'=', value, 'inp:',inp, 'out:',out)

                if re.search('^der',task):
                    derproc(self,inp,out,task,fdl=value)
                elif re.search('^agg',task):
                    aggproc(self,task,inp,out,tml=value)
                elif re.search('^join',task):
                    joinproc(self,inp,out,task,rhs=value)
                elif re.search('^metan',task):
                    metanproc(self,inp,out,meta=value)
                elif re.search('^metax',task):
                    metaxproc(self,inp,meta=value)
                elif re.search('^meta',task):
                    metaproc(self,inp,out,meta=value)
                elif re.search('^copy',task):
                    copyproc(self,inp,to=value)
                elif re.search('^rename',task):
                    renameproc(self,task,inp,out,map=value)
                #elif re.search('^cmd',step):
                #    cmdproc(self,inp,out,meta=value)
                elif re.search('^fin',task):
                    finproc(self,task)
                else:
                    raise Exception('Task',task,'not a supported task')


def metaxproc(self,inp,meta):
    _,name,_=stem(inp)
    qsexportmetadata(rundirsub+'//'+inp, rundirsub+'//'+name+'_'+meta+'.qsfm')


def metanproc(self,inp,out,meta):
    metaproc(self,inp,out,meta,parallel=True)

def metaproc(self,inp,out,meta,parallel=False):
    print('metaproc: ',meta)
    qsfmlist, namelist = get_search_items(meta, ['.qsfm'])
    lastout=None
    for i in range(len(qsfmlist)):
        qsfm=qsfmlist[i]
        name=namelist[i]
        if os.path.dirname(qsfm) != rundirsub:
                copyfile(qsfm, rundirsub+'//'+name+'.qsfm')

        thisin,thisout=sequence(parallel,inp,out,i,namelist)
        qsimportmetadata(rundirsub+'/'+thisin, rundirsub+'/'+name+'.qsfm', rundirsub+'/'+thisout, warn='true')


def sequence(parallel, inp, out, i, namelist):
    ##
    name=namelist[i]
    last=namelist[-1]
    if parallel:    #   One output per parameter: e.g. metadata scoring to multiple files
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
    print('SEQ:',parallel, inp, out, i, namelist, infile,outfile)
    return  infile,outfile







def derproc(self,inp,out,task,fdl):

    if os.path.isfile(rundirsub+'//'+out+'.ftr'):
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')
    else:

        ## Make local copy of fdl
        fdlfile=make_copy_file_or_arg(fdl,rundirsub,out+'_.fdl')

        var = config.get(task+'.var')
        if var is not None:
            fdlfile=expand_dollarvar(self, inp, out, fdlfile, task, var)


        qsderive(fdlfile, rundirsub+'//'+inp, rundirsub+'//'+out+'.ftr')



def renameproc(self,task,inp,out,map,suffix=None,prefix=None,_except=None):
    # Map is either a file, old=new list as a string, or fieldname patterns to be used with suffix or prefix

    if os.path.isfile(rundirsub+'//'+out+'.ftr'):
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')
    else:

        # If patterns or fieldlist (not file, nor old=new mappings), then create mapping file

        # If map is i) a file, or ii) inline old=new statements assignations
        if os.path.isfile(map) or '=' in map:

            ## Make local copy of map
            mapfile=make_copy_file_or_arg(map,rundirsub,out+'_.map')

        else:

            # Make suffix/prefix mapping file
            suffix = suffix or config.get(task+'.suffix')
            prefix = prefix or config.get(task+'.prefix')
            _except = _except or config.get(task+'.except')

            if re.search('[^a-zA-Z_0-9,]',map):    # or iii) a regex (not a pure fieldname or list of fieldnames)
                fields=getmatchingfields(inp,map,_except)
            else:
                fields=map                       # or iv) a field, or list of fields

            print('Pre/Suffix:',suffix or prefix, fields)
            mapfile=rundirsub+'//'+task+'_.map'

            newfields=[]
            f = open(mapfile, 'w')
            for field in fields.split(','):
                if suffix:
                    f.write(field+'='+field+suffix)
                    newfields.append(field+suffix)
                else:
                    f.write(field+'='+prefix+field)
                    newfields.append(prefix+field)
                f.write('\n')
            newfields=','.join(newfields)
            f.close()

        qsrenamefields(inp, mapfile, out+'.ftr')

        return out, newfields

def make_copy_file_or_arg(filearg,dir,outname):

    localfile=dir+'//'+outname
    if not os.path.isfile(filearg):
        print(outname,filearg)
        f = open(localfile, 'w')
        f.write(filearg)
        f.close()
    else:
        copyfile(filearg, localfile)
    return localfile


def expand_dollarvar(self, inp, out, fdl_tml_file, task, var):

            trim = config.get(task+'.trim')
            if trim is not None:
                if type(trim) is tuple:
                    # print('TUPLE')
                    regex=trim[0]
                    replace=trim[1]
                else:
                    regex=trim
                    replace=''
            else:
                regex='dummy'
                replace='dummy'

            print('var: ',regex,replace)

            fields=getmatchingfields(rundirsub+'//'+inp,var)
            print('FIELDS!!:',fields,' match ',var)
            if fields==[]:
                for field in var.split(','):
                    fields.append(field)

            fdlfile_2=rundirsub+'//'+out+'_2.fdl'
            fout = open(fdlfile_2,'w')


            #Write non-recurring derivations

            fout.write('/////// User parameters\n')
            fout.write('// var:'+str(var))
            fout.write('\n')
            fout.write('// trim:'+str(trim))
            fout.write('\n')
            fout.write('\n')

            #Write non-recurring derivations
            with open(fdl_tml_file, 'rU') as fin:
                for line in fin:
                    if not(re.search('\$var',line)):
                        #print('#',line)
                        fout.write(line)
            fout.write('\n')

            # Write recurring derivations
            for field in fields:
                print('Field:@@@@@',field)
                if trim is not None:
                    field=re.sub(regex,replace,field)

                with open(fdl_tml_file, 'rU') as fin:
                    for line in fin:
                        if re.search('\$var',line):
                            #print(re.sub('\$var',field,line))
                            fout.write(re.sub('\$var',field,line))
                            fout.write('\n')

            fin.close()
            fout.close()
            return fdlfile_2




def copyproc(self,ffrom,to):

    qscopy(rundirsub+'//'+ffrom, to+'.ftr')



def aggproc(self,task,inp,out,tml):

    if not os.path.isfile(rundirsub+'//'+out+'.ftr'):

        name=os.path.splitext(inp)[0]

        keys = config.get(name + '.keys',config.get('.keys'))
        library = config.get(name + '.library',config.get('.library'))

        if keys is not None:

             ## Make local copy of tml
            tmlfile=make_copy_file_or_arg(tml,rundirsub,out+'_.tml')

            var = config.get(task+'.var')
            if var is not None:
                tmlfile=expand_dollarvar(self, inp, out, tmlfile, task, var)

            input=qssortfix(rundirsub+'//'+inp,keys)
            qsmeasure(tmlfile, input, rundirsub+'//'+out+'.ftr', keys)

        else:
                print('No key set for ',name)

    else:
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')



def joinprocold(self,inp,out,task,rhs):

    if os.path.isfile(rundirsub+'//'+out+'.ftr'):
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')
    else:

        name=os.path.splitext(inp)[0]

        keys = config.get(name + '.keys',config.get('.keys'))
        if keys is None:
            raise Exception('No key set for ',name)

        offsets = config.get(task + '.offsets',config.get('.offsets'))
        if offsets is not None:
            rhs=expand_offset_date(rhs, offsets)
            print('RHS:',rhs)
        else:
            print('NO OFFSETS',name + '.offsets')

        for focus in rhs.split(';'):
            focusdir, focusbase=dir_base(focus)
            rhsfocus=focusbase.split('.')[0]
            if '.' in focusbase: ## fieldnames are listed, eg. source.field1,field2,...
                rhsfields=focusbase.split('.')[1]
                # Rename where necessary


            else:
                rhsfields=None

            if focus==rhs.split(';')[0]:
                #  First join goes from 'inp' to 'out'
                qsjoinplus(rundirsub+'//'+inp, keys, rundirsub+'//'+out+'.ftr',focusdir+'/'+rhsfocus,fields=rhsfields)
            else:
                # Subsequent joins go from 'out' to 'out'
                qsjoinplus(rundirsub+'//'+out, keys, rundirsub+'//'+out+'.ftr',focusdir+'/'+rhsfocus,fields=rhsfields)



def joinproc(self,inp,out,task,rhs):

    if os.path.isfile(rundirsub+'//'+out+'.ftr'):
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')
    else:

        name=os.path.splitext(inp)[0]

        keys = config.get(name + '.keys',config.get('.keys'))
        if keys is None:
            raise Exception('No key set for ',name)

        suffix = config.get(task+'.suffix') or ''
        _except = config.get(task+'.except')

        offsets = config.get(task + '.offsets',config.get('.offsets'))
        if offsets is not None:
            rhs, offsetlist=expand_offset_date(rhs, offsets)
            print('RHS:',rhs)
        else:
            print('NO OFFSETS',name + '.offsets')
        print('Length offsets and rhs',len(offsets),len(rhs))
        for i in range(len(rhs)):
            print('I:',i)
            focusdir, focusbase=dir_base(rhs[i])
            focusdir, focusbase=dir_base(rhs[i])
            rhsfocus=focusbase.split('.')[0]
            print('focus, offset',rhsfocus,offsetlist[i])
            if '.' in focusbase: ## fieldnames exist
                rhsfields=focusbase.split('.')[1]
            else:
                rhsfields=map

            newrhsfocus=rundirsub+'/'+rhsfocus
            if offsets is not None:
                suffix_offset=suffix+offsetlist[i]
                newrhsfocus,newrhsfields=renameproc(self,task,focusdir+'/'+rhsfocus,rundirsub+'/'+rhsfocus+'_renamed',map=rhsfields,suffix=suffix_offset,_except=keys)
#???? map of all fields, but really only what what matches in list


            if i==0:
                #  First join goes from 'inp' to 'out'
                qsjoinplus(rundirsub+'//'+inp, keys, rundirsub+'//'+out+'.ftr',newrhsfocus,fields=newrhsfields)
            else:
                # Subsequent joins go from 'out' to 'out'
                qsjoinplus(rundirsub+'//'+out, keys, rundirsub+'//'+out+'.ftr',newrhsfocus,fields=newrhsfields)


def dir_base(file):
    fullpath=os.path.abspath(file)
    dir=os.path.dirname(fullpath)
    base=os.path.basename(fullpath)
    return dir, base

def expand_offset_date(list, offsets):
    ## expand_offset_date('foo/hello19650306,bar\\bye196503', '-0,-2,-3')

    expandedlist=[]
    offsetlist=[]

    for focus in list.split(';'):

        focusdir, focusbase=dir_base(focus)

        if re.search('[0-9]{8}',focusbase): # 8 digits in filename
            n='[0-9]{8}'
            dateformat='%Y%m%d'
        elif re.search('[0-9]{6}',focusbase): # 6 digits in filename:
            n='[0-9]{6}'
            dateformat='%Y%m'

        ymd=re.sub('.*('+n+').*',r'\1',focus)
        dateymd=datetime.datetime.strptime(ymd,dateformat)
        for o in offsets.split(','):
            if n=='[0-9]{8}':
                 offsetymd=dateymd+relativedelta(days=int(o))
            else:
                 offsetymd=dateymd+relativedelta(months=int(o))

            #rr=re.compile('(.*)'+n+'[^/\\\]*$')
            #left=re.sub(rr,r'\1',focus)
            #try:
            #    right=re.sub('.*'+n+'([^/\\\]*$)',r'\3',focus)
            #except:
            #    right=''
            offsetfocus=re.sub(ymd,offsetymd.strftime(dateformat),focusbase)

            expandedlist.append(focusdir+'/'+offsetfocus)

            if int(o) >= 0:
                oflag=postflag+o
            elif int(o)==0:
                oflag=''
            else:
                oflag=preflag+str(-int(o))

            offsetlist.append(oflag)

    expandedstring=";".join(expandedlist )
    return expandedlist, offsetlist


def getsources(sourcelist):
    for source in sourcelist:
        #source=os.path.splitext(os.path.basename(source))[0]

        name=os.path.splitext(os.path.basename(source))[0]
        if '_' in name or '.' in name:
            raise Exception('UNDERSCORE OR PERIOD IN SOURCE NAME '+name+' : NOT ALLOwED!')
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
        print(name+'.ftr already exists...')


def getftr(ftr):
    name = os.path.splitext(os.path.basename(ftr))[0]

    if not os.path.isfile(rundirsub+'//'+name+'.ftr'):
        qscopy(ftr,rundirsub+'//'+name+'.ftr',force='true')
    else:
        print(name+'.ftr already exists...')



def gettxt(txt):
    pass


############### QSDBC's #######################################################

def runqsdb(command, args, failonbad=True):

    print('EXECUTING',qshome+command,[qshome+command]+args)
    result = os.spawnv(os.P_WAIT, qshome+command, [qshome+command]+args)
    if result==1 and failonbad:
            raise Exception( qshome+command,' failed for ',[command]+args)
    return result


def runqsdbOther(command, arglist, failonbad=True):
    print('args', arglist)

    command2=qshome+command

    for a in arglist:
        command2=command2+' '+a
    print('EXECUTING in os.system:',command2)
    #os.system(command2)
    result = os.spawnv(os.P_WAIT, qshome+command, command2)
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



def qsrenamefields(input, mapfile, output, force=None):

    args=[]
    args.extend(['-input',input,'-map','@'+mapfile])

    for arg in ['output']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['force']:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdb("qsrenamefields.exe", args)



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
        print('INPUT for sortfix:',input)
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


def getfocusfields(focus):
    _,name,_ = stem(focus)
    runqsdb('qsdescribe',['-input', focus, '-fields', '-output', rundirsub+'/'+name+'__fields.txt'])
    #fields={}

    reader = csv.reader(open(rundirsub+'/'+name+'__fields.txt', 'r'))
    fields=[]
    fielddict={}
    for row in reader:
        try:
            x=row[0].split()
            dummy=int(x[0])   ##  First element is a number
            #print(x)
            #fields[x[1]] = {'seq':x[0], 'type':x[2], 'interps':x[3]}

            fields.append(x[1])
            fielddict[x[1]] = {'type':x[2]}
        except:
                pass
    return fields, fielddict

#getfocusfields('C:\PortraitMiner7.0B\ext\demo\DirectBankUSA/junk201503')

def getfocusfieldsOld2(focus):
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



def getmatchingfields(focus,patterns,_except=None):
    print('ZZZ',focus,patterns,_except)
    fields, fielddict=getfocusfields(focus)
    matchingfields=[]

    for pattern in patterns.split(','):
        pattern=pattern.strip()
        fieldtype=None
        if ':' in pattern:
            fieldtype=pattern.split(':')[1]
            pattern=pattern.split(':')[0]

        notregex=len(re.sub("[a-zA-Z0-9_]","",pattern))==0

        pattern='^'+pattern+'$'
        latestmatchingfields=[]
        for f in fields:
            if re.match(pattern,f,re.IGNORECASE) and (fieldtype==None or re.match(fieldtype,fielddict[f]['type'],re.IGNORECASE)):
                latestmatchingfields.append(f)

        if latestmatchingfields==[]:
            print("WARNING: field pattern ",pattern,"not found in",focus)

        matchingfields.extend(latestmatchingfields)

    dedupematchingfields=[]
    _exceptionlist=_except.split(',')

    for i in matchingfields:
        if i not in dedupematchingfields and i not in _exceptionlist:
            dedupematchingfields.append(i)
    return dedupematchingfields


main()

#print(re.sub('create *([A-Za-z0-9_()\.\*]*).*','\\1',n))

#var=

