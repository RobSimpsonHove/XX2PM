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
## TODO var for fieldnames AND list of patterns

## DOCUMENT
## sources= ....  file or dir, expands dir for *.sql
## taskslist: der, agg, join, meta, copy
## meta task: file or dir, expands dir for *.qsfm

## $today YYYYMMDD, $now YYYYMMDD-HHMMSS

import os
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
import logging
logging.basicConfig(level=logging.DEBUG)  #DEBUG,INFO,WARNING,ERROR,CRITICAL

exec(open('local.py').read())

parser = argparse.ArgumentParser(description='This is a Miner databuild template')
parser.add_argument('-d','--dir', help='Run directory',required=False)
parser.add_argument('-p','--param',help='Build parameters', required=True)
args = parser.parse_args()
#args.dir='20160421-161226'

## Set timestamps
now = time.strftime("%Y%m%d-%H%M%S")
today = time.strftime("%Y%m%d")

# Create run directories
rundirroot = '../rundirs'
if args.dir:
    rundirsub=os.path.abspath(rundirroot + '/' + args.dir)
else:
    rundirsub=os.path.abspath(rundirroot + '/' + now)
if not os.path.exists(rundirroot):
    os.makedirs(rundirroot)
if not os.path.exists(rundirsub):
    os.makedirs(rundirsub)


## Print given arguments
logging.debug('ARGS.PARAM:' +args.param)

## Replace tokens in tempfile
f1 = open(args.param, 'r')
f2 = open(rundirsub+'/template.properties', 'w', encoding='utf-8')
for line in f1:
    line=line.replace('$today', today)
    line=line.replace('$now', now)
    f2.write(line)
f1.close()
f2.close()

# Load template file
c=configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
c.optionxform=str
with codecs.open(rundirsub+'/template.properties', 'r', encoding='utf-8') as f:
    c.read_file(f)
config=dict(c.items('xx2pm'))
#print('Config File:',config)


def globfoci(filepatterns):
    itemlist = []
    for filepattern in filepatterns.split(','):
        logging.debug('Expanding item ' + filepattern)
        base, _, _ = stem(filepattern)

        ## Search locally in rundir if no path given
        if base == filepattern:
            filepattern = rundirsub + '/' + filepattern

        list = []
        # Turn regex into glob pattern
        filepattern = filepattern.replace('.*', '*')
        filepattern = filepattern.replace('.ftr', '')
        filepattern = filepattern+'.ftr'
        logging.debug('looking for pattern:' + filepattern)
        list.extend(glob.glob(filepattern))
        itemlist.extend(list)
    logging.debug('Found items: ' + str(itemlist))

    #namelist = []
    #for item in itemlist:
    #    _, name, _ = stem(item)
#
    #   namelist.append(name)

    return itemlist


def get_search_items(filesOrDirs, extensions):
    ## extensions = (".qsfm","*.jpg","*.jpeg",)

    itemlist = []
    for item in filesOrDirs.split(','):
        logging.debug('Search item '+item)
        base,_,_=stem(item)

        ## Search locally in rundir if no path given
        if base==item:
            item=rundirsub+'/'+item

        logging.debug('Checking dir '+item)

        list = []
        _,name,suffix=stem(item)
        print('suff',suffix)
        for extension in extensions:
            if suffix==extension:
                search=item
            else:
                search=item+extension

            # Turn regex into glob pattern
            search=search.replace('.*','*')
            logging.debug('looking for pattern:'+str(search))
            list.extend(glob.glob(search))
        itemlist.extend(list)
    logging.debug('Found items: '+str(itemlist))

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


def jointest():
    print('jointest')


def dertest(self,task):
    print('AMAZING dertest',task)
    print(self.ops['der']['required'])


def executetasklist(self):

    for task in self.tasklist:
        logging.info('DOING '+task)

        operation = config.get(task + '.op')
        if not operation:
            # If operation not specified, can we infer: does it start with one of the op names?
            for op in self.operations:
                if re.match(op, task):
                    operation=op
                    break

        if operation:
            self.ops[operation]['proc'](self, task)
        else:
            logging.critical('No operation specified or inferred for task '+task+'!!')




#            print a
#            fields = {'name':clean_name,'email':clean_email}

#        for key in fields:
#        fields[key]()
#            if value is None:
#                pass
#            else:
#                print('DOING ',task + '.' + name,'=', value)
#                inp=prevname
#                _,out,_=stem(prevname)
#                out=name+'_'+task



#                print('DOING task.name=value',task + '.' + name,'=', value, 'inp:',inp, 'out:',out)

#                if re.search('^der',task):
#                    derproc(self,inp,out,task,fdl=value)
#                elif re.search('^agg',task):
#                    aggproc(self,task,inp,out,tml=value)
#                elif re.search('^join',task):
#                    joinproc(self,inp,out,task,rhs=value)
#                elif re.search('^metan',task):
#                    metanproc(self,inp,out,meta=value)
#                elif re.search('^metax',task):
#                    metaxproc(self,inp,meta=value)
#                elif re.search('^meta',task):
#                    metaproc(self,inp,out,meta=value)
#                elif re.search('^copy',task):
#                    copyproc(self,inp,to=value)
#                elif re.search('^rename',task):
#                    renameproc(self,task,inp,out,map=value)
#                #elif re.search('^cmd',step):
#                #    cmdproc(self,inp,out,meta=value)
#                elif re.search('^fin',task):
#                    finproc(self,task)
#                else:
#                    raise Exception('Task',task,'not a supported task')


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







def HasValue(task, names, *args):
    n=0
    valid=True
    for arg in args:
        n=n+1
        if not(arg):
            missing=names.split(',')[n-1]
            logging.critical('TASK '+task+' IS MISSING ARG '+missing)
            valid=False
    if not(valid):
        exit()


def derproc(self,task):
#   'der': {'proc': derproc, 'required': ['input', 'derivations'],  'optional': ['output']},

    inp=config.get(task+'.in')
    if '/' not in inp:
        inp=rundirsub+'/'+inp

    out=config.get(task+'.out') or task
    if '/' not in out:
        out = rundirsub + '/' + out

    fdlfile = make_copy_file_or_arg(config.get(task + '.fdl'), out + '_.fdl')

    var = config.get(task + '.var')
    if var is not None:
        fdlfile = expand_dollarvar(self, inp, out, fdlfile, task, var)

    HasValue(task, 'in,out,fdl', inp, out, fdlfile)

    if os.path.isfile(rundirsub+'/'+out+'.ftr'):
        print('Not overwriting', rundirsub+'/'+out+'.ftr','already exists')
        return
    else:
        qsderive(derivations=fdlfile, input=inp, output=out+'.ftr')


def joinproc(self,task):
        print('PROC joinproc')
        inp = config.get(task + '.in')
        if '/' not in inp:
            inp = rundirsub + '/' + inp

        out = config.get(task + '.out') or task
        if '/' not in out:
            out = rundirsub + '/' + out

        rhs = config.get(task + '.join')
        rhs = globfoci(rhs)

        keys = config.get(task + '.keys') or config.get('.keys')
        if keys is None:
            raise Exception('No key set for ',task)

        suffix = config.get(task+'.suffix') or ''
        _except = config.get(task+'.except')


        offsets = config.get(task + '.offsets') or config.get('.offsets')
        if offsets is not None:
            rhs, offsetlist=expand_offset_date(rhs, offsets)
            print('RHS:',rhs)
            print('Length offsets and rhs',len(offsets),len(rhs))
        else:
            rhs, offsetlist=rhs, [offsets]
            print('RHS:',rhs)
            print('Length rhs',len(rhs))

        if os.path.isfile( out + '.ftr'):
            print('Not overwriting', out + '.ftr', 'already exists')
        else:

            for i in range(len(rhs)):
                focusdir, focusbase = dir_base(rhs[i])
                rhsfocus = focusbase.split('.')[0]
                print('i, focusdir, focusbase', i, focusdir, focusbase)
                print('focus, offset',rhsfocus,offsetlist[i])
                if '.' in re.sub('.ftr','',focusbase):  ## fieldnames exist
                    rhsfields=re.sub('.ftr','',focusbase).split('.')[1]
                else:
                    rhsfields=map

                newrhsfocus=rhs[i]
                if offsets is not None:
                    suffix_offset=suffix+offsetlist[i]

                    newrhsfocus,newrhsfields=renameproc(self,task,focusdir+'/'+rhsfocus,rundirsub+'/'+rhsfocus+'_renamed',map=rhsfields,suffix=suffix_offset,_except=keys)
                        #???? map of all fields, but really only what what matches in list
                else:
                    newrhsfields=rhsfields

                print("newrhsfields",newrhsfields)
                if i==0:
                    #  First join goes from 'inp' to 'out'

                    qsjoinplus(inp+'.ftr', keys, out+'.ftr',newrhsfocus,fields=newrhsfields)
                else:
                    # Subsequent joins go from 'out' to 'out'

                    print('yyyy',  inp, keys, out + '.ftr', 'qqq', newrhsfocus, 'www', newrhsfields)
                    qsjoinplus(out+'.ftr', keys, out+'.ftr',newrhsfocus,fields=newrhsfields)




def joinproc2(self,task):


    inp = config.get(task + '.in')
    if '/' not in inp:
        inp = rundirsub + '/' + inp

    out = config.get(task + '.out') or task
    if '/' not in out:
        out = rundirsub + '/' + out

    join =  config.get(task + '.join')

    keys = config.get(task + '.keys') or config.get('.keys')

    suffix = config.get(task+'.suffix') or ''

    _except = config.get(task + '.except')

    rhs,_=get_search_items(join,['.ftr'])

    offsets = config.get(task + '.offsets', config.get('.offsets'))
    if offsets is not None:
        rhs, offsetlist = expand_offset_date(rhs, offsets)
        print('RHS:', rhs)
        print('Length offsets and rhs', len(offsets), len(rhs))
    else:
        rhs, offsetlist = [rhs], [offsets]

    print(join)
    ##for i in range(len(join)):
    ##    print('index:',i)
    ##    focusdir, focusbase=dir_base(join[i])
    ##    rhsfocus=focusbase.split('.')[0]
    ##    if '.' in focusbase: ## fieldnames exist
    ##        rhsfields=focusbase.split('.')[1]
    ##    else:
    ##        rhsfields=None
##
    ##    newrhsfocus=rundirsub+'/'+task+'_'+str(i)
    ##    if offsets is not None:
    ##        suffix_offset=suffix+offsetlist[i]
    ##        newrhsfocus,newrhsfields=renameproc(self,task,focusdir+'/'+rhsfocus,rundirsub+'/'+rhsfocus+'_renamed',map=rhsfields,suffix=suffix_offset,_except=keys)
    ##    else:
    ##        newrhsfields=rhsfields


    ####    if i==0:
    ####        #  First join goes from 'inp' to 'out'
    ####        qsjoinplus(rundirsub+'//'+inp, keys, rundirsub+'//'+out+'.ftr',newrhsfocus,fields=newrhsfields)
    ####    else:
    ####        # Subsequent joins go from 'out' to 'out'
    ####        qsjoinplus(rundirsub+'//'+out, keys, rundirsub+'//'+out+'.ftr',newrhsfocus,fields=newrhsfields)






def renameproc(self,task,inp,out,map,suffix=None,prefix=None,_except=None):
    print('PROC renameproc',self,task,inp,out,map)
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

def make_copy_file_or_arg(filearg,outname):

    if not os.path.isfile(filearg):
        # Write property to a file
        f = open(outname, 'w')
        f.write(filearg)
        f.close()
    else:
        #Copy a file
        copyfile(filearg, outname)
    return outname


def expand_dollarvar(self, inp, out, fdl_tml_file, task, var):

            logging.debug('expand_dollarvar '+inp+', '+out+', '+fdl_tml_file+', '+task+', '+var)
            trim = config.get(task+'.trim')
            if trim is not None:
                #if type(trim) is tuple:
                if ',' in trim:
                    print('TUPLE')
                    trimpattern=trim.split(',')[0]
                    trimreplace=trim.split(',')[1]
                else:
                    trimpattern=trim
                    trimreplace=''
            else:
                trimpattern='dummy'
                trimreplace='dummy'

            logging.debug('var: '+trimpattern+' '+trimreplace)

            # Regex if contains anything not alphanumerics, underscores or commas
            isregex = len(re.sub("[a-zA-Z0-9_,]","",var)) > 0
            if isregex:
                patterns=getmatchingfields(inp,var)
            else:
                patterns=[]
                for pattern in var.split(','):
                    patterns.append(pattern)
            print('PATTERNS',patterns)

            fdlfile_2=out+'_2.fdl'
            fout = open(fdlfile_2,'w')

            #Write non-recurring derivations

            fout.write('/////// User parameters\n')
            fout.write('// var:'+str(var))
            fout.write('// trim:'+str(trim))
            fout.write('\n')

            #Write non-recurring derivations
            with open(fdl_tml_file, 'rU') as fin:
                for line in fin:
                    if not(re.search('\$var',line)):
                        print('#',line)
                        fout.write(line)
            fout.write('\n')

            # Write recurring derivations
            for pattern in patterns:
                #print('Pattern:@@@@@',trimpattern,trimreplace)
                if trim is not None:
                    pattern=re.sub(trimpattern,trimreplace,pattern)

                with open(fdl_tml_file, 'rU') as fin:
                    for line in fin:
                        if re.search('\$var',line):
                            #print('##var##','\$var',line,re.sub('\$var',pattern,line))
                            fout.write(re.sub('\$var',pattern,line))
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


def getfoci(sourcelist):
    for source in sourcelist:
        # source=os.path.splitext(os.path.basename(source))[0]

        name = os.path.splitext(os.path.basename(source))[0]
        if '_' in name or '.' in name:
            raise Exception('UNDERSCORE OR PERIOD IN SOURCE NAME ' + name + ' : NOT ALLOwED!')
        suffix = os.path.splitext(os.path.basename(source))[1]

        print('Fetching ', name, suffix, source)

        if suffix == '.sql':
            getsql(source)
        elif suffix == '.ftr':
            getftr(source)
        elif suffix == '.txt':
            gettxt(source)
        else:
            raise Exception('Suffix ' + suffix + ' for item ' + name + ' not known!!!')


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

    logging.info('EXECUTING '+qshome+command)
    logging.info('with args '+str(args))
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
        print('PROC sortfix  INPUT for sortfix:',input)
        sorted=os.path.splitext(input)[0]+'__s.ftr'
        if os.path.isfile(rundirsub+'//'+sorted):
            return sorted

        result=runqsdb("qssort.exe", ['-check -input '+input+' -keys '+keys],False)

        if result==1:
            runqsdb("qssort.exe", ['-input '+input+' -keys '+keys+' -output '+sorted])
            input=sorted

        return input


def qsjoinplus(input, keys, output, join, fields=None, xfields=None, force=None):
    print('PROC qsjoinplus', input, keys, output, join)
    presort=True
    if presort:
        input=qssortfix(input,keys)
        join=qssortfix(join,keys)

    print('input, keys, output, join, fields, xfields, force \n',input, keys, output, join, fields, xfields, force)
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



def getmatchingfields(focus,patterns,_except=''):
    print('getmatchingfields',focus,patterns,_except)
    fields, fielddict=getfocusfields(focus)
    matchingfields=[]

    for pattern in patterns.split(','):
        pattern=pattern.strip()
        fieldtype=None
        if ':' in pattern:
            fieldtype=pattern.split(':')[1]
            pattern=pattern.split(':')[0]



        pattern='^'+pattern+'$'
        latestmatchingfields=[]
        for f in fields:
            #print('ff',pattern,f)
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


class main:

    def __init__(self):

        self.operations = ['der','join']
        self.ops = {
            'der': {'proc': derproc, 'required': ['input', 'derivations'],  'optional': ['output']},
            'join': {'proc': joinproc, 'required': ['input', 'aggregations'], 'optional': ['output']}
        }

        self.tasklist = config.get('tasklist').split(',')
        executetasklist(self)

main()
