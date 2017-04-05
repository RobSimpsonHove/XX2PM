#####
#
# qs: der(ive), join

## TODO Spaces in path names
## DONE What should trim replace do?  A single patt,replace on fld file
## TODO auto rename RH key
## DONE combinatorial vars

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

logging.basicConfig(level=logging.DEBUG)  # DEBUG,INFO,WARNING,ERROR,CRITICAL

# Initialise
qsbin = preflag = postflag = 'dummy'
exec(open('local.py').read())  # qsbin, pre/post labels

parser = argparse.ArgumentParser(description='This is a Miner databuild template')
parser.add_argument('-d', '--dir', help='Run directory', required=False)
parser.add_argument('-p', '--param', help='Build parameters', required=True)
args = parser.parse_args()

# Set timestamps
now = time.strftime("%Y%m%d-%H%M%S")
today = time.strftime("%Y%m%d")

# Use fixed directory
#args.dir='20170224-104120'

# Print given arguments
logging.debug('ARGS.PARAM:' + args.param)

# Replace tokens in tempfile
f1 = open(args.param, 'r')
f2 = open('../temp' + '/template.properties', 'w', encoding='utf-8')
for line in f1:
    line = line.replace('$today', today)
    line = line.replace('$now', now)
    f2.write(line)
f1.close()
f2.close()

# Load template file
c = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
c.optionxform = str
with codecs.open('../temp' + '/template.properties', 'r', encoding='utf-8') as f:
    c.read_file(f)
config = dict(c.items('xx2pm'))
print('Config File:', config)


# Create run directories
rundirroot = config.get('runroot') or '../rundirs'
base = config.get('name') or ''

if args.dir or config.get('rundir'):
    rundir = args.dir or config.get('rundir')
    rundirsub = os.path.abspath(rundirroot + '/' + rundir)
else:
    rundirsub = os.path.abspath(rundirroot + '/' + base + now)
if not os.path.exists(rundirroot):
    os.makedirs(rundirroot)
if not os.path.exists(rundirsub):
    os.makedirs(rundirsub)



def runqsdbc(command, args, failonbad=True):
    logging.info('EXECUTING ' + qsbin + command)
    logging.info('with args ' + str(args))
    result = os.spawnv(os.P_WAIT, qsbin + command, [qsbin + command] + args)
    if result == 1 and failonbad:
        raise Exception(qsbin + command, ' failed for ', [command] + args)
    return result


def derproc(self, task):
    #   'der': {'proc': derproc, 'required': ['input', 'derivations'],  'optional': ['output']},
    ##

    # Input focus is local or absolute (includes a '/')
    inp = config.get(task + '.in')
    if '/' not in inp:
        inp = rundirsub + '/' + inp

    # Output is option. Defaults to task name
    out = config.get(task + '.out') or task
    if '/' not in out:
        out = rundirsub + '/' + out


    # fdl is written to a local file first
    fdlfile = make_copy_file_or_arg(config.get(task + '.fdl'), out + '_.fdl')

    # if var, expand fdl accordingly
    vars = config.get(task + '.var')
    if vars is not None:
            fdlfile = expand_dollarvar(self, inp, out, fdlfile, task, vars)

    # Report on any missing required parameters
    hasValue(task, 'in,out,fdl', inp, out, fdlfile)

    if os.path.isfile(rundirsub + '/' + out + '.ftr'):
        print('Not overwriting', rundirsub + '/' + out + '.ftr', 'already exists')
        return
    else:
        qsderive(derivations=fdlfile, input=inp, output=out + '.ftr')


def joinproc(self,task):
        print('PROC joinproc')
        inp = config.get(task + '.in')
        if '/' not in inp:
            inp = rundirsub + '/' + inp

        out = config.get(task + '.out') or task
        if '/' not in out:
            out = rundirsub + '/' + out

        rhs = config.get(task + '.join')
        print('!rhs',rhs)

        keys = config.get(task + '.keys') or config.get('.keys')
        if keys is None:
            raise Exception('No key set for ',task)

        suffix = config.get(task+'.suffix') or ''
        _except = config.get(task+'.except')

        offsets = config.get(task + '.offsets') or config.get('.offsets')

        # Report on any missing required parameters
        hasValue(task, 'in,join,keys', inp, rhs, keys)

        if os.path.isfile(out + '.ftr'):
            print('Not overwriting', out + '.ftr', 'already exists')
        else:
            rhslist, rhsff = globfocifields(rhs)

            if offsets is not None:
                rhslist, offsetlist=expand_offset_date(rhslist, offsets)
                print('RHS1:',rhslist)
                print('Length offsets and rhs',len(offsets),len(rhslist))
            else:
                rhslist, offsetlist=rhslist, [None]
                print('RHS2:',rhslist)
                print('Length rhs',len(rhslist))

            for i in range(len(rhslist)):
                print('ZZZ',rhslist[i])
                # focus or focus.field1,field2
                focusdir, rhsfocus, _, _ = stem(rhslist[i][0])
                if rhsff[i]!=[]:  ## fieldnames exist
                    rhsfields=rhsff[i][0]
                else:
                    rhsfields=None

                newrhsfocus=rhslist[i][0]

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



def qsjoinplus(input, keys, output, join, fields=None, xfields=None, force=None):
    print('PROC qsjoinplus', input, keys, output, join)
    presort=True
    if presort:
        input=qssortfix(input,keys)
        join=qssortfix(join,keys)

    print('input, keys, output, join, fields, xfields, force \n',input, keys, output, join, fields, xfields, force)
    qsjoin(input, keys, output, join, fields, xfields, force)


def qsjoin(input, keys, output, join, fields=None, xfields=None, force=None):

    args=[]
    args.extend(['-input',input,'-keys',keys,'-output',output, '-join', join])

    for arg in ['fields','xfields']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['force']:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdbc("qsjoin.exe", args)


def aggproc(self,task):
    inp = config.get(task + '.in')
    if '/' not in inp:
        inp = rundirsub + '/' + inp

    out = config.get(task + '.out') or task
    if '/' not in out:
        out = rundirsub + '/' + out

    keys = config.get(task + '.keys') or config.get('.keys')
    print('keys',keys)
    if keys is None:
        raise Exception('No key set for ', task)
    if re.search('\|',keys):
        key2=keys.split('|')[1]
        keys=keys.split('|')[0]

        print('keys2', keys, key2)
        inp = qssortfix(inp, key2)


    # fdl is written to a local file first
    tmlfile = make_copy_file_or_arg(config.get(task + '.tml'), out + '_.tml')

    library = config.get(task + '.library',config.get('.library'))

    if not os.path.isfile(out+'.ftr'):

        # if var, expand fdl accordingly
        vars = config.get(task + '.var')
        print('vars',vars)
        if vars is not None:
            tmlfile = expand_dollarvar2(self, inp, out, tmlfile, task, vars)

        input=qssortfix(inp,keys)
        qsmeasure(tmlfile, input, out+'.ftr', keys)

    else:
        print('Not overwriting', rundirsub+'//'+out+'.ftr','already exists')


def exportproc(self,task):
    print('PROC exportproc')

    inp = config.get(task + '.in')
    if '/' not in inp:
        inp = rundirsub + '/' + inp

    out = config.get(task + '.out') or task
    print('out1',out)
    if '/' not in out:
        out = rundirsub + '/' + out

        print('out2', out)

    fields = config.get(task + '.fields')
    xfields = config.get(task + '.xfields')
    records = config.get(task + '.records') or 'true'
    print('r:',rundirsub+'//'+out+'.ftr')
    if not os.path.isfile(out+'.ftr'):
        qsimportfocus(inp,out,fields,xfields,records)
    else:
        print('Not overwriting', out+'.ftr','already exists')

def qssortfix(input,keys):
        print('!! PROC sortfix  INPUT:',input, keys)
        _,_,name,_ = stem(input)
        sorted=rundirsub+'//'+name+'__s.ftr'
        if os.path.isfile(sorted):
            return sorted

        result=runqsdbc("qssort.exe", ['-check -input ' + input + ' -keys ' + keys], False)

        if result==1:
            runqsdbc("qssort.exe", ['-input ' + input + ' -keys ' + keys + ' -output ' + sorted])
            input=sorted

        return input


def hasValue(task, names, *args):
    n = 0
    valid = True
    for arg in args:
        n = n + 1
        if not arg:
            missing = names.split(',')[n - 1]
            logging.critical('TASK ' + task + ' IS MISSING ARG ' + missing)
            valid = False
    if not (valid):
        exit()


def expand_dollarvar(self, inp, out, fdl_tml_file, task, vars):
    print('!! expand_dollarvar ' + inp + ', ' + out + ', ' + fdl_tml_file + ', ' + task + ', ' + vars)

    expandedfdlfile = out + 'task' + '.fdl'
    fout = open(expandedfdlfile, 'w')
    fout.write('/////// User parameters\n')

    # Write non-recurring derivations
    with open(fdl_tml_file, 'rU') as fin:
        for line in fin:
            if not (re.search('\$', line)):
                print('#', line)
                fout.write(line)
    fout.write('\n')

    # loop through pattern sets
    for item in vars.split(';'):  # either pattern (name defaults to var), or name1:patt;name2:list,a,b,c ..
        if item[1]:
            varname,var = item.split(':')[0], item.split(':')[1]  # name1:regex or name1:list,a,b,c
        else:
            varname,var = 'var',item

        # Regex if contains anything not alphanumerics, underscores or commas, so find matching fields
        isregex = len(re.sub("[a-zA-Z0-9_,]", "", var)) > 0
        if isregex:
            patterns = getmatchingfields(inp, var)
        else:
            patterns = []
            for pattern in var.split(','):
                patterns.append(pattern)
        print('PATTERNS', patterns)

        #Write recurring derivations
        for pattern in patterns:
           # print('Pattern:@@@@@',trimpattern,trimreplace)
           if trim is not None:
               pattern = re.sub(trimpattern, trimreplace, pattern)
           with open(fdl_tml_file, 'rU') as fin:
               for line in fin:
                   if re.search('\$' + varname, line):
                       fout.write(re.sub('\$' + varname, pattern, line))
        fout.write('\n')

    fin.close()
    fout.close()

    with open(expandedfdlfile, 'rU') as fin:
        for line in fin:
            print('>>', line)

    return expandedfdlfile


def expand_dollarvar2(self, inp, out, fdl_tml_file, task, vars):

    # Write all derivations
    expandedfdlfile = out + '.fdl'
    fout = open(out+'_start.fdl', 'w')
    fout.write('/////// User parameters\n')
    with open(fdl_tml_file, 'rU') as fin:
        for line in fin:
            print('#', line)
            fout.write(line)
    fout.close()
    last='start'
    # loop through pattern sets
    for item in vars.split(';'):  # either pattern (name defaults to var), or name1:patt;name2:list,a,b,c ..
        if item[1]:
            varname,var = item.split(':')[0], item.split(':')[1]  # name1:regex or name1:list,a,b,c
        else:
            varname,var = 'var',item

        # Regex if contains anything not alphanumerics, underscores or commas, so find matching fields
        isregex = len(re.sub("[a-zA-Z0-9_,]", "", var)) > 0
        if isregex:
            patterns = getmatchingfields(inp, var)
        else:
            patterns = []
            for pattern in var.split(','):
                patterns.append(pattern)
        print('PATTERNS', patterns)

        #Write recurring derivations
        for pattern in patterns:

            fin = open(out + '_' + last + '.fdl', 'rU')
            next = varname + '-' + pattern
            fout = open(out + '_' + next + '.fdl', 'w')
            for line in fin:
                if line !='\n':
                    if '$' + varname in line:
                        fout.write(re.sub('\$' + varname, pattern, line)+'\n')
                    fout.write(line)
            fin.close()
            fout.close()
            last=next

    # Set trim patterns: trim is either 'pattern' or 'pattern,replace'
    trim = config.get(task + '.trim')
    if trim is not None:
        if ',' in trim:
            print('TUPLE')
            trimpattern,trimreplace = trim.split(',')[0], trim.split(',')[1]
        else:
            trimpattern,trimreplace = trim, ''
    else:
        trimpattern,trimreplace = 'dummy','dummy'

    final = out + '.fdl'
    fout = open(final, 'w')
    with open(out + '_' + last + '.fdl', 'rU') as fin:
        for line in fin:
            if '$' not in line:
                fout.write(re.sub(trimpattern, trimreplace, line))
    fout.close()
    fin.close()
    return final


def globfocifields(filepatterns):
    ## Glob[.field1[.field2]...][;Glob]...
    ## eg. test.join=Cust2017*;field1,field2|Address;Postcode
    print('fp',filepatterns)
    focilist = []
    fieldlist = []
    for filepattern in filepatterns.split('|'):
        print('Expanding item ' + filepattern)

        if ';' in filepattern:
            focuspattern=filepattern.split(';')[0]
            fieldlist.append([filepattern.split(';')[1]])
        else:
            focuspattern = filepattern
            fieldlist.append([])
        ## Search locally in rundir if no path given

        focusdir, focusbase, _, _ = stem(focuspattern)
        if focusbase == focuspattern:
            focuspattern = rundirsub + '/' + focuspattern

        print('mm', focusbase, focuspattern)
        # Turn regex into glob pattern
        focuspattern = focuspattern.replace('.*', '*')
        print('looking for pattern:' + focuspattern, glob.glob(focuspattern+'.ftr'),focilist)
        print('f1',focilist)
        focilist.append(glob.glob(focuspattern+'.ftr'))
        print('f2',focilist)

    print('Found items: ', focilist, fieldlist)
    return focilist, fieldlist


def stem(filename):                     # ../bdir/file.txt
    fullpath = os.path.abspath(filename)# C:/mydir/bdir/file.txt
    dir = os.path.dirname(fullpath)             # C:/mydir/bdir/
    base = os.path.basename(fullpath)   # file.txt
    name = os.path.splitext(base)[0]            # file
    suffix = os.path.splitext(base)[1]          # txt
    return dir, base, name, suffix


def qsderive(derivations, input, output=None):
    args = []

    # Add required arguments: eg. -input focusname.ftr
    args.extend(['-derivations', derivations, '-input', input])

    # Add optional arguments: eg. -input focusname.ftr
    for arg in ['output']:
        if eval(arg):
            args.extend(['-' + arg, eval(arg)])

    # Add booleans: eg. -verbose
    for arg in []:
        if eval(arg):
            args.extend(['-' + arg])

    runqsdbc("qsderive.exe", args)



def qsmeasure(aggregations, input, output, keys, fields=None, xfields=None, force=None, library=None):

    args=[]
    args.extend(['-aggregations',aggregations,'-input',input,'-output',output,'-keys',keys])

    for arg in ['fields','xfields','library']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['force']:
        if eval(arg):
            args.extend(['-'+arg])

    runqsdbc("qsmeasure.exe", args)


def qsimportfocus(input, output, fields=None, xfields=None, records=None):

    args=[]
    args.extend(['-input',input,'-output',output])

    for arg in ['fields','xfields','records']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    runqsdbc("qsimportfocus.exe", args)


def getfocusfields(focus):
    _, _, name, _ = stem(focus)
    runqsdbc('qsdescribe', ['-input', focus, '-fields', '-output', rundirsub + '/' + name + '__fields.txt'])
    # fields={}

    reader = csv.reader(open(rundirsub + '/' + name + '__fields.txt', 'r'))
    fields = []
    fielddict = {}
    for row in reader:
        try:
            x = row[0].split()
            dummy = int(x[0])  ##  First element is a number
            # print(x)
            # fields[x[1]] = {'seq':x[0], 'type':x[2], 'interps':x[3]}

            fields.append(x[1])
            fielddict[x[1]] = {'type': x[2]}
        except:
            pass
    return fields, fielddict


def getmatchingfields(focus, patterns, _except=''):
    print('getmatchingfields', focus, patterns, _except)
    fields, fielddict = getfocusfields(focus)
    matchingfields = []

    for pattern in patterns.split(','):
        pattern = pattern.strip()
        fieldtype = None
        if ':' in pattern:
            fieldtype = pattern.split(':')[1]
            pattern = pattern.split(':')[0]

        pattern = '^' + pattern + '$'
        latestmatchingfields = []
        for f in fields:
            # print('ff',pattern,f)
            if re.match(pattern, f, re.IGNORECASE) and (
                            fieldtype == None or re.match(fieldtype, fielddict[f]['type'], re.IGNORECASE)):
                latestmatchingfields.append(f)

        if latestmatchingfields == []:
            print("WARNING: field pattern ", pattern, "not found in", focus)

        matchingfields.extend(latestmatchingfields)

    dedupematchingfields = []
    _exceptionlist = _except.split(',')

    for i in matchingfields:
        if i not in dedupematchingfields and i not in _exceptionlist:
            dedupematchingfields.append(i)
    return dedupematchingfields


def make_copy_file_or_arg(filearg, outname):
    print('filearg',filearg)
    if not os.path.isfile(filearg):
        # Write property to a file
        f = open(outname, 'w')
        f.write(filearg)
        f.close()
    else:
        # Copy a file
        copyfile(filearg, outname)
    ##TODO insert space after :=
    return outname


def executetasklist(self):
    for task in self.tasklist:
        logging.info('DOING ' + task)

        operation = config.get(task + '.op')
        if not operation:
            # If operation not specified, can we infer: does it start with one of the op names?
            for op in self.operations:
                if re.match(op, task):
                    operation = op
                    break

        if not operation:
            logging.critical('No operation specified or inferred for task ' + task + '!!')
        else:
            self.ops[operation]['proc'](self, task)
            #executetask()


def metaIproc(self,inp,meta):
    _,name,_=stem(inp)
    qsimportmetadata(rundirsub+'//'+inp, rundirsub+'//'+name+'_'+meta+'.qsfm')


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


class main:
    def __init__(self):
        self.operations = ['der', 'join','export','agg']
        self.ops = {
            'der': {'proc': derproc, 'required': ['input', 'derivations'],
                    'optional': ['output'], 'boolean': []},
            'join': {'proc': joinproc, 'required': ['input', 'aggregations'],
                     'optional': ['output']},
            'export': {'proc': exportproc, 'required': ['input'],
                       'optional': ['output','fields','xfields','records']},
            'agg': {'proc': aggproc, 'required': ['aggregations', 'input', 'output', 'keys'],'optional': []},
            meta:

        }

        self.tasklist = config.get('tasklist').split(',')
        executetasklist(self)


main()
print('DONE')
