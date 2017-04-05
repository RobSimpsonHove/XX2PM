import os


def runqsdb(path, command, args):
    print('EXEC',path+command,[command]+args)
    result = os.spawnv(os.P_WAIT, path+command, [command]+args)
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
    print('args',args)
    runqsdb('qsimportdb.exe', args)


def qsdbinsert(input,udc,table,fields=None,xfields=None):

    args=[]
    args.extend(['-input',input,'-udc',udc,'-table',table])

    for arg in ['fields','xfields']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    print('args',args)
    runqsdb('qsdbinsert.exe', args)


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


def qsjoin(input, keys, output, join, fields=None, xfields=None, force=None):

    presort=True
    if presort==True:
        result=runqsdb("qssort.exe", ['-check -input '+input+' -keys'+keys])
        print('RESULT:',result)
        if result!=0:
            result=runqsdb("qssort.exe", ['-input '+input+' -keys'+keys])



    args=[]
    args.extend(['-input',input,'-keys',keys,'-output',output, '-join', join])

    for arg in ['fields','xfields']:
        if eval(arg):
            args.extend(['-'+arg,eval(arg)])

    for arg in ['force']:
        if eval(arg):
            args.extend(['-'+arg])



    runqsdb("qsjoin.exe", args)


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



