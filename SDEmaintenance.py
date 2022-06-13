"""
Script to maintain enterpreise GDB (SDE) performance

Reconcile GDB
Compress GDB
Rebuild indexes (GDB Admin and Data Owner connections)
Analyze datasets (GDB Admin and Data Owner connections)
Sends log email to desired recipients
"""

import sys
import logging
import traceback
import arcgis
import arcpy
import time
import datetime
import os
import json
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


class Service:
    def __init__(self, folder, servicename, servicetype, servicenode=None, beginstate=None, stopsucess=None,
                 startsuccess=None):
        self.folder = folder
        self.servicename = servicename
        self.servicetype = servicetype
        self.servicenode = servicenode
        self.beginstate = beginstate
        self.stopsuccess = stopsucess
        self.startsuccess = startsuccess

    def setStartStop(self):
        if self.servicenode:
            if self.beginstate == 'STARTED':
                return False, False
            else:
                return True, True
        else:
            return True, True


class SdeConnection:
    def __init__(self, privilege, connectionpath, datasets=None):
        self.privilege = privilege
        self.connectionpath = connectionpath
        self.gdbuser, self.geodatabase, self.server, self.wildcard = self.parseConnection()
        self.datasets = datasets

    def parseConnection(self):
        try:
            conncomponents = os.path.split(self.connectionpath)[1].split('@')
            if len(conncomponents) != 3:
                raise ValueError('UNABLE TO PARSE [{}] DATABASE CONNECTION. VERIFY INPUTS.'.format(self.privilege))
            else:
                user = os.path.split(self.connectionpath)[1].split('@')[0].upper()
                gdb = os.path.split(self.connectionpath)[1].split('@')[1].upper()
                server = os.path.splitext(os.path.split(self.connectionpath)[1].split('@')[2])[0].upper()
                wildcard = '*.{}.*'.format(user)
                return user, gdb, server, wildcard
        except ValueError as e:
            logError(e)
            logInfo('----------------------')
            sys.exit()

    def setAsWorkspace(self):
        try:
            logInfo('Setting {} workspace..'.format(self.gdbuser))
            arcpy.env.workspace = self.connectionpath
        except Exception:
            logError('SETTING WORKSPACE FAILED')

    def listDatasets(self):
        try:
            logInfo('----------------------')
            logInfo('Listing datasets, {} connection..'.format(self.privilege))
            self.setAsWorkspace()
            wldcrd = self.wildcard
            logInfo('Workspace: {}'.format(os.path.split(arcpy.env.workspace)[1]))
            logInfo('Wildcard: {}'.format(wldcrd))
            datalist = arcpy.ListTables(wild_card=wldcrd) \
                       + arcpy.ListFeatureClasses(wild_card=wldcrd) \
                       + arcpy.ListRasters(wild_card=wldcrd)
            for featdataset in arcpy.ListDatasets("", "Feature"):
                fcs = arcpy.ListFeatureClasses(wild_card=wldcrd, feature_dataset=featdataset)
                fcs = [os.path.join(featdataset, fc) for fc in fcs]
                datalist += fcs
            logInfo('{} datasets found'.format(len(datalist)))
            if len(datalist) > 0:
                logInfo('Datasets: {}'.format(datalist))
            return datalist
        except Exception:
            logError('LISTING DATASETS FAILED')
            return None


class LogRecipient:
    def __init__(self, email, level):
        self.email = email
        self.level = level


# ------------------------------------------------------------------------------
def parseConfig(configfile):
    with open(configfile, 'r') as f:
        config = json.load(f)
    f.close()

    interruptservices = config['interruptservices']
    portalurl = config['portalurl']
    portaladmin = config['portaladmin']
    portaladminpw = config['portaladminpw']
    serverurl = config['serverurl']

    services = []
    for s in config['services']:
        services.append(Service(folder=s['folder'], servicename=s['servicename'], servicetype=s['servicetype']))

    gdbadminpath = config['gdbadminpath']
    dataownerpath = config['dataownerpath']
    reconcilemode = config['reconcilemode']
    targetversion = config['targetversion']
    abortconflicts = config['abortconflicts']
    postversions = config['postversions']
    deleteversions = config['deleteversions']

    logdirectory = config['logdirectory']
    loglevel = config['loglevel']
    emaillog = config['emaillog']
    senderemail = config['senderemail'],
    senderpassword = config['senderpassword'],
    logrecipients = []
    for r in config['logrecipients']:
        logrecipients.append(LogRecipient(email=r['email'], level=r['level']))

    return \
        interruptservices, portalurl, portaladmin, portaladminpw, serverurl, services, gdbadminpath, dataownerpath, \
        reconcilemode, targetversion, abortconflicts, postversions, deleteversions, \
        logdirectory, loglevel, emaillog, senderemail, senderpassword, logrecipients


# ------------------------------------------------------------------------------
def configureLogging(logdirectory, loglevel, now):
    logname = 'SDEmaintenance_' + now.strftime('%Y%m%d%H%M%S') + '.txt'
    logfile = os.path.join(logdirectory, logname)
    leveldict = {'WARNING': logging.WARNING, 'INFO': logging.INFO, 'DEBUG': logging.DEBUG}
    loglevel = leveldict[loglevel]
    logging.basicConfig(filename=logfile, filemode='w',
                        format='%(asctime)s - %(levelname)s - %(message)s', level=loglevel)

    logger = logging.getLogger()

    return logfile, logger


# ------------------------------------------------------------------------------
def quitLogging(logger):
    handlers = logger.handlers
    for h in handlers:
        h.close()
        logger.removeHandler(h)
    logging.shutdown()


# ------------------------------------------------------------------------------
def logInfo(infomsg):
    logging.info(infomsg)
    arcpy.AddMessage(infomsg)


# ------------------------------------------------------------------------------
def logWarning(warningmsg):
    logging.warning(warningmsg)
    arcpy.AddMessage(warningmsg)


# ------------------------------------------------------------------------------
def logError(errormsg):
    logging.error(errormsg, exc_info=True)
    arcpy.AddError(errormsg)
    arcpy.AddError(traceback.format_exc())


# ------------------------------------------------------------------------------
def checkConnections(adminconn, ownerconn):
    try:
        if adminconn.geodatabase == ownerconn.geodatabase and adminconn.server == ownerconn.server:
            gdb = '@'.join([adminconn.geodatabase, adminconn.server])
            logInfo('GEODATABASE: {}'.format(gdb))
            return True, gdb
        else:
            raise ValueError('GDBADMIN AND DATAOWNER DATABASE CONNECTIONS DO NOT MATCH. VERIFY INPUTS.')
    except ValueError as e:
        logError(e)
        return False, None


# ------------------------------------------------------------------------------
def connectGIS(portalurl, portaladmin, portaladminpw):
    logInfo('----------------------')
    logInfo('Connecting to GIS..')
    gis = None
    try:
        gis = arcgis.gis.GIS(url=portalurl, username=portaladmin, password=portaladminpw)
        logInfo(gis)
    except Exception:
        logError('Unable to connect to ArcGIS Portal')

    return gis


# ------------------------------------------------------------------------------
def validateServers(gis):
    validservers = gis.admin.servers.validate()
    logInfo('Valid servers: {}'.format(validservers))
    if not validservers:
        logError('Servers not functioning normally!')

    return validservers


# ------------------------------------------------------------------------------
def findTargetServer(gis, serverurl):
    logInfo('----------------------')
    logInfo('Finding target server..')
    targetserver = None
    gis_servers = gis.admin.servers.list()
    for srvr in gis_servers:
        if srvr.url == serverurl:
            targetserver = srvr
            logInfo('Target server located: {}'.format(srvr))
            break
    if not targetserver:
        logError('Target server {} not located!'.format(serverurl))

    return targetserver


# ------------------------------------------------------------------------------
def findServiceNodes(targetserver, services):
    logInfo('----------------------')
    logInfo('Finding service nodes..')
    for srvc in services:
        serverservices = targetserver.services.list(folder=srvc.folder)
        for s in serverservices:
            if s.properties.serviceName == srvc.servicename and s.properties.type == srvc.servicetype:
                srvc.servicenode = s
                srvc.beginstate = s.status['realTimeState']
                logInfo('Service node located: {}, {}'.format(srvc.servicenode, srvc.beginstate))
                break
        srvc.stopsuccess, srvc.startsuccess = srvc.setStartStop()
        if not srvc.servicenode:
            logWarning('Service node {}/{}.{} not located'.format(srvc.folder, srvc.servicename, srvc.servicetype))


# ------------------------------------------------------------------------------
def stopServices(services):
    logInfo('----------------------')
    logInfo('Stopping services..')
    failstop = False
    for srvc in services:
        if srvc.servicenode and srvc.beginstate == 'STARTED':
            success = srvc.servicenode.stop()
            if success:
                logInfo('{}, {}'.format(srvc.servicenode, srvc.servicenode.status['realTimeState']))
                srvc.stopsuccess = True
            else:
                failstop = True
                logWarning('Unable to stop {}'.format(srvc.servicenode))
    if failstop:
        logWarning('All services may not have been stopped!')


# ------------------------------------------------------------------------------
def startServices(services):
    logInfo('----------------------')
    logInfo('Starting services..')
    failstart = False
    for srvc in services:
        if srvc.servicenode and srvc.beginstate == 'STARTED':
            success = srvc.servicenode.start()
            if success:
                logInfo('{}, {}'.format(srvc.servicenode, srvc.servicenode.status['realTimeState']))
                srvc.startsuccess = True
            else:
                failstart = True
                logWarning('Unable to start {}'.format(srvc.servicenode))
    if failstart:
        logWarning('All services may not have been started!')


# ------------------------------------------------------------------------------
def reconcileVersions(sdeconn, reconcilemode, targetversion, abortconflicts, postversions, deleteversions):
    try:
        logInfo('----------------------')

        if reconcilemode != 'ALL_VERSIONS':
            logWarning('Override BLOCKING_VERSIONS. Not supported in this tool. Setting as ALL_VERSIONS')

        # Set postversions='NO_POST' and deleteversions='KEEP_VERSION' if reconcilemode == 'BLOCKING_VERSIONS'
        # Acquire locks if versions will be posted
        # Set deleteversions='KEEP_VERSION' if postversions='NO_POST'
        if reconcilemode == 'BLOCKING_VERSIONS':
            if postversions == 'POST':
                logWarning('Override POST. Must reconcile ALL_VERSIONS to post. Setting as NO_POST')
                postversions = 'NO_POST'
            if deleteversions == 'DELETE_VERSION':
                logWarning('Override DELETE_VERSION. Must reconcile ALL_VERSIONS to delete versions. Setting as KEEP_VERSION.')
                deleteversions = 'KEEP_VERSION'
        if postversions == 'POST':
            lock = 'LOCK_ACQUIRED'
        else:  # postversions == 'NO_POST'
            lock = 'NO_LOCK_ACQUIRED'
            if deleteversions == 'DELETE_VERSION':
                logWarning('Override DELETE_VERSION. Must POST to delete versions. Setting as KEEP_VERSION.')
                deleteversions = 'KEEP_VERSION'

        editversions = []
        versionlist = arcpy.da.ListVersions(sdeconn.connectionpath)
        for v in versionlist:
            if v.name == targetversion:
                for child in v.children:
                    editversions.append(child.name)

        logInfo('Reconciling versions, {} connection..'.format(sdeconn.privilege))
        logInfo('Input database: {}'.format(os.path.split(sdeconn.connectionpath)[1]))
        logInfo('Reconcile mode: {}'.format(reconcilemode))
        logInfo('Target version: {}'.format(targetversion))
        logInfo('Edit versions: {}'.format(editversions))
        logInfo('Abort if conflicts: {}'.format(abortconflicts))
        logInfo('Post versions: {}'.format(postversions))
        logInfo('Delete versions: {}'.format(deleteversions))
        logInfo('Acquire locks: {}'.format(lock))

        # No argument provided for edit_versions param
        # ReconcileVersions_management will determine which versions need to be reconciled based on reconcile_mode
        logInfo('Reconciling..')
        arcpy.ReconcileVersions_management(input_database=sdeconn.connectionpath,
                                           reconcile_mode=reconcilemode,  # 'ALL_VERSIONS' or 'BLOCKING_VERSIONS'
                                           target_version=targetversion,
                                           edit_versions=editversions,
                                           acquire_locks=lock,
                                           abort_if_conflicts=abortconflicts,  # 'NO_ABORT' or 'ABORT_CONFLICTS'
                                           conflict_definition='BY_OBJECT',
                                           conflict_resolution='FAVOR_TARGET_VERSION',
                                           with_post=postversions,  # 'POST' or 'NO_POST'
                                           with_delete=deleteversions)  # 'KEEP_VERSION' or 'DELETE_VERSION'
        logInfo(arcpy.GetMessages())
        severity = arcpy.GetMaxSeverity()
        if severity == 2:
            errormessages = arcpy.GetMessages(2)
            logError('Error occurred in reconcile process')
            return 2, errormessages
        if severity == 1:
            warningmessages = arcpy.GetMessages(1)
            logWarning('Warning raised in reconcile process')
            return 1, warningmessages
        else:
            return 0, None
    except Exception:
        logError('RECONCILE FAILED')
        return None, None


# ------------------------------------------------------------------------------
def compressGDB(sdeconn):
    try:
        logInfo('----------------------')
        logInfo('Compressing geodatabase, {} connection..'.format(sdeconn.privilege))
        arcpy.Compress_management(sdeconn.connectionpath)
        return True
    except Exception:
        logError('COMPRESS FAILED')
        return False


# ------------------------------------------------------------------------------
def setWorkspace(database):
    try:
        connection = os.path.split(database)[1].split('@')[0].upper()
        logInfo('Setting {} workspace..'.format(connection))
        arcpy.env.workspace = database
        kw = os.path.split(database)[1].split('@')[0].upper()
        wldcrd = '*.{}.*'.format(kw)
        return database, wldcrd
    except Exception:
        logError('SET WORKSPACE FAILED')


# ------------------------------------------------------------------------------
def rebuildIndexes(sdeconn):
    try:
        logInfo('----------------------')
        logInfo('Rebuilding indexes, {} connection..'.format(sdeconn.privilege))

        # If connected as GDBADMIN, include system stables
        if sdeconn.privilege == 'GDBADMIN':
            includesys = 'SYSTEM'
        else:
            includesys = 'NO_SYSTEM'
        logInfo('Include system: {}'.format(includesys))

        indatasets = sdeconn.datasets
        if len(sdeconn.datasets) < 1:
            logWarning('NO INPUT DATASETS')
            indatasets = ''
        arcpy.RebuildIndexes_management(input_database=sdeconn.connectionpath,
                                        include_system=includesys,
                                        in_datasets=indatasets,
                                        delta_only='ALL')
        return True
    except Exception:
        logError('REBUILD INDEXES FAILED')
        return False


# ------------------------------------------------------------------------------
def analyzeDatasets(sdeconn):
    try:
        logInfo('----------------------')
        logInfo('Analyzing datasets, {} connection..'.format(sdeconn.privilege))

        # If connected as GDBADMIN, include system stables
        if sdeconn.privilege == 'GDBADMIN':
            includesys = 'SYSTEM'
        else:
            includesys = 'NO_SYSTEM'
        logInfo('Include system: {}'.format(includesys))

        indatasets = sdeconn.datasets
        if len(sdeconn.datasets) < 1:
            logWarning('NO INPUT DATASETS')
            indatasets = ''
        arcpy.AnalyzeDatasets_management(input_database=sdeconn.connectionpath,
                                         include_system=includesys,
                                         in_datasets=indatasets,
                                         analyze_base='ANALYZE_BASE',
                                         analyze_delta='ANALYZE_DELTA',
                                         analyze_archive='ANALYZE_ARCHIVE')
        return True
    except Exception:
        logError('ANALYZE DATASETS FAILED')
        return False


# ------------------------------------------------------------------------------
def runSdeMaintenance(gdbadmin, dataowner, reconcilemode, targetversion, abortconflicts, postversions, deleteversions):
    # Reconcile and compress operation
    reconcile_success = reconcileVersions(gdbadmin, reconcilemode, targetversion,
                                          abortconflicts, postversions, deleteversions)
    compress_success = compressGDB(gdbadmin)

    # List datasets owned by user, rebuild indexes and analyze datasets, GDBADMIN connection
    gdbadmin.datasets = gdbadmin.listDatasets()
    gdbadmin_rebldindx_success = rebuildIndexes(gdbadmin)
    gdbadmin_analyze_success = analyzeDatasets(gdbadmin)

    # List datasets owned by user, rebuild indexes and analyze datasets, DATAOWNER connection
    dataowner.datasets = dataowner.listDatasets()
    dataowner_rebldindx_success = rebuildIndexes(dataowner)
    dataowner_analyze_success = analyzeDatasets(dataowner)

    return \
        reconcile_success, compress_success, \
        gdbadmin_rebldindx_success, gdbadmin_analyze_success, \
        dataowner_rebldindx_success, dataowner_analyze_success


# ------------------------------------------------------------------------------
def composeBody(now, runtime, targetgdb,
                interrupservices, portalurl, gis, validservers, serverurl, targetserver, services,
                reconcile_success, compress_success,
                gdbadmin_rebldindx_success, gdbadmin_analyze_success,
                dataowner_rebldindx_success, dataowner_analyze_success):
    mailbody = '<h3>SDE Maintenance Summary, {}</h3>'.format(str(now))
    mailbody += '<h3>Target GDB: {}'.format(targetgdb)

    sdemaintsuccess = (reconcile_success[0] == 0 and compress_success and gdbadmin_rebldindx_success and
                       gdbadmin_analyze_success and dataowner_rebldindx_success and dataowner_analyze_success)

    if interrupservices:
        servicenodeserrors = []
        for srvc in services:
            if not srvc.servicenode:
                servicenodeserrors.append(
                    '{}/{}.{}, UNABLE TO LOCATE SERVICE NODE'.format(srvc.folder, srvc.servicename, srvc.servicetype))
            else:
                if not srvc.stopsuccess:
                    servicenodeserrors.append(
                        '{}/{}.{}, UNABLE TO STOP SERVICE'.format(srvc.folder, srvc.servicename, srvc.servicetype))
                if not srvc.startsuccess:
                    servicenodeserrors.append(
                        '{}/{}.{}, UNABLE TO START SERVICE'.format(srvc.folder, srvc.servicename, srvc.servicetype))
        servicenodeserrors = '; '.join(servicenodeserrors)

        if gis and validservers and targetserver:
            if len(servicenodeserrors) == 0 and sdemaintsuccess:
                successlevel = ('SUCCESS', 'green')
            else:
                if not sdemaintsuccess:
                    if reconcile_success[0] == 2:
                        successlevel = ('ERROR', 'red')
                    elif reconcile_success[0] == 1:
                        successlevel = ('WARNING', 'orange')
                    else:
                        successlevel = ('ERROR', 'red')
                elif 'START' in servicenodeserrors:
                    successlevel = ('ERROR', 'red')
                else:
                    successlevel = ('WARNING', 'orange')
        else:
            successlevel = ('ERROR', 'red')

        mailbody += '<h3><font color="{}">Overall Summary: {}</h3>'.format(successlevel[1], successlevel[0])
        mailbody += '<h3>Run Time: {} seconds</h3>'.format(runtime)

        if gis:
            mailbody += '<h4>Portal Login: {}</h4>'.format(gis)
            if validservers:
                mailbody += '<h4>Valid Servers: True</h4>'
                if targetserver:
                    mailbody += '<h4>Target Server: {}</h4>'.format(serverurl)
                    if len(servicenodeserrors) == 0:
                        mailbody += '<h4>Service Nodes: All Located</h4>'
                    else:
                        if 'START' in servicenodeserrors:
                            color = 'red'
                        else:
                            color = 'orange'
                        mailbody += '<h4><font color="{}">Service Nodes: {}</h4>'.format(color, servicenodeserrors)

                    if reconcile_success[0] == 0:
                        mailbody += '<h4>Reconcile Versions: SUCCESS</h4>'
                    elif reconcile_success[0] == 2:
                        mailbody += '<h4><font color="red">Reconcile Versions: ERROR, {}</h4>'.format(
                            reconcile_success[1])
                    elif reconcile_success[0] == 1:
                        mailbody += '<h4><font color="orange">Reconcile Versions: WARNING, {}</h4>'.format(
                            reconcile_success[1])
                    else:
                        mailbody += '<h4><font color="red">Reconcile Versions: FAILED</h4>'
                    if compress_success:
                        mailbody += '<h4>Compress GDB: SUCCESS</h4>'
                    else:
                        mailbody += '<h4><font color="red">Compress GDB: FAILED</h4>'
                    if gdbadmin_rebldindx_success:
                        mailbody += '<h4>GDB Admin Rebuild Indexes: SUCCESS</h4>'
                    else:
                        mailbody += '<h4><font color="red">GDB Admin Rebuild Indexes: FAILED</h4>'
                    if gdbadmin_analyze_success:
                        mailbody += '<h4>GDB Admin Analyze Datasets: SUCCESS</h4>'
                    else:
                        mailbody += '<h4><font color="red">GDB Admin Analyze Datasets: FAILED</h4>'
                    if dataowner_rebldindx_success:
                        mailbody += '<h4>Data Owner Rebuild Indexes: SUCCESS</h4>'
                    else:
                        mailbody += '<h4><font color="red">Data Owner Rebuild Indexes: FAILED</h4>'
                    if dataowner_analyze_success:
                        mailbody += '<h4>Data Owner Analyze Datasets: SUCCESS</h4>'
                    else:
                        mailbody += '<h4><font color="red">Data Owner Analyze Datasets: FAILED</h4>'
                else:
                    mailbody += '<h4><font color="red">Target Server: {} NOT LOCATED</h4>'.format(serverurl)
            else:
                mailbody += '<h4><font color="red">Valid Servers: FALSE</h4>'
        else:
            mailbody += '<h4><font color="red">Portal Login: {} LOGIN FAILED</h4>'.format(portalurl)

    else:
        if sdemaintsuccess:
            successlevel = ('SUCCESS', 'green')
        else:
            if reconcile_success[0] == 2:
                successlevel = ('ERROR', 'red')
            elif reconcile_success[0] == 1:
                successlevel = ('WARNING', 'orange')
            else:
                successlevel = ('ERROR', 'red')

        mailbody += '<h3><font color="{}">Overall Summary: {}</h3>'.format(successlevel[1], successlevel[0])
        mailbody += '<h3>Run Time: {} seconds</h3>'.format(runtime)

        if reconcile_success[0] == 0:
            mailbody += '<h4>Reconcile Versions: SUCCESS</h4>'
        elif reconcile_success[0] == 2:
            mailbody += '<h4><font color="red">Reconcile Versions: ERROR, {}</h4>'.format(reconcile_success[1])
        elif reconcile_success[0] == 1:
            mailbody += '<h4><font color="orange">Reconcile Versions: WARNING, {}</h4>'.format(reconcile_success[1])
        else:
            mailbody += '<h4><font color="red">Reconcile Versions: FAILED</h4>'
        if compress_success:
            mailbody += '<h4>Compress GDB: {}</h4>'.format('SUCCESS')
        else:
            mailbody += '<h4><font color="red">Compress GDB: {}</h4>'.format('FAILED')
        if gdbadmin_rebldindx_success:
            mailbody += '<h4>GDB Admin Rebuild Indexes: {}</h4>'.format('SUCCESS')
        else:
            mailbody += '<h4><font color="red">GDB Admin Rebuild Indexes: {}</h4>'.format('FAILED')
        if gdbadmin_analyze_success:
            mailbody += '<h4>GDB Admin Analyze Datasets: {}</h4>'.format('SUCCESS')
        else:
            mailbody += '<h4><font color="red">GDB Admin Analyze Datasets: {}</h4>'.format('FAILED')
        if dataowner_rebldindx_success:
            mailbody += '<h4>Data Owner Rebuild Indexes: {}</h4>'.format('SUCCESS')
        else:
            mailbody += '<h4><font color="red">Data Owner Rebuild Indexes: {}</h4>'.format('FAILED')
        if dataowner_analyze_success:
            mailbody += '<h4>Data Owner Analyze Datasets: {}</h4>'.format('SUCCESS')
        else:
            mailbody += '<h4><font color="red">Data Owner Analyze Datasets: {}</h4>'.format('FAILED')

    mailbody += '<h5>See attached log file for detailed info</h5>'

    return mailbody, successlevel


# ------------------------------------------------------------------------------
def sendLogEmail(logfile, mailbody, emaillist, targetgdb, sender_address, sender_pass):
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = ', '.join(emaillist)
    message['Subject'] = '[GISmaintenance] SDE Maintenance, {}, {}'.format(targetgdb, time.strftime('%Y-%m-%d'))
    message.attach(MIMEText(mailbody, 'html'))

    attachment = open(logfile, 'rb')
    payload = MIMEBase('application', 'octet-stream')
    payload.set_payload(attachment.read())
    encoders.encode_base64(payload)
    payload.add_header('Content-Disposition', 'attachment; filename={}'.format(os.path.basename(logfile)))

    message.attach(payload)

    # DUMMY PORT
    port = 99999
    session = smtplib.SMTP('<HOST>', port)
    session.starttls()
    session.login(sender_address, sender_pass)
    session.sendmail(sender_address, emaillist, message.as_string())
    session.quit()


# ------------------------------------------------------------------------------
def main(configfile):
    start = time.perf_counter()
    now = datetime.datetime.now()

    interruptservices, portalurl, portaladmin, portaladminpw, serverurl, services, gdbadminpath, dataownerpath, \
    reconcilemode, targetversion, abortconflicts, postversions, deleteversions, \
    logdirectory, loglevel, emaillog, senderemail, senderpassword, logrecipients = parseConfig(configfile)

    logfile, logger = configureLogging(logdirectory, loglevel, now)

    logInfo('Python interpreter: {}'.format(sys.executable))
    logInfo('SDE Maintenance Log: {}'.format(logfile))

    gdbadmin = SdeConnection('GDBADMIN', gdbadminpath)
    dataowner = SdeConnection('DATAOWNER', dataownerpath)

    gdbmatch, targetgdb = checkConnections(gdbadmin, dataowner)

    reconcile_success = False, False
    compress_success = False
    gdbadmin_rebldindx_success = False
    gdbadmin_analyze_success = False
    dataowner_rebldindx_success = False
    dataowner_analyze_success = False

    validservers = None
    targetserver = None
    gis = None

    if gdbmatch:
        logInfo('GDBADMIN Connection ({} user): {}'.format(gdbadmin.gdbuser, gdbadmin.connectionpath))
        logInfo('DATAOWNER Connection ({} user): {}'.format(dataowner.gdbuser, dataowner.connectionpath))
        if interruptservices:
            gis = connectGIS(portalurl, portaladmin, portaladminpw)
            if gis:
                validservers = validateServers(gis)
                if validservers:
                    targetserver = findTargetServer(gis, serverurl)
                    if targetserver:
                        findServiceNodes(targetserver, services)
                        stopServices(services)
                        reconcile_success, compress_success, \
                        gdbadmin_rebldindx_success, gdbadmin_analyze_success, \
                        dataowner_rebldindx_success, dataowner_analyze_success \
                            = runSdeMaintenance(gdbadmin, dataowner, reconcilemode, targetversion,
                                                abortconflicts, postversions, deleteversions)
                        startServices(services)
        else:
            reconcile_success, compress_success, \
            gdbadmin_rebldindx_success, gdbadmin_analyze_success, \
            dataowner_rebldindx_success, dataowner_analyze_success \
                = runSdeMaintenance(gdbadmin, dataowner, reconcilemode, targetversion,
                                    abortconflicts, postversions, deleteversions)

    logInfo('----------------------')
    runtime = round(time.perf_counter() - start)
    logInfo('RUN TIME: {} seconds'.format(runtime))

    mailbody, successlevel = composeBody(now, runtime, targetgdb,
                                         interruptservices, portalurl, gis, validservers, serverurl, targetserver, services,
                                         reconcile_success, compress_success,
                                         gdbadmin_rebldindx_success, gdbadmin_analyze_success,
                                         dataowner_rebldindx_success, dataowner_analyze_success)
    logInfo('OVERALL SUMMARY: {}'.format(successlevel[0]))

    if emaillog:
        if successlevel[0] == 'SUCCESS':
            logrecipients = [r for r in logrecipients if r.level == 'all']
        emaillist = [r.email for r in logrecipients]
        logInfo('Sending log via email to {}'.format(emaillist))
        quitLogging(logger)
        sendLogEmail(logfile, mailbody, emaillist, targetgdb, senderemail, senderpassword)
    else:
        quitLogging(logger)


# ------------------------------------------------------------------------------
if __name__ == '__main__':

    sdemconfig = "PATH/TO/SDEMAINTENANCE/CONFIG.JSON"

    main(sdemconfig)
