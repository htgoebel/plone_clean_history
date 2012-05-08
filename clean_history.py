# -*- coding: utf-8 -*-

import sys
import optparse

import transaction

from ZODB.POSException import ConflictError
from Products.CMFEditions.utilities import dereference

from AccessControl.SecurityManagement import newSecurityManager
from AccessControl.SecurityManager import setSecurityPolicy
from Testing.makerequest import makerequest
from Products.CMFCore.tests.base.security import PermissiveSecurityPolicy, OmnipotentUser
#from Products.CMFEditions.utilities import isObjectVersioned
from Products.Archetypes.utils import shasattr

version = '0.2'
usage = "usage: /your/instance run %prog [options] [sites]"
description = ("Cleanup CMFEdition history in Plone sites. "
               "Default is: all sites in the database.")

try:
    app
except NameError:
    raise SystemExit(p.get_usage())


def spoofRequest(app):
    """
    Make REQUEST variable to be available on the Zope application server.

    This allows acquisition to work properly
    """
    _policy=PermissiveSecurityPolicy()
    _oldpolicy=setSecurityPolicy(_policy)
    newSecurityManager(None, OmnipotentUser().__of__(app.acl_users))
    return makerequest(app)


def purge_history(id, site, portal_types_to_purge=[], maxNumberOfVersionsToKeep=None, verbose=False):
    print "Analyzing %s" % id
    policy = site.portal_purgepolicy
    portal_repository = site.portal_repository
    if policy.maxNumberOfVersionsToKeep==-1 and not maxNumberOfVersionsToKeep:
        print "... maxNumberOfVersionsToKeep is -1; skipping"
        return

    old_maxNumberOfVersionsToKeep = policy.maxNumberOfVersionsToKeep
    if maxNumberOfVersionsToKeep:
        print "... Putting maxNumberOfVersionsToKeep from %d to %s" % (old_maxNumberOfVersionsToKeep,
                                                                       maxNumberOfVersionsToKeep)
        policy.maxNumberOfVersionsToKeep = maxNumberOfVersionsToKeep

    catalog = site.portal_catalog
    if portal_types_to_purge:
        results = catalog(portal_type=portal_types_to_purge)
    else:
        results = catalog()
    for x in results:
        if verbose:
            print "... cleaning history for %s (%s)" % (x.getPath(), x.portal_type)
        try:
            obj = x.getObject()
            if not portal_repository.isVersionable(obj):
                continue
            obj, history_id = dereference(obj)
            policy.beforeSaveHook(history_id, obj)
            if shasattr(obj, 'version_id'):
                del obj.version_id
        except ConflictError:
            raise
        except Exception, inst:
            # sometimes, even with the spoofed request, the getObject failed
            print "ERROR purging %s (%s)" % (x.getPath(), x.portal_type)
            print "    %s" % inst

    policy.maxNumberOfVersionsToKeep = old_maxNumberOfVersionsToKeep
    transaction.commit()


def main(site_ids, portal_types_to_purge, maxNumberOfVersionsToKeep,
         verbose):
    sites = [(id, site)
             for (id, site) in app.items()
             if getattr(site, 'meta_type', None) == 'Plone Site']

    print 'Starting analysis for %s.'  % (not site_ids and 'all sites' or ', '.join(site_ids))
    print 'Types to cleanup: %s' % (not portal_types_to_purge and 'all' or ', '.join(portal_types_to_purge))

    for id, site in sites:
        if not site_ids or id in site_ids:
            purge_history(id, site, portal_types_to_purge,
                          maxNumberOfVersionsToKeep, verbose)

    print 'End analysis'

p = optparse.OptionParser(usage=usage,
                          version="%prog " + version,
                          description=description,
                          prog="clean_history")
p.add_option('--portal-types', '-p', action="append",
             help=('select to cleanup only histories for a kind of portal_type. '
                   'Default is "all types". Can be called multiple times.'))
p.add_option('--keep-history', '-k', type="int", metavar="HISTORY_SIZE",
             help=('Before purging, temporary set the value of "maximum number '
                   'of versions to keep in the storage" in the '
                   'portal_purgehistory to this value. '
                   'Default is: do not change the value. In any case, the '
                   'original value will be restored.'))
p.add_option('--verbose', '-v', action="store_true",
             help="Show verbose output, for every cleaned content's history.")

# Enable Faux HTTP request object
app = spoofRequest(app)

args = sys.argv[1:]
options, site_ids = p.parse_args(args)

main(site_ids, options.portal_types, options.keep_history,
     options.verbose)
