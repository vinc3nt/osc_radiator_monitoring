#!/usr/bin/env python
# -*- coding: utf-8 -*-

import csv
import MySQLdb
import getopt
import gzip
import datetime
import ConfigParser


# Config file
Config = ConfigParser.ConfigParser()
Config.read('keep.cfg')


def ConfigSectionMap(section):
   dict1 = {}
   options = Config.options(section)
   for option in options:
      try:
         dict1[option] = Config.get(section, option)
         if dict1[option] == -1:
            DebugPrint("skip: %s" % option)
      except:
         print("exception on %s!" % option)
         dict1[option] = None
   return dict1


def yesterday():
   today = datetime.date.today()
   yesterday = today - datetime.timedelta(1)
   return yesterday.strftime("%Y%m%d")


def import_db(db, host):
   debug = int(ConfigSectionMap("debug")['debug'])
   mydb = MySQLdb.connect(host='%s' % (ConfigSectionMap("mysql")['host']),
         user='%s' % (ConfigSectionMap("mysql")['user']),
         passwd='%s' % (ConfigSectionMap("mysql")['password']),
         db='%s' % (db))
   cursor = mydb.cursor()
   if debug:
      print "[+] Aperta connessione mysql"
   mycsv = '%s/acct-%s_%s.csv.gz' % (ConfigSectionMap("log")['path'], yesterday(), host)
   auth_data = csv.reader(gzip.open(mycsv), delimiter=';')
   for row in auth_data:
      cursor.execute('INSERT INTO acct(matricola, \
         start, stop)' \
         'VALUES(%s, %s, %s)',
         row)
   mydb.commit()
   if debug:
      print "[+] Dati accounting importati nel database"
   cursor.close()


def main():
   try:
      import_db(ConfigSectionMap("client1")['db'], ConfigSectionMap("client1")['hostname'])
      import_db(ConfigSectionMap("client2")['db'], ConfigSectionMap("client2")['hostname'])
   except IOError:
      if debug:
         print "[-] Il file csv non e' presente"


if __name__ == '__main__':
   main()
